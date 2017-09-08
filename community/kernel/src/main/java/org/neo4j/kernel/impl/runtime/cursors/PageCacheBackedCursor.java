/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.runtime.cursors;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.runtime.PoorlyNamedException;

import static java.lang.String.format;

/**
 * In the naive Kernel API implementation, the following nomenclature is used.
 *
 * A store consists of a number of fixed size records.
 *
 * The logical position of a record in the store is called it's address, meaning that the first records has address
 * 0, the second 1, and so on.
 *
 * The store is mapped in the page cache into a number of pages. Each page contains a fixed number of records, and
 * exactly aligns with the record size, so no records are split between pages. Pages are referenced via they're pageId.
 *
 * In a page, each record has an offset. This is the byte offset of that record in the page, computed as logical
 * position in page * record size. The page size is the byte size of the page.
 *
 * ----------
 *
 * The PageCacheBackedCursor has two modes of operation: Scanning and Jumping.
 *
 * The cursor starts in the Unbound state. From there it can be initialized as either a scanning cursor or a jumping
 * one. Once in a certain mode, only the traversal operations particular to that mode can be called. It is the
 * responsibility of extending classed to ensure this. On `close()` the cursor will revert to the unbound state, and
 * can be reused in either of the two modes.
 */
abstract class PageCacheBackedCursor implements AutoCloseable
{
    private static final int FIRST_JUMP = -1;
    private static final int JUMPING_CURSOR = -2;

    private int offsetInPage;
    private long address;
    private long maxAddress;
    private PageCursor pageCursor;

    abstract int recordSize();
    abstract int pageSize();

    // SCANNING CURSORS

    void initScanningCursor( PageCursor pageCursor, long startAddress, long maxAddress )
    {
        assert isUnbound() : "Can only initialize unbound cursor";

        if ( startAddress >= 0 && startAddress < maxAddress && maxAddress > 0 )
        {
            offsetInPage = pageSize();
            address = startAddress - 1;

            this.pageCursor = pageCursor;
            this.maxAddress = maxAddress;
        }
        else
        {
            close();
        }
    }

    boolean scanNextByAddress()
    {
        assert isBound() : "Cannot use unbound cursor!";
        assert isScanningCursor() : "Cannot scan using jumping cursor";

        this.address++;
        this.offsetInPage += recordSize();

        if ( address >= maxAddress )
        {
            return false;
        }

        if ( offsetInPage < pageSize() )
        {
            return true;
        }

        if ( advancePageCursor() )
        {
            int pageSizeInRecords = pageSize() / recordSize();
            this.offsetInPage = (int)(address % pageSizeInRecords) * recordSize();
            return true;
        }

        return false;
    }

    private boolean isScanningCursor()
    {
        return maxAddress != JUMPING_CURSOR;
    }

    // JUMPING CURSOR

    void initJumpingCursor( PageCursor pageCursor, long initialAddress )
    {
        assert isUnbound() : "Can only initialize unbound cursor";

        if ( initialAddress >= 0 )
        {
            this.pageCursor = pageCursor;
            this.address = initialAddress;
            this.offsetInPage = FIRST_JUMP;
            this.maxAddress = JUMPING_CURSOR;
        }
        else
        {
            close();
        }
    }

    boolean firstJump()
    {
        assert isJumpingCursor() : "Cannot jump using scanning cursor";
        return offsetInPage == FIRST_JUMP && jumpToAddress( address );
    }

    boolean jumpToAddress( long address )
    {
        assert isBound() : "Cannot use unbound cursor!";
        assert isJumpingCursor() : "Cannot jump using scanning cursor";

        if ( address < 0 )
        {
            close();
            return false;
        }

        int pageSizeInRecords = pageSize() / recordSize();
        long pageId = address / pageSizeInRecords;
        boolean result;
        try
        {
            result = pageCursor.next( pageId );
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during pageCursor.next( pageId )", e );
        }
        if ( result )
        {
            this.address = address;
            this.offsetInPage = (int)((address % pageSizeInRecords) * recordSize());
        }
        return result;
    }

    private boolean isJumpingCursor()
    {
        return maxAddress == JUMPING_CURSOR;
    }

    // CLOSING CURSOR

    @Override
    public final void close()
    {
        onClose();
        if ( pageCursor != null )
        {
            if ( pageCursor.checkAndClearBoundsFlag() )
            {
                throw new PoorlyNamedException( "OutOfBounds flag raised!", null );
            }
            pageCursor.close();
            pageCursor = null;
        }
        address = -1;
        maxAddress = -1;
        offsetInPage = -1;
    }

    protected void onClose()
    {
        // Extending classes can hook in logic here
    }

    // CURSOR HELPERS

    boolean isUnbound()
    {
        return pageCursor == null;
    }

    boolean isBound()
    {
        return pageCursor != null;
    }

    long address()
    {
        return address;
    }

    public boolean shouldRetry()
    {
        assert isBound() : "Cannot use unbound cursor!";

        try
        {
            return pageCursor.shouldRetry();
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during pageCursor.shouldRetry()!", e );
        }
    }

    // DATA ACCESS

    final byte readByte( int offset )
    {
        return pageCursor.getByte( offsetInPage( offset, 1 ) );
    }

    final int unsignedByte( int offset )
    {
        return 0xFF & readByte( offset );
    }

    final short readShort( int offset )
    {
        return pageCursor.getShort( offsetInPage( offset, 2 ) );
    }

    final int unsignedShort( int offset )
    {
        return 0xFFFF & readShort( offset );
    }

    final int readInt( int offset )
    {
        return pageCursor.getInt( offsetInPage( offset, 4 ) );
    }

    final long unsignedInt( int offset )
    {
        return 0xFFFF_FFFFL & readInt( offset );
    }

    final long readLong( int offset )
    {
        return pageCursor.getLong( offsetInPage( offset, 8 ) );
    }

    // PRIVATE HELPERS

    private int offsetInPage( int offsetInRecord, int size )
    {
        if ( pageCursor == null )
        {
            throw new IllegalStateException( "Cursor has not been initialized." );
        }
        assert withinBounds( offsetInRecord, size );
        return offsetInPage + offsetInRecord;
    }

    private boolean withinBounds( int offsetInRecord, int size )
    {
        int bound = offsetInPage + recordSize();
        if ( offsetInRecord + size > bound )
        {
            throw new IndexOutOfBoundsException( format(
                    "This cursor is bounded to %d bytes, tried to access %d bytes at offset %d.",
                    bound, size, offsetInRecord ) );
        }
        return true;
    }

    private boolean advancePageCursor()
    {
        assert isBound() : "Cannot use unbound cursor!";
        try
        {
            return pageCursor.next();
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during pageCursor.next()", e );
        }
    }
}
