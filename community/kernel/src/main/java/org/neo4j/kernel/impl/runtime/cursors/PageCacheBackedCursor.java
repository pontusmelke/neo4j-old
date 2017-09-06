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
 */
abstract class PageCacheBackedCursor
{
    private int offsetInPage;
    private long address;
    private long maxAddress;
    private PageCursor pageCursor;

    abstract int recordSize();
    abstract int pageSize();

    boolean initCursor( PageCursor pageCursor, long startAddress, long maxAddress )
    {
        if ( startAddress >= 0 && startAddress < maxAddress )
        {
            offsetInPage = pageSize();
            address = startAddress - 1;

            this.pageCursor = pageCursor;
            this.maxAddress = maxAddress;
            return true;
        }
        else
        {
            tearDownCursor();
            return false;
        }
    }

    void tearDownCursor()
    {
        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }
        address = -1;
        maxAddress = -1;
        offsetInPage = -1;
    }

    long address()
    {
        return address;
    }

    // CURSOR MOVEMENT

    boolean scanNextByAddress()
    {
//        System.out.println( String.format( "%6d %6d | %6d %6d",
//                currentAddress, maxAddress, offsetInPage, pageSizeInRecords ) );
        if ( address + 1 >= maxAddress )
        {
            return false;
        }

        if ( offsetInPage + recordSize() < pageSize() )
        {
            this.offsetInPage += recordSize();
            this.address++;
            return true;
        }

        if ( advancePageCursor() )
        {
            this.address++;
            int pageSizeInRecords = pageSize() / recordSize();
            this.offsetInPage = (int)(address % pageSizeInRecords) * recordSize();
            return true;
        }

        return false;
    }

    private boolean advancePageCursor()
    {
        boolean result;
        try
        {
            do
            {
                result = pageCursor.next();
            }
            while ( pageCursor.shouldRetry() );
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during pageCursor.next()", e );
        }
        return result;
    }

    boolean moveToAddress( long address )
    {
        int pageSizeInRecords = pageSize() / recordSize();
        long pageId = address / pageSizeInRecords;
        boolean result;
        try
        {
            do
            {
                result = pageCursor.next( pageId );
            }
            while ( pageCursor.shouldRetry() );
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during pageCursor.next( pageId )", e );
        }
        if ( result )
        {
            this.address = address;
            this.maxAddress = address + 1;
            this.offsetInPage = (int)((address % pageSizeInRecords) * recordSize());
        }
        return result;
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
}
