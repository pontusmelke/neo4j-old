package org.neo4j.kernel.impl.runtime.cursors;

import java.io.IOException;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.runtime.NaiveRuntime;
import org.neo4j.kernel.impl.runtime.PoorlyNamedException;

public class NaiveNodeCursor implements NodeCursor
{
    /**
     * Node record byte layout
     *
     * <pre>
     *  0: in use        (1 byte)
     *  1: relationships (4 bytes)
     *  5: properties    (4 bytes)
     *  9: labels        (5 bytes)
     * 14: extra         (1 byte)
     * </pre>
     * <h2>in use</h2>
     * <pre>
     * [    ,   x] in use
     * [    ,xxx ] high(relationships)
     * [xxxx,    ] high(properties)
     * </pre>
     * <h2>labels</h2>
     * byte order: <code>[3][2][1][0][4]</code> (4 is msb, 0 is lsb)
     * <pre>
     * [    ,    ] [    ,    ] [    ,    ] [    ,    ] [1   ,    ] reference to labels store (0x80_0000_0000)
     * [    ,    ] [    ,    ] [    ,    ] [    ,    ] [0xxx,    ] number of inlined labels  (0x70_0000_0000)
     * </pre>
     * <h2>extra</h2>
     * <pre>
     * [    ,   x] dense
     * </pre>
     */

    private static final int RECORD_SIZE = 15;
    private int addressInPage;
    private long virtualAddress;
    private long maxVirtualAddress;
    private PageCursor pageCursor;
    private int pageNumRecords;

    public void init( PageCursor pageCursor, long maxPageId )
    {
        pageNumRecords = NaiveRuntime.NODE_STORE_PAGE_SIZE / RECORD_SIZE;

        // on init we stand on the last record before first page
        addressInPage = pageNumRecords - 1;
        virtualAddress = -1;

        this.pageCursor = pageCursor;
        this.maxVirtualAddress = (maxPageId + 1) * pageNumRecords;
    }

    @Override
    public long nodeReference()
    {
        return pageCursor.getCurrentPageId() * pageNumRecords + addressInPage;
    }

    @Override
    public boolean next()
    {
        while ( scanNextByVirtualAddress( pageCursor, virtualAddress, maxVirtualAddress ) )
        {
            if ( inUse() )
            {
                return true;
            }
        }
        return false;
    }

    private boolean scanNextByVirtualAddress( PageCursor pageCursor, long currentAddress, long maxAddress )
    {
        if ( currentAddress + 1 >= maxAddress )
        {
            return false;
        }

        if ( addressInPage + 1 < pageNumRecords )
        {
            this.addressInPage++;
            this.virtualAddress++;
            return true;
        }
        else
        {
            if ( advancePageCursor() )
            {
                this.addressInPage = 0;
                this.virtualAddress++;
                return true;
            }
            else
            {
                return false;
            }
        }
    }

    private boolean advancePageCursor()
    {
        boolean result;
        try
        {
            do
            {
                result = pageCursor.next();
            } while ( pageCursor.shouldRetry() );
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during pageCursor advance", e );
        }
        return result;
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {

    }

    // DATA ACCESSOR METHODS

    private boolean inUse()
    {
        return (pageCursor.getByte( addressInPage * RECORD_SIZE ) & 0x01) != 0;
    }

    @Override
    public LabelSet labels()
    {
        return LabelSet.NONE;
    }

    @Override
    public boolean hasProperties()
    {
        return false;
    }

    @Override
    public void relationships( RelationshipGroupCursor cursor )
    {

    }

    @Override
    public void outgoingRelationships( RelationshipGroupCursor groups, RelationshipTraversalCursor relationships )
    {

    }

    @Override
    public void incomingRelationships( RelationshipGroupCursor groups, RelationshipTraversalCursor relationships )
    {

    }

    @Override
    public void allRelationships( RelationshipGroupCursor groups, RelationshipTraversalCursor relationships )
    {

    }

    @Override
    public void properties( PropertyCursor cursor )
    {

    }

    @Override
    public long relationshipGroupReference()
    {
        return 0;
    }

    @Override
    public long propertiesReference()
    {
        return 0;
    }
}
