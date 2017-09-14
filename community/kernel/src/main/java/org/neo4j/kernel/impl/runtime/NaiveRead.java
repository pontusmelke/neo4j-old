/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.runtime;

import java.io.IOException;
import java.lang.*;

import org.neo4j.internal.kernel.api.IndexPredicate;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.runtime.cursors.NaiveNodeCursor;
import org.neo4j.kernel.impl.runtime.cursors.NaivePropertyCursor;
import org.neo4j.kernel.impl.runtime.cursors.NaiveRelationshipCursor;
import org.neo4j.kernel.impl.runtime.cursors.NaiveRelationshipGroupCursor;
import org.neo4j.kernel.impl.runtime.cursors.NaiveRelationshipScanCursor;
import org.neo4j.kernel.impl.runtime.cursors.NaiveRelationshipTraversalCursor;
import org.neo4j.kernel.impl.runtime.cursors.StateAwareNodeCursor;

import static org.neo4j.kernel.impl.runtime.cursors.NaiveBitManipulation.decodeDirectRelationshipReference;
import static org.neo4j.kernel.impl.runtime.cursors.NaiveBitManipulation.isDirectRelationshipReference;

public class NaiveRead implements Read
{
    private final PagedFile nodeStore;
    private final PagedFile relationshipStore;
    private final PagedFile relationshipGroupStore;
    private final PagedFile propertyStore;
    private final TxStateHolder stateHolder;

    NaiveRead( PagedFile nodeStore, PagedFile relationshipStore, PagedFile relationshipGroupStore, PagedFile propertyStore,
            TxStateHolder stateHolder )
    {
        this.nodeStore = nodeStore;
        this.relationshipStore = relationshipStore;
        this.relationshipGroupStore = relationshipGroupStore;
        this.propertyStore = propertyStore;
        this.stateHolder = stateHolder;
    }

    @Override
    public void nodeIndexSeek( IndexReference index, NodeValueIndexCursor cursor, IndexPredicate... predicates )
    {

    }

    @Override
    public void nodeIndexScan( IndexReference index, NodeValueIndexCursor cursor )
    {

    }

    @Override
    public void nodeLabelScan( int label, NodeLabelIndexCursor cursor )
    {

    }

    @Override
    public Scan<NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        return null;
    }

    @Override
    public void allNodesScan( NodeCursor cursor )
    {
        int pageSizeInRecords = nodeStore.pageSize() / NaiveNodeCursor.RECORD_SIZE;
        try
        {
            long maxAddress = pageSizeInRecords * (nodeStore.getLastPageId() + 1);
            ((StateAwareNodeCursor) cursor).init(
                    nodeStore.io( 0, PagedFile.PF_SHARED_READ_LOCK ), 0, maxAddress, this, stateHolder );
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during allNodesScan!", e );
        }
    }

    @Override
    public Scan<NodeCursor> allNodesScan()
    {
        return null;
    }

    @Override
    public void singleNode( long reference, NodeCursor cursor )
    {
        int pageSizeInRecords = nodeStore.pageSize() / NaiveNodeCursor.RECORD_SIZE;
        long pageId = reference / pageSizeInRecords;
        try
        {
            PageCursor pageCursor = nodeStore.io( pageId, PagedFile.PF_SHARED_READ_LOCK );
            ((StateAwareNodeCursor) cursor).init( pageCursor, reference, reference + 1, this, stateHolder );
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during singleNode!", e );
        }
    }

    @Override
    public void singleRelationship( long reference, RelationshipScanCursor cursor )
    {
        int pageSizeInRecords = relationshipStore.pageSize() / NaiveRelationshipCursor.RECORD_SIZE;
        long pageId = reference / pageSizeInRecords;
        try
        {
            PageCursor pageCursor = relationshipStore.io( pageId, PagedFile.PF_SHARED_READ_LOCK );
            ((NaiveRelationshipScanCursor) cursor).init( pageCursor, reference, reference + 1, this);
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during singleRelationship!", e );
        }
    }

    @Override
    public void allRelationshipsScan( RelationshipScanCursor cursor )
    {
        int pageSizeInRecords = relationshipStore.pageSize() / NaiveRelationshipCursor.RECORD_SIZE;
        try
        {
            long maxAddress = pageSizeInRecords * (relationshipStore.getLastPageId() + 1);
            PageCursor pageCursor = relationshipStore.io( 0, PagedFile.PF_SHARED_READ_LOCK );
            ((NaiveRelationshipScanCursor) cursor).init( pageCursor, 0, maxAddress, this );
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during allRelationshipsScan!", e );
        }
    }

    @Override
    public Scan<RelationshipScanCursor> allRelationshipsScan()
    {
        return null;
    }

    @Override
    public void relationshipLabelScan( int label, RelationshipScanCursor cursor )
    {

    }

    @Override
    public Scan<RelationshipScanCursor> relationshipLabelScan( int label )
    {
        return null;
    }

    @Override
    public void relationshipGroups( long nodeReference, long reference, RelationshipGroupCursor cursor )
    {
        if ( isDirectRelationshipReference( reference ) )
        {
            ((NaiveRelationshipGroupCursor) cursor).initVirtual( nodeReference, reference,this );
        }
        else
        {
            int pageSizeInRecords = relationshipGroupStore.pageSize() / NaiveRelationshipGroupCursor.RECORD_SIZE;
            long pageId = reference / pageSizeInRecords;
            try
            {
                PageCursor pageCursor = relationshipGroupStore.io( pageId, PagedFile.PF_SHARED_READ_LOCK );
                ((NaiveRelationshipGroupCursor) cursor).init(
                        pageCursor, nodeReference, reference, this );
            }
            catch ( IOException e )
            {
                throw new PoorlyNamedException( "IOException during nodeProperties!", e );
            }
        }
    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {
        int pageSizeInRecords;
        assert ( !isDirectRelationshipReference( reference ) ); // We must not get any encoded references here

        pageSizeInRecords = relationshipStore.pageSize() / NaiveRelationshipTraversalCursor.RECORD_SIZE;
        long pageId = reference / pageSizeInRecords;
        try
        {
            PageCursor pageCursor = relationshipStore.io( pageId, PagedFile.PF_SHARED_READ_LOCK );
            ((NaiveRelationshipTraversalCursor) cursor).init( pageCursor, nodeReference, reference, this );
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during relationships!", e );
        }
    }

    @Override
    public void nodeProperties( long reference, PropertyCursor cursor )
    {
        int pageSizeInRecords = propertyStore.pageSize() / NaivePropertyCursor.RECORD_SIZE;
        long pageId = reference / pageSizeInRecords;
        try
        {
            PageCursor pageCursor = propertyStore.io( pageId, PagedFile.PF_SHARED_READ_LOCK );
            ((NaivePropertyCursor) cursor).init( pageCursor, reference );
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during nodeProperties!", e );
        }
    }

    @Override
    public void relationshipProperties( long reference, PropertyCursor cursor )
    {

    }

    @Override
    public void futureNodeReferenceRead( long reference )
    {

    }

    @Override
    public void futureRelationshipsReferenceRead( long reference )
    {

    }

    @Override
    public void futureNodePropertyReferenceRead( long reference )
    {

    }

    @Override
    public void futureRelationshipPropertyReferenceRead( long reference )
    {

    }
}
