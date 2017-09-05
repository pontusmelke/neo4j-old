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
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.runtime.cursors.NaiveNodeCursor;

public class NaiveRead implements Read
{
    private final PagedFile nodeStore;

    NaiveRead( PagedFile nodeStore )
    {
        this.nodeStore = nodeStore;
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
        try
        {
            ((NaiveNodeCursor) cursor).init( nodeStore.io( 0, PagedFile.PF_SHARED_READ_LOCK ), nodeStore.getLastPageId() );
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException( "IOException during allNodeScan!", e );
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

    }

    @Override
    public void singleRelationship( long reference, RelationshipScanCursor cursor )
    {

    }

    @Override
    public void allRelationshipsScan( RelationshipScanCursor cursor )
    {

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

    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {

    }

    @Override
    public void nodeProperties( long reference, PropertyCursor cursor )
    {

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
