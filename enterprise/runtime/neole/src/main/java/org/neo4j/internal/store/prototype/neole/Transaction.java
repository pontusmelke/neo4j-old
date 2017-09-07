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
package org.neo4j.internal.store.prototype.neole;

import java.io.IOException;

import org.neo4j.internal.kernel.api.IndexPredicate;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.values.storable.Value;

public class Transaction implements org.neo4j.internal.kernel.api.Transaction
{
    private final ReadStore read;

    public Transaction( ReadStore read ) throws IOException
    {
        this.read = read;
    }

    // WRITES ARE NOT SUPPORTED

    @Override
    public long nodeCreate()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeDelete( long node )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long relationshipCreate( long sourceNode, int relationshipLabel, long targetNode )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void relationshipDelete( long relationship )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeAddLabel( long node, int nodeLabel )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeRemoveLabel( long node, int nodeLabel )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeSetProperty( long node, int propertyKey, Object value )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeRemoveProperty( long node, int propertyKey )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void relationshipSetProperty( long relationship, int propertyKey, Value value )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void relationshipRemoveProperty( long node, int propertyKey )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    // THIS IS NOT REALLY A TRANSACTION

    @Override
    public void success()
    {

    }

    @Override
    public void failure()
    {

    }

    @Override
    public void close() throws Exception
    {

    }

    // READ OPS

    @Override
    public void nodeIndexSeek( IndexReference index, NodeValueIndexCursor cursor, IndexPredicate... predicates )
    {
        read.nodeIndexSeek( index, cursor, predicates );
    }

    @Override
    public void nodeIndexScan( IndexReference index, NodeValueIndexCursor cursor )
    {
        read.nodeIndexScan( index, cursor );
    }

    @Override
    public void nodeLabelScan( int label, NodeLabelIndexCursor cursor )
    {
        read.nodeLabelScan( label, cursor );
    }

    @Override
    public Scan<NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        return read.nodeLabelScan( label );
    }

    @Override
    public void allNodesScan( NodeCursor cursor )
    {
        read.allNodesScan( cursor );
    }

    @Override
    public Scan<NodeCursor> allNodesScan()
    {
        return read.allNodesScan();
    }

    @Override
    public void singleNode( long reference, NodeCursor cursor )
    {
        read.singleNode( reference, cursor );
    }

    @Override
    public void singleRelationship( long reference, RelationshipScanCursor cursor )
    {
        read.singleRelationship( reference, cursor );
    }

    @Override
    public void allRelationshipsScan( RelationshipScanCursor cursor )
    {
        read.allRelationshipsScan( cursor );
    }

    @Override
    public Scan<RelationshipScanCursor> allRelationshipsScan()
    {
        return read.allRelationshipsScan();
    }

    @Override
    public void relationshipLabelScan( int label, RelationshipScanCursor cursor )
    {
        read.relationshipLabelScan( label, cursor );
    }

    @Override
    public Scan<RelationshipScanCursor> relationshipLabelScan( int label )
    {
        return read.relationshipLabelScan( label );
    }

    @Override
    public void relationshipGroups( long nodeReference, long reference, RelationshipGroupCursor cursor )
    {
        read.relationshipGroups( nodeReference, reference, cursor );
    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {
        read.relationships( nodeReference, reference, cursor );
    }

    @Override
    public void nodeProperties( long reference, PropertyCursor cursor )
    {
        read.nodeProperties( reference, cursor );
    }

    @Override
    public void relationshipProperties( long reference, PropertyCursor cursor )
    {
        read.relationshipProperties( reference, cursor );
    }

    @Override
    public void futureNodeReferenceRead( long reference )
    {
        read.futureNodeReferenceRead( reference );
    }

    @Override
    public void futureRelationshipsReferenceRead( long reference )
    {
        read.futureRelationshipsReferenceRead( reference );
    }

    @Override
    public void futureNodePropertyReferenceRead( long reference )
    {
        read.futureNodePropertyReferenceRead( reference );
    }

    @Override
    public void futureRelationshipPropertyReferenceRead( long reference )
    {
        read.futureRelationshipPropertyReferenceRead( reference );
    }

    // READ OPS

}
