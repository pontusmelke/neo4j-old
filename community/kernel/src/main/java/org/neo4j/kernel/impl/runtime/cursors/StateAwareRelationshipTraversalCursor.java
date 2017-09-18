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

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.storageengine.api.txstate.RelationshipState;

import static org.neo4j.kernel.impl.runtime.cursors.NaiveConstants.NO_NODE;
import static org.neo4j.kernel.impl.runtime.cursors.NaiveConstants.NO_RELATIONSHIP;

// A nicer design could be to have the PageBackedCursor as a delegate member instead of superclass
// This could improve the close() logic

public class StateAwareRelationshipTraversalCursor extends VirtualGroupAwareRelationshipTraversalCursor
{
    private TxStateHolder stateHolder;
    private PrimitiveLongIterator addedRelationshipsIterator;
    private long currentRelationship = NO_RELATIONSHIP;

    private boolean hasExtractedCurrentRelationshipState = false;
    private long sourceNodeReference = NO_NODE;
    private long targetNodeReference = NO_NODE;
    private int label = -1;

    public void init( PageCursor pageCursor, long originNodeReference, long initialAddress, Read read,
            TxStateHolder stateHolder )
    {
        // TODO: If no physical records exists the cursor will be closed immediately and resetting the fields
        super.init( pageCursor, originNodeReference, initialAddress, read );
        this.stateHolder = stateHolder;
    }

    public void initVirtual( long originNodeReference, Read read, TxStateHolder stateHolder )
    {
        if ( isBound() )
        {
            close();
        }
        setOriginNodeReference( originNodeReference );
        this.read = read;
        this.stateHolder = stateHolder;
    }

    @Override
    public boolean next()
    {
        if ( super.next() )
        {
            boolean hasNext = true;
            if ( stateHolder.hasTxStateWithChanges() )
            {
                // Filter out relationships deleted in this transaction
                long relationshipId = super.relationshipReference();
                while ( relationshipId != NO_RELATIONSHIP &&
                        stateHolder.txState().relationshipIsDeletedInThisTx( relationshipId ) )
                {
                    if ( !super.next() )
                    {
                        hasNext = false;
                        break;
                    }
                    relationshipId = super.relationshipReference();
                }
            }
            if ( hasNext )
            {
                return true;
            }
        }
        if ( addedRelationshipsIterator != null && addedRelationshipsIterator.hasNext() )
        {
            setCurrentRelationship( addedRelationshipsIterator.next() );
            return true;
        }
        return false;
    }

    private void setCurrentRelationship( long relationshipId )
    {
        currentRelationship = relationshipId;
        hasExtractedCurrentRelationshipState = false;
    }

    @Override
    public void neighbour( NodeCursor cursor )
    {
        if ( stateHolder.hasTxStateWithChanges() )
        {
            long neighbourId = neighbourNodeReference();
            if ( stateHolder.txState().nodeIsAddedInThisTx( neighbourId ) )
            {
                ((StateAwareNodeCursor) cursor).initFromTransactionState( neighbourId, stateHolder );
                return;
            }
        }
        read.singleNode( neighbourNodeReference(), cursor );
    }

    @Override
    public long relationshipReference()
    {
        if ( currentRelationship != NO_RELATIONSHIP )
        {
            return currentRelationship;
        }
        return super.relationshipReference();
    }

    @Override
    public long sourceNodeReference()
    {
        if ( hasExtractedCurrentRelationshipState )
        {
            return sourceNodeReference;
        }
        if ( currentRelationship != NO_RELATIONSHIP )
        {
            assert stateHolder.hasTxStateWithChanges();
            extractRelationshipState();
            return sourceNodeReference;
        }
        return super.sourceNodeReference();
    }

    @Override
    public long targetNodeReference()
    {
        if ( hasExtractedCurrentRelationshipState )
        {
            return targetNodeReference;
        }
        if ( currentRelationship != NO_RELATIONSHIP )
        {
            assert stateHolder.hasTxStateWithChanges();
            extractRelationshipState();
            return targetNodeReference;
        }
        return super.targetNodeReference();
    }

    @Override
    public int label()
    {
        if ( hasExtractedCurrentRelationshipState )
        {
            return label;
        }
        if ( currentRelationship != NO_RELATIONSHIP )
        {
            assert stateHolder.hasTxStateWithChanges();
            extractRelationshipState();
            return label;
        }
        return super.label();
    }

    private void extractRelationshipState()
    {
        RelationshipState relState = stateHolder.txState().getRelationshipState( currentRelationship );
        relState.accept( (RelationshipVisitor<RuntimeException>)
                ( relationshipId, typeId, startNodeId, endNodeId ) -> {
                    sourceNodeReference = startNodeId;
                    targetNodeReference = endNodeId;
                    label = typeId;
                } );
        hasExtractedCurrentRelationshipState = true;
    }

    @Override
    protected void onClose()
    {
        // Prevent setting originNodeReference to NO_NODE
    }

    public void initWithAddedRelationshipsIterator( PrimitiveLongIterator addedRelationshipsIterator,
            long originNodeReference, Read read, TxStateHolder stateHolder )
    {
        initVirtual( originNodeReference, read, stateHolder );
        setAddedRelationshipsIterator( addedRelationshipsIterator );
    }

    public void setAddedRelationshipsIterator( PrimitiveLongIterator addedRelationshipsIterator )
    {
        this.addedRelationshipsIterator = addedRelationshipsIterator;
        currentRelationship = NO_RELATIONSHIP;
    }
}