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
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.txstate.NodeState;

public class StateAwareRelationshipGroupCursor extends NaiveRelationshipGroupCursor
{
    private TxStateHolder stateHolder;

    StateAwareRelationshipGroupCursor( NaiveRelationshipTraversalCursor innerHelper )
    {
        super( innerHelper );
    }

    public void init( PageCursor pageCursor, long originNodeReference, long initialAddress, Read read,
            TxStateHolder stateHolder )
    {
        super.init( pageCursor, originNodeReference, initialAddress, read );
        this.stateHolder = stateHolder;
        augmentWithTransactionState();
    }

    public void initFromDirectRelationship( long originNodeReference, long initialAddress, Read read,
            TxStateHolder stateHolder )
    {
        super.initFromDirectRelationship( originNodeReference, initialAddress, read );
        this.stateHolder = stateHolder;
        augmentWithTransactionState();
    }

    public void initFromTransactionState( long originNodeReference, TxStateHolder stateHolder )
    {
        super.initVirtual( originNodeReference, read );
        this.stateHolder = stateHolder;
        augmentWithTransactionState();
    }

    private void augmentWithTransactionState()
    {
        if ( stateHolder.hasTxStateWithChanges() )
        {
            // When we access the groups from the page cache, we need to augment with the
            // virtual data from the transaction state.
            // We need to support both the case where we have a mixed cursor with both
            // physical (page cache backed) data and virtual data,
            // as well as the case where we have a pure virtual cursor.
            VirtualRelationshipGroupCursor virtualGroupCursor = getOrCreateVirtualGroupCursor();
            virtualGroupCursor.augmentWithTransactionState( stateHolder.txState(), originNodeReference() );
        }
    }

    @Override
    public boolean next()
    {
        // TODO: Check deleted relationships in transaction state.
        return super.next();
    }

    @Override
    public void outgoing( RelationshipTraversalCursor cursor )
    {
        if ( stateHolder.hasTxStateWithChanges() )
        {
            NodeState originNodeState = stateHolder.txState().getNodeState( originNodeReference() );
            int[] relTypes = new int[]{ relationshipLabel() };
            PrimitiveLongIterator addedRelIterator =
                    originNodeState.getAddedRelationships( Direction.OUTGOING, relTypes );
            ((StateAwareRelationshipTraversalCursor) cursor).initWithAddedRelationshipsIterator(
                    addedRelIterator,
                    originNodeReference(), read, stateHolder );
        }
        else
        {
            ((StateAwareRelationshipTraversalCursor) cursor).initWithAddedRelationshipsIterator(
                    null,
                    originNodeReference(), read, stateHolder );
        }
        super.outgoing( cursor );

        if ( isPureVirtualGroup() )
        {
            ((StateAwareRelationshipTraversalCursor) cursor).initVirtual( originNodeReference(), read, stateHolder );
        }
    }

    @Override
    public void incoming( RelationshipTraversalCursor cursor )
    {
        if ( stateHolder.hasTxStateWithChanges() )
        {
            NodeState originNodeState = stateHolder.txState().getNodeState( originNodeReference() );
            int[] relTypes = new int[]{ relationshipLabel() };
            PrimitiveLongIterator addedRelIterator =
                    originNodeState.getAddedRelationships( Direction.INCOMING, relTypes );
            ((StateAwareRelationshipTraversalCursor) cursor).setAddedRelationshipsIterator( addedRelIterator );
        }
        else
        {
            ((StateAwareRelationshipTraversalCursor) cursor).setAddedRelationshipsIterator( null );
        }
        super.incoming( cursor );

        if ( isPureVirtualGroup() )
        {
            ((StateAwareRelationshipTraversalCursor) cursor).initVirtual( originNodeReference(), read, stateHolder );
        }
    }

}