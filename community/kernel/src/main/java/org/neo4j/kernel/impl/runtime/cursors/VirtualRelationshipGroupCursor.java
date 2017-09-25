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
package org.neo4j.kernel.impl.runtime.cursors;

import java.util.Iterator;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.RelationshipState;

import static org.neo4j.kernel.impl.runtime.cursors.NaiveConstants.NO_RELATIONSHIP;

class VirtualRelationshipGroupCursor implements RelationshipGroupCursor
{
    private PrimitiveIntObjectMap<VirtualRelationshipGroup> virtualGroups;
    private Iterator<VirtualRelationshipGroup> virtualGroupIterator;
    private VirtualRelationshipGroup currentVirtualGroup;
    private PrimitiveIntSet alreadyShadowedPhysicalGroups;

    VirtualRelationshipGroupCursor()
    {
        virtualGroups = Primitive.intObjectMap();
    }

    public void init( NaiveRelationshipTraversalCursor cursor )
    {
        readDirectRelationshipsIntoVirtualGroups( cursor );
        virtualGroupIterator = null;
        currentVirtualGroup = null;
    }

    public void augmentWithTransactionState( TransactionState txState, long originNodeReference )
    {
        readTransactionStateIntoVirtualGroups( txState, originNodeReference );
        virtualGroupIterator = null;
        currentVirtualGroup = null;
    }

//    public void reset()
//    {
//        virtualGroups = null;
//        virtualGroupIterator = null;
//        currentVirtualGroup = null;
//        alreadyShadowedPhysicalGroups = null;
//    }

    private void readDirectRelationshipsIntoVirtualGroups( NaiveRelationshipTraversalCursor cursor )
    {
        VirtualRelationshipGroup group;

        while ( cursor.next() )
        {
            int label = cursor.label();
            group = getOrCreateVirtualRelationshipGroup( label );
            long reference = cursor.relationshipReference();

            // Categorize relationships in mutually exclusive buckets
            // NOTE: We have to check isLoop() first since isOutgoing() and isIncoming() is also true for loops
            if ( cursor.isLoop() )
            {
                assert cursor.sourceNodeReference() == cursor.originNodeReference();
                group.loops.push( reference );
            }
            else if ( cursor.isOutgoing() )
            {
                group.outgoing.push( reference );
            }
            else // if ( cursor.isIncoming() )
            {
                assert cursor.isIncoming();
                group.incoming.push( reference );
            }
        }
    }

    private VirtualRelationshipGroup getOrCreateVirtualRelationshipGroup( int label )
    {
        VirtualRelationshipGroup group;
        group = virtualGroups.get( label );
        if ( group == null )
        {
            // Create a new virtual group if it does not exist
            group = new VirtualRelationshipGroup();
            group.label = label;
            virtualGroups.put( label, group );
        }
        return group;
    }

    private void readTransactionStateIntoVirtualGroups( TransactionState txState, long originNodeReference )
    {
        NodeState nodeState = txState.getNodeState( originNodeReference );
        if ( nodeState.hasChanges() )
        {
            PrimitiveLongIterator addedRelationships = nodeState.getAddedRelationships( Direction.BOTH );
            while ( addedRelationships.hasNext() )
            {
                long rel = addedRelationships.next();
                RelationshipState relState = txState.getRelationshipState( rel );
                relState.accept( (RelationshipVisitor<RuntimeException>)
                        ( relationshipId, typeId, startNodeId, endNodeId ) ->
                        {
                            VirtualRelationshipGroup group = getOrCreateVirtualRelationshipGroup( typeId );
                            // We only need to track new groups, since an iterator is added separately in StateAwareRelationshipGroupCursor
//                            if ( startNodeId == endNodeId )
//                            {
//                                assert startNodeId == relationshipId;
//                                group.loopsAdded.push( relationshipId );
//                            }
//                            else if ( startNodeId == originNodeReference )
//                            {
//                                group.outgoingAdded.push( relationshipId );
//                            }
//                            else // if ( endNodeId == originNodeReference )
//                            {
//                                assert endNodeId == originNodeReference;
//                                group.incomingAdded.push( relationshipId );
//                            }
                        } );
            }
        }
    }

    @Override
    public int relationshipLabel()
    {
        assert currentVirtualGroup != null;
        return currentVirtualGroup.label;
    }

    @Override
    public int outgoingCount()
    {
        return currentVirtualGroup.outgoing.size();
    }

    @Override
    public int incomingCount()
    {
        return currentVirtualGroup.incoming.size();
    }

    @Override
    public int loopCount()
    {
        return currentVirtualGroup.loops.size();
    }

    @Override
    public void outgoing( RelationshipTraversalCursor cursor )
    {
        VirtualRelationshipTraversalCursor virtualCursor = new VirtualRelationshipTraversalCursor();
        virtualCursor.init( currentVirtualGroup.outgoing );
        ((VirtualGroupAwareRelationshipTraversalCursor) cursor).setVirtualCursor( virtualCursor );
    }

    @Override
    public void incoming( RelationshipTraversalCursor cursor )
    {
        VirtualRelationshipTraversalCursor virtualCursor = new VirtualRelationshipTraversalCursor();
        virtualCursor.init( currentVirtualGroup.incoming );
        ((VirtualGroupAwareRelationshipTraversalCursor) cursor).setVirtualCursor( virtualCursor );
    }

    @Override
    public void loops( RelationshipTraversalCursor cursor )
    {
        VirtualRelationshipTraversalCursor virtualCursor = new VirtualRelationshipTraversalCursor();
        virtualCursor.init( currentVirtualGroup.loops );
        ((VirtualGroupAwareRelationshipTraversalCursor) cursor).setVirtualCursor( virtualCursor );
    }

    @Override
    public long outgoingReference()
    {
        if ( currentVirtualGroup.outgoing.size() > 0 )
        {
            return currentVirtualGroup.outgoing.peekAt( 0 );
        }
        return NO_RELATIONSHIP;
    }

    @Override
    public long incomingReference()
    {
        if ( currentVirtualGroup.incoming.size() > 0 )
        {
            return currentVirtualGroup.incoming.peekAt( 0 );
        }
        return NO_RELATIONSHIP;
    }

    @Override
    public long loopsReference()
    {
        if ( currentVirtualGroup.loops.size() > 0 )
        {
            return currentVirtualGroup.loops.peekAt( 0 );
        }
        return NO_RELATIONSHIP;
    }

    @Override
    public Position suspend()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume( Position position )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean next()
    {
        if ( virtualGroupIterator == null )
        {
            virtualGroupIterator = virtualGroups.valueIterator();
        }
        if ( virtualGroupIterator.hasNext() )
        {
            VirtualRelationshipGroup virtualGroup = virtualGroupIterator.next();
            if ( alreadyShadowedPhysicalGroups != null )
            {
                // Skip over already shadowed physical groups
                while ( alreadyShadowedPhysicalGroups.contains( virtualGroup.label ) )
                {
                    if ( !virtualGroupIterator.hasNext() )
                    {
                        return false;
                    }
                    virtualGroup = virtualGroupIterator.next();
                }
            }
            currentVirtualGroup = virtualGroup;
            return true;
        }
        return false;
    }

    @Override
    public boolean shouldRetry()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
    }

    public void shadowPhysicalGroupAt( int label )
    {
        VirtualRelationshipGroup group = virtualGroups.get( label );
        if ( group != null )
        {
            currentVirtualGroup = group;
            if ( alreadyShadowedPhysicalGroups == null )
            {
                alreadyShadowedPhysicalGroups = Primitive.intSet();
            }
            alreadyShadowedPhysicalGroups.add( label );
        }
    }
}
