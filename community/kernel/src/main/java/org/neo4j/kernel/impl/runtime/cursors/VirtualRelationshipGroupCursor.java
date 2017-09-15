package org.neo4j.kernel.impl.runtime.cursors;

import java.util.Iterator;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongStack;
import org.neo4j.collection.primitive.hopscotch.PrimitiveIntHashSet;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.ReadableRelationshipDiffSets;
import org.neo4j.storageengine.api.txstate.RelationshipState;

import static org.neo4j.kernel.impl.runtime.cursors.NaiveConstants.NO_RELATIONSHIP;

class VirtualRelationshipGroupCursor implements RelationshipGroupCursor
{
    private PrimitiveIntObjectMap<VirtualRelationshipGroup> virtualGroups;
    private Iterator<VirtualRelationshipGroup> virtualGroupIterator;
    private VirtualRelationshipGroup currentVirtualGroup;
    private PrimitiveIntSet alreadyShadowedPhysicalGroups;

    public VirtualRelationshipGroupCursor()
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
                try
                {
                    relState.accept( (RelationshipVisitor<Exception>)
                            ( relationshipId, typeId, startNodeId, endNodeId ) -> {
                                VirtualRelationshipGroup group = getOrCreateVirtualRelationshipGroup( typeId );
                                if ( startNodeId == endNodeId )
                                {
                                    assert startNodeId == relationshipId;
                                    group.loops.push( relationshipId );
                                }
                                else if ( startNodeId == relationshipId )
                                {
                                    group.outgoing.push( relationshipId );
                                }
                                else // if ( endNodeId == relationshipId )
                                {
                                    assert endNodeId == relationshipId;
                                    group.incoming.push( relationshipId );
                                }
                            } );
                }
                catch ( Exception e )
                {
                }
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
