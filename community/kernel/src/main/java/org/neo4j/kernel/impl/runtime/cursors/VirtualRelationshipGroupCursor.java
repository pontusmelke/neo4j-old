package org.neo4j.kernel.impl.runtime.cursors;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

import static org.neo4j.kernel.impl.runtime.cursors.NaiveConstants.NO_RELATIONSHIP;

class VirtualRelationshipGroupCursor implements RelationshipGroupCursor
{
    private PrimitiveIntObjectMap<VirtualRelationshipGroup> virtualGroups;
    private PrimitiveIntIterator virtualGroupIterator;
    private int currentVirtualGroupLabel;
    private VirtualRelationshipGroup currentVirtualGroup;

    private static int NOT_INITIALIZED = -1;

    public VirtualRelationshipGroupCursor()
    {
        virtualGroups = Primitive.intObjectMap();
        currentVirtualGroupLabel = NOT_INITIALIZED;
    }

    public void init( NaiveRelationshipTraversalCursor cursor )
    {
        readDirectRelationshipsIntoVirtualGroups( cursor );
    }

    private void readDirectRelationshipsIntoVirtualGroups( NaiveRelationshipTraversalCursor cursor )
    {
        VirtualRelationshipGroup group;

        while ( cursor.next() )
        {
            int label = cursor.label();
            group = virtualGroups.get( label );
            if ( group == null )
            {
                // Create a new virtual group if it does not exist
                group = new VirtualRelationshipGroup();
                virtualGroups.put( label, group );
            }
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

    @Override
    public int relationshipLabel()
    {
        assert currentVirtualGroupLabel != NOT_INITIALIZED;
        return currentVirtualGroupLabel;
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
            virtualGroupIterator = virtualGroups.iterator();
        }
        if ( virtualGroupIterator.hasNext() )
        {
            // TODO: We would like to iterate over values directly, but the collection does not allow us to do that
            currentVirtualGroupLabel = virtualGroupIterator.next();
            currentVirtualGroup = virtualGroups.get( currentVirtualGroupLabel );
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
}
