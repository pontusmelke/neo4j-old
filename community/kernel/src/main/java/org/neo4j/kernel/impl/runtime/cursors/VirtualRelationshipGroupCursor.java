package org.neo4j.kernel.impl.runtime.cursors;

import java.util.Iterator;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

import static org.neo4j.kernel.impl.runtime.cursors.NaiveConstants.NO_RELATIONSHIP;

class VirtualRelationshipGroupCursor implements RelationshipGroupCursor
{
    private PrimitiveIntObjectMap<VirtualRelationshipGroup> virtualGroups;
    private Iterator<VirtualRelationshipGroup> virtualGroupIterator;
    private VirtualRelationshipGroup currentVirtualGroup;

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
                group.label = label;
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
            currentVirtualGroup = virtualGroupIterator.next();
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
