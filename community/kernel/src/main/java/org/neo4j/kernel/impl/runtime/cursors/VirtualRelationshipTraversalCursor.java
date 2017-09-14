package org.neo4j.kernel.impl.runtime.cursors;

import org.neo4j.collection.primitive.PrimitiveLongStack;

import static org.neo4j.kernel.impl.runtime.cursors.NaiveConstants.NO_RELATIONSHIP;

class VirtualRelationshipTraversalCursor
{
    private int index = NOT_INITIALIZED;
    private PrimitiveLongStack relationships;

    private static int NOT_INITIALIZED = -1;

    public VirtualRelationshipTraversalCursor()
    {
    }

    /**
     * NOTE: This initializes the cursor to point _at_ the first element (instead of before the first element)
     */
    public void init( PrimitiveLongStack relationships )
    {
        this.relationships = relationships;
        index = NOT_INITIALIZED;
        next(); // The user relies on this being pointed at the first element
    }

    private boolean next()
    {
        assert relationships != null;

        // NOTE: This assumes NOT_INITIALIZED is -1, so that incrementing it will make it 0
        if ( index < relationships.size() - 1 )
        {
            index++;
            return true;
        }
        else
        {
            return false;
        }
    }

    public long nextRelationshipReference()
    {
        if ( next() )
        {
            return relationships.peekAt( index );
        }
        return NO_RELATIONSHIP;
    }
}
