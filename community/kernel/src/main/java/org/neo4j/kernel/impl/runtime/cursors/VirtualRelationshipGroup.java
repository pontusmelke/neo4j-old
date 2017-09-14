package org.neo4j.kernel.impl.runtime.cursors;

import org.neo4j.collection.primitive.PrimitiveLongStack;

class VirtualRelationshipGroup
{
    PrimitiveLongStack outgoing;
    PrimitiveLongStack incoming;
    PrimitiveLongStack loops;

    public VirtualRelationshipGroup()
    {
        // TODO: Calculate reasonable initial sizes based on dense node threshold setting
        outgoing = new PrimitiveLongStack( 8 );
        incoming = new PrimitiveLongStack( 8 );
        loops = new PrimitiveLongStack( 4 );
    }
}
