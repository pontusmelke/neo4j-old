package org.neo4j.kernel.impl.runtime.cursors;

import org.neo4j.collection.primitive.PrimitiveLongStack;

class VirtualRelationshipGroup
{
    int label;
    PrimitiveLongStack outgoing;
    PrimitiveLongStack incoming;
    PrimitiveLongStack loops;

    // TODO: FIXME We should not need these
    PrimitiveLongStack outgoingAdded;
    PrimitiveLongStack incomingAdded;
    PrimitiveLongStack loopsAdded;

    public VirtualRelationshipGroup()
    {
        // TODO: Calculate reasonable initial sizes based on dense node threshold setting
        outgoing = new PrimitiveLongStack( 8 );
        incoming = new PrimitiveLongStack( 8 );
        loops = new PrimitiveLongStack( 4 );

        outgoingAdded = new PrimitiveLongStack( 8 );
        incomingAdded = new PrimitiveLongStack( 8 );
        loopsAdded = new PrimitiveLongStack( 4 );
    }
}
