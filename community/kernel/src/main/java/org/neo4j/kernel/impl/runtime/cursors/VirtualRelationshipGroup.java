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

    VirtualRelationshipGroup()
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