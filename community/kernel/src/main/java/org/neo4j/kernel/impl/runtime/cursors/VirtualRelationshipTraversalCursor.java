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

import static org.neo4j.kernel.impl.runtime.cursors.NaiveConstants.NO_RELATIONSHIP;

class VirtualRelationshipTraversalCursor
{
    private int index = NOT_INITIALIZED;
    private PrimitiveLongStack relationships;

    private static int NOT_INITIALIZED = -1;

    VirtualRelationshipTraversalCursor()
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
