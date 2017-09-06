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

class NaiveBitManipulation
{
    private NaiveBitManipulation()
    {
    }

    private static final long INTEGER_MINUS_ONE = 0xFFFF_FFFFL;

    static int nextPowerOfTwo( int v )
    {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }

    static long combineReference( long base, long modifier )
    {
        return modifier == 0 && base == INTEGER_MINUS_ONE ? -1 : base | modifier;
    }

    static int lcm( int a, int b )
    {
        return (a / gcd( a, b )) * b;
    }

    private static int gcd( int a, int b )
    {
        return a == b ? a : a > b ? gcd( a - b, b ) : gcd( a, b - a );
    }
}
