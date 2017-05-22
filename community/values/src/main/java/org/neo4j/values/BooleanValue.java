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
package org.neo4j.values;

/**
 * This does not extend AbstractProperty since the JVM can take advantage of the 4 byte initial field alignment if
 * we don't extend a class that has fields.
 */
final class BooleanValue extends ScalarValue
{
    private final boolean bool;

    BooleanValue( boolean bool )
    {
        this.bool = bool;
    }

    @Override
    public boolean equals( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    public boolean equals( Value other )
    {
        return other instanceof BooleanValue && bool == ((BooleanValue) other).bool;
    }

    @Override
    boolean equals( boolean x )
    {
        return bool == x;
    }

    @Override
    boolean equals( char x )
    {
        return false;
    }

    @Override
    boolean equals( String x )
    {
        return false;
    }

    @Override
    public int hashCode()
    {
        return bool ? -1 : 0;
    }
}
