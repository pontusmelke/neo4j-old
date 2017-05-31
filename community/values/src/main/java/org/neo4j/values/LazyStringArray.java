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

import java.util.Arrays;

final class LazyStringArray extends LazyTextArray<String[]>
{
    LazyStringArray( Values.ValueLoader<String[]> producer )
    {
        super( producer );
    }

    @Override
    public int hashCode()
    {
        return TextValues.hash( getOrLoad() );
    }

    @Override
    public boolean equals( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    public boolean equals( Value other )
    {
        return other.equals( getOrLoad() );
    }

    @Override
    public boolean equals( char[] x )
    {
        return PrimitiveArrayValues.equals( x, getOrLoad() );
    }

    @Override
    public boolean equals( String[] x )
    {
        return Arrays.equals( getOrLoad(), x );
    }

    @Override
    public int compareTo( ValueGroup.VTextArray other )
    {
        return TextValues.compareTextArrays( this, other );
    }

    @Override
    public void writeTo( ValueWriter writer )
    {
        PrimitiveArrayWriting.writeTo( writer, getOrLoad() );
    }

    @Override
    public Object asObject()
    {
        return getOrLoad().clone();
    }

    @Override
    public String[] asStringArray()
    {
        return getOrLoad().clone();
    }

    @Override
    public String toString()
    {
        return "StringArray()";
    }

    @Override
    public int length()
    {
        return getOrLoad().length;
    }

    @Override
    public String stringValue( int offset )
    {
        return getOrLoad()[offset];
    }
}
