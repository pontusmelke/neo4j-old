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

import static java.lang.String.format;

final class DirectStringArray extends DirectTextArray
{
    final String[] value;

    DirectStringArray( String[] value )
    {
        assert value != null;
        this.value = value;
    }

    @Override
    public int length()
    {
        return value.length;
    }

    @Override
    public String stringValue( int offset )
    {
        return value[offset];
    }

    @Override
    public boolean equals( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    public boolean equals( Value other )
    {
        return other.equals( value );
    }

    @Override
    public boolean equals( char[] x )
    {
        return PrimitiveArrayValues.equals( x, value );
    }

    @Override
    public boolean equals( String[] x )
    {
        return Arrays.equals( value, x );
    }

    @Override
    public int hashCode()
    {
        return TextValues.hash( value );
    }

    @Override
    public void writeTo( ValueWriter writer )
    {
        PrimitiveArrayWriting.writeTo( writer, value );
    }

    @Override
    public Object asObject()
    {
        return value.clone();
    }

    @Override
    public String[] asStringArray()
    {
        return value.clone();
    }

    @Override
    public String toString()
    {
        return format( "StringArray(%s)", Arrays.toString( value ) );
    }

    public int compareTo( ValueGroup.VTextArray other )
    {
        return TextValues.compareTextArrays( this, other );
    }
}
