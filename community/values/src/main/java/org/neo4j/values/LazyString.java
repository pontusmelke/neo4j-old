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

import static java.lang.String.format;

final class LazyString extends LazyValue<String> implements ValueGroup.VText
{
    LazyString( Values.ValueLoader<String> producer )
    {
        super( producer );
    }

    @Override
    public boolean equals( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    public boolean equals( Value value )
    {
        return value.equals( getOrLoad() );
    }

    @Override
    public boolean equals( byte[] x )
    {
        return false;
    }

    @Override
    public boolean equals( short[] x )
    {
        return false;
    }

    @Override
    public boolean equals( int[] x )
    {
        return false;
    }

    @Override
    public boolean equals( long[] x )
    {
        return false;
    }

    @Override
    public boolean equals( float[] x )
    {
        return false;
    }

    @Override
    public boolean equals( double[] x )
    {
        return false;
    }

    @Override
    public boolean equals( boolean x )
    {
        return false;
    }

    @Override
    public boolean equals( boolean[] x )
    {
        return false;
    }

    @Override
    public boolean equals( char x )
    {
        return false;
    }

    @Override
    public boolean equals( String x )
    {
        return getOrLoad().equals( x );
    }

    @Override
    public boolean equals( char[] x )
    {
        // TODO: do we want to support this?
        return false;
    }

    @Override
    public boolean equals( String[] x )
    {
        return false;
    }

    @Override
    public void writeTo( ValueWriter writer )
    {
        writer.writeString( getOrLoad() );
    }

    @Override
    public Object asObject()
    {
        return getOrLoad();
    }

    @Override
    public int hashCode()
    {
        return getOrLoad().hashCode();
    }

    @Override
    public String toString()
    {
        return format( "LazyString(%s)", valueIsLoaded() ? getOrLoad() : "not-loaded" );
    }

    @Override
    public String stringValue()
    {
        return getOrLoad();
    }

    @Override
    public int compareTo( ValueGroup.VText other )
    {
        return getOrLoad().compareTo( other.stringValue() );
    }
}
