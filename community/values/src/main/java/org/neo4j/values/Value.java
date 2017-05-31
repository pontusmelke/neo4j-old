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

public abstract class Value implements ValueGroup.WithId
{
    @Override
    public abstract boolean equals( Object other );

    @Override
    public abstract int hashCode();

    public abstract boolean equals( Value other );

    public abstract boolean equals( byte[] x );

    public abstract boolean equals( short[] x );

    public abstract boolean equals( int[] x );

    public abstract boolean equals( long[] x );

    public abstract boolean equals( float[] x );

    public abstract boolean equals( double[] x );

    public abstract boolean equals( boolean x );

    public abstract boolean equals( boolean[] x );

    public abstract boolean equals( char x );

    public abstract boolean equals( String x );

    public abstract boolean equals( char[] x );

    public abstract boolean equals( String[] x );

    public abstract void writeTo( ValueWriter writer );

    public abstract Object asObject();

    public long asLong()
    {
        throw new UnsupportedOperationException( toString() + " is not of type long" );
    }

    public int asInt()
    {
        throw new UnsupportedOperationException( toString() + " is not of type int" );
    }

    public short asShort()
    {
        throw new UnsupportedOperationException( toString() + " is not of type short" );
    }

    public char asChar()
    {
        throw new UnsupportedOperationException( toString() + " is not of type char" );
    }

    public byte asByte()
    {
        throw new UnsupportedOperationException( toString() + " is not of type byte" );
    }

    public double asDouble()
    {
        throw new UnsupportedOperationException( toString() + " is not of type double" );
    }

    public float asFloat()
    {
        throw new UnsupportedOperationException( toString() + " is not of type float" );
    }

    public String asString()
    {
        throw new UnsupportedOperationException( toString() + " is not of type String" );
    }

    public boolean asBoolean()
    {
        throw new UnsupportedOperationException( toString() + " is not of type boolean" );
    }

    public long[] asLongArray()
    {
        throw new UnsupportedOperationException( toString() + " is not of type long[]" );
    }

    public int[] asIntArray()
    {
        throw new UnsupportedOperationException( toString() + " is not of type int[]" );
    }

    public char[] asCharArray()
    {
        throw new UnsupportedOperationException( toString() + " is not of type char[]" );
    }

    public short[] asShortArray()
    {
        throw new UnsupportedOperationException( toString() + " is not of type short[]" );
    }

    public byte[] asByteArray()
    {
        throw new UnsupportedOperationException( toString() + " is not of type byte[]" );
    }

    public double[] asDoubleArray()
    {
        throw new UnsupportedOperationException( toString() + " is not of type double[]" );
    }

    public boolean[] asBooleanArray()
    {
        throw new UnsupportedOperationException( toString() + " is not of type boolean[]" );
    }

    public String[] asStringArray()
    {
        throw new UnsupportedOperationException( toString() + " is not of type string[]" );
    }

    public float[] asFloatArray()
    {
        throw new UnsupportedOperationException( toString() + " is not of type float[]" );
    }

    @Override
    public abstract String toString();

}
