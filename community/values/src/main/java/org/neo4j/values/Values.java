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

import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings( "WeakerAccess" )
public class Values
{
    public static final Value EMPTY_BOOLEAN_ARRAY_VALUE = Values.booleanArray(new boolean[0]);
    public static final Value EMPTY_BYTE_ARRAY_VALUE = Values.byteArray( new byte[0] );
    public static final Value EMPTY_SHORT_ARRAY_VALUE = Values.shortArray( new short[0] );
    public static final Value EMPTY_CHAR_ARRAY_VALUE = Values.charArray( new char[0] );
    public static final Value EMPTY_INT_ARRAY_VALUE = Values.intArray( new int[0] );
    public static final Value EMPTY_LONG_ARRAY_VALUE = Values.longArray( new long[0] );
    public static final Value EMPTY_FLOAT_ARRAY_VALUE = Values.floatArray( new float[0] );
    public static final Value EMPTY_DOUBLE_ARRAY_VALUE = Values.doubleArray(  new double[0]);

    public static final Value MIN_NUMBER = Values.doubleValue( Double.NEGATIVE_INFINITY );
    public static final Value MAX_NUMBER = Values.doubleValue( Double.NaN );
    public static final Value MIN_STRING = Values.stringValue( "" );
    public static final Value MAX_STRING = Values.booleanValue( false );

    private Values()
    {
    }

    private static final Map<Class<?>, Value> EMPTY_ARRAYS = new HashMap<>(  );

    static
    {
        EMPTY_ARRAYS.put( boolean.class, EMPTY_BOOLEAN_ARRAY_VALUE );
        EMPTY_ARRAYS.put( byte.class, EMPTY_BYTE_ARRAY_VALUE );
        EMPTY_ARRAYS.put( short.class, EMPTY_SHORT_ARRAY_VALUE );
        EMPTY_ARRAYS.put( char.class, EMPTY_CHAR_ARRAY_VALUE );
        EMPTY_ARRAYS.put( int.class, EMPTY_INT_ARRAY_VALUE );
        EMPTY_ARRAYS.put( float.class, EMPTY_FLOAT_ARRAY_VALUE );
        EMPTY_ARRAYS.put( double.class, EMPTY_DOUBLE_ARRAY_VALUE );
    }

    public static Value emptyArray( Class<?> primitiveClass )
    {
        return EMPTY_ARRAYS.getOrDefault( primitiveClass, Values.of( Array.newInstance( primitiveClass, 0 ) ) );
    }

    public interface ValueLoader<T>
    {
        T load() throws ValueLoadException;
    }

    public class ValueLoadException extends RuntimeException
    {
    }

    public static ValueComparator VALUE_COMPARATOR =
            new ValueComparator( Comparator.comparingInt( ValueGroup.Id::comparabilityGroup ) );

    // DIRECT FACTORY METHODS

    public static Value NO_VALUE = NoValue.NO_VALUE;

    public static Value stringValue( String value )
    {
        if ( value == null )
        {
            return NO_VALUE;
        }
        return new DirectString( value );
    }

    public static Value lazyStringValue( ValueLoader<String> producer )
    {
        return new LazyString( producer );
    }

    public static Value lazyByteArray( ValueLoader<byte[]> producer )
    {
        return new LazyByteArray( producer );
    }

    public static Value lazyShortArray( ValueLoader<short[]> producer )
    {
        return new LazyShortArray( producer );
    }

    public static Value lazyIntArray( ValueLoader<int[]> producer )
    {
        return new LazyIntArray( producer );
    }

    public static Value lazyLongArray( ValueLoader<long[]> producer )
    {
        return new LazyLongArray( producer );
    }

    public static Value lazyFloatArray( ValueLoader<float[]> producer )
    {
        return new LazyFloatArray( producer );
    }

    public static Value lazyDoubleArray( ValueLoader<double[]> producer )
    {
        return new LazyDoubleArray( producer );
    }

    public static Value lazyCharArray( ValueLoader<char[]> producer )
    {
        return new LazyCharArray( producer );
    }

    public static Value lazyStringArray( ValueLoader<String[]> producer )
    {
        return new LazyStringArray( producer );
    }

    public static Value lazyBooleanArray( ValueLoader<boolean[]> producer )
    {
        return new LazyBooleanArray( producer );
    }

    public static Value numberValue( Number number )
    {
        if ( number instanceof Long )
        {
            return longValue( number.longValue() );
        }
        if ( number instanceof Integer )
        {
            return intValue( number.intValue() );
        }
        if ( number instanceof Double )
        {
            return doubleValue( number.doubleValue() );
        }
        if ( number instanceof Byte )
        {
            return byteValue( number.byteValue() );
        }
        if ( number instanceof Float )
        {
            return floatValue( number.floatValue() );
        }
        if ( number instanceof Short )
        {
            return shortValue( number.shortValue() );
        }
        if ( number == null )
        {
            return NO_VALUE;
        }

        throw new UnsupportedOperationException( "Unsupported type of Number " + number.toString() );
    }

    public static Value longValue( long value )
    {
        return new DirectLong( value );
    }

    public static Value intValue( int value )
    {
        return new DirectInt( value );
    }

    public static Value shortValue( short value )
    {
        return new DirectShort( value );
    }

    public static Value byteValue( byte value )
    {
        return new DirectByte( value );
    }

    public static Value booleanValue( boolean value )
    {
        return new DirectBoolean( value );
    }

    public static Value charValue( char value )
    {
        return new DirectChar( value );
    }

    public static Value doubleValue( double value )
    {
        return new DirectDouble( value );
    }

    public static Value floatValue( float value )
    {
        return new DirectFloat( value );
    }

    public static Value stringArray( String[] value )
    {
        return new DirectStringArray( value );
    }

    public static Value byteArray( byte[] value )
    {
        return new DirectByteArray( value );
    }

    public static Value longArray( long[] value )
    {
        return new DirectLongArray( value );
    }

    public static Value intArray( int[] value )
    {
        return new DirectIntArray( value );
    }

    public static Value doubleArray( double[] value )
    {
        return new DirectDoubleArray( value );
    }

    public static Value floatArray( float[] value )
    {
        return new DirectFloatArray( value );
    }

    public static Value booleanArray( boolean[] value )
    {
        return new DirectBooleanArray( value );
    }

    public static Value charArray( char[] value )
    {
        return new DirectCharArray( value );
    }

    public static Value shortArray( short[] value )
    {
        return new DirectShortArray( value );
    }

    // Utilities
    public static boolean isArray( Value value )
    {
        int group = value.valueGroupId().comparabilityGroup();

        return group >= ValueGroup.Id.INTEGER_ARRAY.comparabilityGroup() &&
               group <= ValueGroup.Id.BOOLEAN_ARRAY.comparabilityGroup();
    }

    // BOXED FACTORY METHODS

    public static Value of( Object value )
    {
        if ( value instanceof String )
        {
            return stringValue( (String) value );
        }
        if ( value instanceof Object[] )
        {
            return arrayValue( (Object[]) value );
        }
        if ( value instanceof Long )
        {
            return longValue( (Long) value );
        }
        if ( value instanceof Integer )
        {
            return intValue( (Integer) value );
        }
        if ( value instanceof Boolean )
        {
            return booleanValue( (Boolean) value );
        }
        if ( value instanceof Double )
        {
            return doubleValue( (Double) value );
        }
        if ( value instanceof Float )
        {
            return floatValue( (Float) value );
        }
        if ( value instanceof Short )
        {
            return shortValue( (Short) value );
        }
        if ( value instanceof Byte )
        {
            return byteValue( (Byte) value );
        }
        if ( value instanceof Character )
        {
            return charValue( (Character) value );
        }
        if ( value instanceof byte[] )
        {
            return byteArray( ((byte[]) value).clone() );
        }
        if ( value instanceof long[] )
        {
            return longArray( ((long[]) value).clone() );
        }
        if ( value instanceof int[] )
        {
            return intArray( ((int[]) value).clone() );
        }
        if ( value instanceof double[] )
        {
            return doubleArray( ((double[]) value).clone() );
        }
        if ( value instanceof float[] )
        {
            return floatArray( ((float[]) value).clone() );
        }
        if ( value instanceof boolean[] )
        {
            return booleanArray( ((boolean[]) value).clone() );
        }
        if ( value instanceof char[] )
        {
            return charArray( ((char[]) value).clone() );
        }
        if ( value instanceof short[] )
        {
            return shortArray( ((short[]) value).clone() );
        }
        if ( value == null )
        {
            return NoValue.NO_VALUE;
        }
        // otherwise fail
        throw new IllegalArgumentException(
                    String.format( "[%s:%s] is not a supported property value", value, value.getClass().getName() ) );
    }

    private static Value arrayValue( Object[] value )
    {
        if ( value instanceof String[] )
        {
            return stringArray( copy( value, new String[value.length] ) );
        }
        if ( value instanceof Byte[] )
        {
            return byteArray( copy( value, new byte[value.length] ) );
        }
        if ( value instanceof Long[] )
        {
            return longArray( copy( value, new long[value.length] ) );
        }
        if ( value instanceof Integer[] )
        {
            return intArray( copy( value, new int[value.length] ) );
        }
        if ( value instanceof Double[] )
        {
            return doubleArray( copy( value, new double[value.length] ) );
        }
        if ( value instanceof Float[] )
        {
            return floatArray( copy( value, new float[value.length] ) );
        }
        if ( value instanceof Boolean[] )
        {
            return booleanArray( copy( value, new boolean[value.length] ) );
        }
        if ( value instanceof Character[] )
        {
            return charArray( copy( value, new char[value.length] ) );
        }
        if ( value instanceof Short[] )
        {
            return shortArray( copy( value, new short[value.length] ) );
        }
        throw new IllegalArgumentException(
                String.format( "%s[] is not a supported property value type",
                               value.getClass().getComponentType().getName() ) );
    }

    private static <T> T copy( Object[] value, T target )
    {
        for ( int i = 0; i < value.length; i++ )
        {
            if ( value[i] == null )
            {
                throw new IllegalArgumentException( "Property array value elements may not be null." );
            }
            Array.set( target, i, value[i] );
        }
        return target;
    }
}
