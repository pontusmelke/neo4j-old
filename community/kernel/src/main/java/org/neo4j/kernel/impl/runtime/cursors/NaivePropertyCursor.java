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

import java.util.regex.Pattern;

import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.runtime.NaiveKernel;
import org.neo4j.kernel.impl.store.LongerShortString;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.runtime.cursors.NaiveBitManipulation.combineReference;
import static org.neo4j.kernel.impl.store.LongerShortString.ENCODINGS;

public class NaivePropertyCursor extends PageCacheBackedCursor implements PropertyCursor
{
    /**
     * Property record byte layout
     *
     * <pre>
     *  0: high bits  ( 1 byte)
     *  1: next       ( 4 bytes)    where new property records are added
     *  5: prev       ( 4 bytes)    points to more PropertyRecords in this chain
     *  9: payload    (32 bytes - 4 x 8 byte blocks)
     * </pre>
     * <h2>high bits</h2>
     * <pre>
     * [    ,xxxx] high(next)
     * [xxxx,    ] high(prev)
     * </pre>
     * <h2>block structure</h2>
     * <pre>
     * [][][][] [    ,xxxx] [    ,    ] [    ,    ] [    ,    ] type (0x0000_0000_0F00_0000)
     * [][][][] [    ,    ] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] key  (0x0000_0000_00FF_FFFF)
     * </pre>
     * <h2>property types</h2>
     * <pre>
     *  1: BOOL
     *  2: BYTE
     *  3: SHORT
     *  4: CHAR
     *  5: INT
     *  6: LONG
     *  7: FLOAT
     *  8: DOUBLE
     *  9: STRING REFERENCE
     * 10: ARRAY  REFERENCE
     * 11: SHORT STRING
     * 12: SHORT ARRAY
     * </pre>
     * <h2>value formats</h2>
     * <pre>
     * BOOL:      [    ,    ] [    ,    ] [    ,    ] [    ,    ] [   x,type][K][K][K]           (0x0000_0000_1000_0000)
     * BYTE:      [    ,    ] [    ,    ] [    ,    ] [    ,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0000_000F_F000_0000)
     * SHORT:     [    ,    ] [    ,    ] [    ,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0000_0FFF_F000_0000)
     * CHAR:      [    ,    ] [    ,    ] [    ,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0000_0FFF_F000_0000)
     * INT:       [    ,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0FFF_FFFF_F000_0000)
     * LONG:      [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxx1,type][K][K][K] inline>>29(0xFFFF_FFFF_E000_0000)
     * LONG:      [    ,    ] [    ,    ] [    ,    ] [    ,    ] [   0,type][K][K][K] value in next long block
     * FLOAT:     [    ,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0FFF_FFFF_F000_0000)
     * DOUBLE:    [    ,    ] [    ,    ] [    ,    ] [    ,    ] [    ,type][K][K][K] value in next long block
     * REFERENCE: [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0xFFFF_FFFF_F000_0000)
     * SHORT STR: [    ,    ] [    ,    ] [    ,    ] [    ,   x] [xxxx,type][K][K][K] encoding  (0x0000_0001_F000_0000)
     *            [    ,    ] [    ,    ] [    ,    ] [ xxx,xxx ] [    ,type][K][K][K] length    (0x0000_007E_0000_0000)
     *            [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [x   ,    ] payload(+ maybe in next block) (0xFFFF_FF80_0000_0000)
     *                                                            bits are densely packed, bytes torn across blocks
     * SHORT ARR: [    ,    ] [    ,    ] [    ,    ] [    ,    ] [xxxx,type][K][K][K] data type (0x0000_0000_F000_0000)
     *            [    ,    ] [    ,    ] [    ,    ] [  xx,xxxx] [    ,type][K][K][K] length    (0x0000_003F_0000_0000)
     *            [    ,    ] [    ,    ] [    ,xxxx] [xx  ,    ] [    ,type][K][K][K] bits/item (0x0000_003F_0000_0000)
     *                                                                                 0 means 64, other values "normal"
     *            [xxxx,xxxx] [xxxx,xxxx] [xxxx,    ] [    ,    ] payload(+ maybe in next block) (0xFFFF_FF00_0000_0000)
     *                                                            bits are densely packed, bytes torn across blocks
     * </pre>
     */

    public static final int RECORD_SIZE = 41;
    private static final int BOOL = 1, BYTE = 2, SHORT = 3, CHAR = 4, INT = 5, LONG = 6, FLOAT = 7, DOUBLE = 8,
            STRING_REFERENCE = 9, ARRAY_REFERENCE = 10, SHORT_STRING = 11, SHORT_ARRAY = 12;

    private int block;

    @Override
    final int recordSize()
    {
        return RECORD_SIZE;
    }

    @Override
    final int pageSize()
    {
        return NaiveKernel.PROPERTY_STORE_PAGE_SIZE;
    }

    public void init( PageCursor pageCursor, long initialAddress )
    {
        initJumpingCursor( pageCursor, initialAddress );
    }

    @Override
    public boolean next()
    {
        if ( isUnbound() )
        {
            return false;
        }
        if ( firstJump() )
        {
            block = 0;
            return true;
        }

        int nextBlock = block + blocksUsedByCurrent();
        while ( nextBlock >= 0 && nextBlock < 4 )
        {
            block = nextBlock;
            if ( typeIdentifier() != 0 )
            {
                return true;
            }
            nextBlock = block + blocksUsedByCurrent();
        }

        block = 0;
        return jumpToAddress( prevPropertyRecordReference() );
    }

    protected void onClose()
    {
        // This is done for debugging sanity, but is probably not needed.
        block = -1;
    }

    // DATA ACCESSOR METHODS

    @Override
    public int propertyKey()
    {
        return (int) (block( block ) & 0x00FF_FFFFL);
    }

    private int typeIdentifier()
    {
        return (int) ((block( block ) & 0x0F00_0000L) >> 24);
    }

    private long block( int offset )
    {
        return readLong( 9 + Long.BYTES * offset );
    }

    private long nextPropertyRecordReference()
    {
        return combineReference( unsignedInt( 1 ), ((long) unsignedByte( 0 ) & 0x0FL) << 32 );
    }

    private long prevPropertyRecordReference()
    {
        return combineReference( unsignedInt( 5 ), ((long) unsignedByte( 0 ) & 0xF0L) << 31 );
    }

    @Override
    public ValueGroup propertyType()
    {
        switch ( typeIdentifier() )
        {
        case BOOL:
            return ValueGroup.BOOLEAN;
        case BYTE:
            return ValueGroup.NUMBER;
        case SHORT:
            return ValueGroup.NUMBER;
        case CHAR:
            return ValueGroup.TEXT;
        case INT:
            return ValueGroup.NUMBER;
        case LONG:
            return ValueGroup.NUMBER;
        case FLOAT:
            return ValueGroup.NUMBER;
        case DOUBLE:
            return ValueGroup.NUMBER;
        case STRING_REFERENCE:
            return ValueGroup.TEXT;
        case ARRAY_REFERENCE:
            throw new UnsupportedOperationException( "not implemented" );
        case SHORT_STRING:
            return ValueGroup.TEXT;
        case SHORT_ARRAY:
            throw new UnsupportedOperationException( "not implemented" );
        default:
            return null;
        }
    }

    @Override
    public Value propertyValue()
    {
        long valueBytes = block( this.block );
        switch ( typeIdentifier() )
        {
        case BOOL:
            return Values.booleanValue( (valueBytes & 0x0000_0000_1000_0000) != 0 );
        case BYTE:
            return Values.byteValue( (byte)((valueBytes & 0x0000_000F_F000_0000L) >> 28) );
        case SHORT:
            return Values.shortValue( (short)((valueBytes & 0x0000_0FFF_F000_0000L) >> 28) );
        case CHAR:
            return Values.charValue( (char)((valueBytes & 0x0000_0FFF_F000_0000L) >> 28) );
        case INT:
            return Values.intValue( (int)((valueBytes & 0x0FFF_FFFF_F000_0000L) >> 28 ) );
        case LONG:
            if ( ( valueBytes & 0x0000_0000_1000_0000 ) == 0 )
            {
                if ( moreBlocksInRecord() )
                {
                    return Values.longValue( block( this.block + 1 ) );
                }
                else
                {
                    throw new UnsupportedOperationException( "not implemented" ); // long bytes in next record
                }
            }
            return Values.longValue( (valueBytes & 0xFFFF_FFFF_E000_0000L) >> 29 );
        case FLOAT:
            return Values.floatValue( Float.intBitsToFloat((int)((valueBytes & 0x0FFF_FFFF_F000_0000L) >> 28) ) );
        case DOUBLE:
            return Values.doubleValue( Double.longBitsToDouble( block( this.block + 1 ) ) );
        case STRING_REFERENCE:
            throw new UnsupportedOperationException( "not implemented" );
        case ARRAY_REFERENCE:
            throw new UnsupportedOperationException( "not implemented" );
        case SHORT_STRING:
            int encoding = shortStringEncoding( valueBytes );
            int stringLength = shortStringLength( valueBytes );
            if ( encoding == LongerShortString.ENCODING_UTF8 )
            {
                return Values.stringValue( decodeUTF8( stringLength ) );
            }
            if ( encoding == LongerShortString.ENCODING_LATIN1 )
            {
                return Values.stringValue( decodeLatin1( stringLength ) );
            }

            LongerShortString table = LongerShortString.getEncodingTable( encoding );
            return Values.stringValue( decode( stringLength, table ) );

        case SHORT_ARRAY:
            throw new UnsupportedOperationException( "not implemented" );
        default:
            return null;
        }
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> target )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean booleanValue()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public String stringValue()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long longValue()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public double doubleValue()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueEqualTo( long value )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueEqualTo( double value )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueEqualTo( String value )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueMatches( Pattern regex )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueGreaterThan( long number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueGreaterThan( double number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueLessThan( long number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueLessThan( double number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueGreaterThanOrEqualTo( long number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueGreaterThanOrEqualTo( double number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueLessThanOrEqualTo( long number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueLessThanOrEqualTo( double number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    // HELPERS

    private boolean moreBlocksInRecord()
    {
        return block < 3;
    }

    private int blocksUsedByCurrent()
    {
        if ( block == -1 )
        {
            return 1;
        }
        long valueBytes = block( this.block );
        long typeId = (valueBytes & 0x1F00_0000L) >> 24;
        if ( typeId == DOUBLE ||
                (typeId == LONG && ( valueBytes & 0x0000_0000_1000_0000 ) == 0 ) )
        {
            if ( moreBlocksInRecord() )
            {
                return 2;
            }
            else
            {
                throw new UnsupportedOperationException( "not implemented" ); // long/double bytes in next record
            }
        }
        if ( typeId == SHORT_STRING )
        {
            int encoding = shortStringEncoding( valueBytes );
            int stringLength = shortStringLength( valueBytes );

            if ( encoding == LongerShortString.ENCODING_UTF8 || encoding == LongerShortString.ENCODING_LATIN1 )
            {
                return LongerShortString.calculateNumberOfBlocksUsedForStep8( stringLength );
            }

            return LongerShortString.calculateNumberOfBlocksUsed( ENCODINGS[ encoding - 1 ], stringLength );
        }
        return 1;
    }

    // SHORT STRING DECODE

    private int shortStringEncoding( long valueBytes )
    {
        return (int) ((valueBytes & 0x0001_F000_0000L) >>> 28); // 5 bits of encoding
    }

    private int shortStringLength( long valueBytes )
    {
        return (int) ((valueBytes & 0x007E_0000_0000L) >>> 33); // 6 bits of stringLength
    }

    private String decodeUTF8( int stringLength )
    {
        byte[] result = new byte[stringLength];
        int blockIndex = this.block;
        int maskShift = LongerShortString.HEADER_SIZE;
        for ( int i = 0; i < result.length; i++ )
        {
            byte codePoint = (byte) (block(blockIndex) >>> maskShift);
            maskShift += 8;
            if ( maskShift >= 64 )
            {
                maskShift %= 64;
                codePoint |= (block(++blockIndex) & (0xFF >>> (8 - maskShift))) << (8 - maskShift);
            }
            result[i] = codePoint;
        }
        return UTF8.decode( result );
    }

    private String decodeLatin1( int stringLength )
    {
        StringBuilder sb = new StringBuilder( stringLength );
        int blockIndex = this.block;
        int maskShift = LongerShortString.HEADER_SIZE;
        for ( int i = 0; i < stringLength; i++ )
        {
            char codePoint = (char) ((block(blockIndex) >>> maskShift) & 0xFF);
            maskShift += 8;
            if ( maskShift >= 64 )
            {
                maskShift %= 64;
                codePoint |= (block(++blockIndex) & (0xFF >>> (8 - maskShift))) << (8 - maskShift);
            }
            sb.append( codePoint );
        }
        return sb.toString();
    }

    private String decode( int stringLength, LongerShortString table )
    {
        StringBuilder sb = new StringBuilder( stringLength );
        int blockIndex = this.block;
        int maskShift = LongerShortString.HEADER_SIZE;
        long baseMask = table.mask;
        for ( int i = 0; i < stringLength; i++ )
        {
            byte codePoint = (byte) ((block(blockIndex) >>> maskShift) & baseMask);
            maskShift += table.step;
            if ( maskShift >= 64 && blockIndex + 1 < 4 )
            {
                maskShift %= 64;
                codePoint |= (block(++blockIndex) & (baseMask >>> (table.step - maskShift))) << (table.step - maskShift);
            }
            sb.append( table.decTranslate( codePoint ) );
        }
        return sb.toString();
    }
}
