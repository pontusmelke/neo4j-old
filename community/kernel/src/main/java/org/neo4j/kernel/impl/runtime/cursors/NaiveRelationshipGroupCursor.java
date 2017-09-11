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

import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.kernel.impl.runtime.NaiveKernel.RELATIONSHIP_GROUP_STORE_PAGE_SIZE;
import static org.neo4j.kernel.impl.runtime.cursors.NaiveBitManipulation.combineReference;
import static org.neo4j.kernel.impl.runtime.cursors.NaiveConstants.NO_RELATIONSHIP;

public class NaiveRelationshipGroupCursor extends PageCacheBackedCursor implements RelationshipGroupCursor
{
    /**
     * <pre>
     *  0: in_use, high_1 (1 bytes)
     *  1: high_2         (1 bytes)
     *  2: type           (2 bytes)
     *  4: next           (4 bytes)
     *  8: out            (4 bytes)
     * 12: in             (4 bytes)
     * 16: loop           (4 bytes)
     * 20: node           (4 bytes)
     * 24: node_high      (1 bytes)
     * </pre>
     * <h2>high_1</h2>
     * <pre>
     * [    ,   x] in_use
     * [    ,xxx ] high(next)
     * [ xxx,    ] high(out)
     * </pre>
     * <h2>high_2</h2>
     * <pre>
     * [    ,xxx ] high(in)
     * [ xxx,    ] high(loop)
     * </pre>
     */
    static final int RECORD_SIZE = 25;
    /** used for accessing counts */
    private final NaiveRelationshipTraversalCursor innerHelper;
    private Read read;
    private long originNodeReference;

    NaiveRelationshipGroupCursor( NaiveRelationshipTraversalCursor innerHelper )
    {
        this.innerHelper = innerHelper;
    }

    @Override
    protected int recordSize()
    {
        return RECORD_SIZE;
    }

    @Override
    protected int pageSize()
    {
        return RELATIONSHIP_GROUP_STORE_PAGE_SIZE;
    }

    public void init( PageCursor pageCursor, long originNodeReference, long initialAddress, Read read )
    {
        this.read = read;
        this.originNodeReference = originNodeReference;
        initJumpingCursor( pageCursor, initialAddress );
    }

    @Override
    protected void onClose()
    {
        innerHelper.close();
        read = null;
        originNodeReference = -1;
    }

    @Override
    public Position suspend()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void resume( Position position )
    {
        throw new UnsupportedOperationException( "not implemented" );
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
            return true;
        }

        return jumpToAddress( nextReference() );
    }

    @Override
    public int relationshipLabel()
    {
        return unsignedShort( 2 );
    }

    @Override
    public int outgoingCount()
    {
        return count( outgoingReference(), true );
    }

    @Override
    public int incomingCount()
    {
        return count( incomingReference(), false );
    }

    @Override
    public int loopCount()
    {
        return count( loopsReference(), true );
    }

    private int count( long relationshipReference, boolean source )
    {
        if ( NO_RELATIONSHIP == relationshipReference )
        {
            return 0;
        }
        try ( NaiveRelationshipTraversalCursor relationship = this.innerHelper )
        {
            read.relationships( nodeReference(), relationshipReference, relationship );
            if ( !relationship.next() )
            {
                return 0;
            }
            return source ? (int) relationship.sourcePrevRelationshipReference()
                          : (int) relationship.targetPrevRelationshipReference();
        }
    }

    private long nextReference()
    {
        return combineReference( unsignedInt( 4 ), ((long) (unsignedByte( 0 ) & 0x0E)) << 31 );
    }

    @Override
    public long outgoingReference()
    {
        return combineReference( unsignedInt( 8 ), ((long) (unsignedByte( 0 ) & 0x70)) << 28 );
    }

    @Override
    public long incomingReference()
    {
        return combineReference( unsignedInt( 12 ), ((long) (unsignedByte( 1 ) & 0x0E)) << 31 );
    }

    @Override
    public long loopsReference()
    {
        return combineReference( unsignedInt( 16 ), ((long) (unsignedByte( 1 ) & 0x70)) << 28 );
    }

    private long nodeReference()
    {
        return combineReference( unsignedInt( 20 ), ((long) unsignedByte( 24 )) << 32 );
    }

    private long originNodeReference()
    {
        return originNodeReference;
    }

    @Override
    public void outgoing( RelationshipTraversalCursor cursor )
    {
        read.relationships( originNodeReference(), outgoingReference(), cursor );
    }

    @Override
    public void incoming( RelationshipTraversalCursor cursor )
    {
        read.relationships( originNodeReference(), incomingReference(), cursor );
    }

    @Override
    public void loops( RelationshipTraversalCursor cursor )
    {
        read.relationships( originNodeReference(), loopsReference(), cursor );
    }
}
