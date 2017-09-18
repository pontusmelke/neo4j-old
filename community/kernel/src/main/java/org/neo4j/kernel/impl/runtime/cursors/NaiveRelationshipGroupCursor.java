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
import static org.neo4j.kernel.impl.runtime.cursors.NaiveBitManipulation.isDirectRelationshipReference;
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
    public static final int RECORD_SIZE = 25;
    /** used for accessing counts */
    private final NaiveRelationshipTraversalCursor innerHelper;
    protected Read read;
    private long originNodeReference;
    protected VirtualRelationshipGroupCursor virtualGroupCursor;
    boolean isPureVirtual;

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
        virtualGroupCursor = null;
        isPureVirtual = false; // TODO: Better name
    }

    /**
     * Use this init when there is no group record (i.e. on a dense node)
     * and the initialAddress is pointing directly to the relationship store
     */
    public void initFromDirectRelationship( long originNodeReference, long initialAddress, Read read )
    {
        // If this cursor was bound to a page, close it first
        if ( isBound() )
        {
            close();
        }

        this.originNodeReference = originNodeReference;
        this.read = read;

        if ( initialAddress != NO_RELATIONSHIP )
        {
            assert isDirectRelationshipReference( initialAddress );


            // Initialize inner cursor to relationship store and use it to build a virtual relationship group
            long reference = NaiveBitManipulation.decodeDirectRelationshipReference( initialAddress );
            read.relationships( originNodeReference, reference, innerHelper );
            virtualGroupCursor = new VirtualRelationshipGroupCursor();
            virtualGroupCursor.init( innerHelper );
        }
        else
        {
            // TODO: It could be prefered if the case that ends us up in here was shortcutted by a call to initVirtual() instead
            virtualGroupCursor = null;
            isPureVirtual = true;
        }
    }

    public void initVirtual( long originNodeReference, Read read )
    {
        // If this cursor was bound to a page, close it first
        if ( isBound() )
        {
            close();
        }
        this.read = read;
        this.originNodeReference = originNodeReference;
        virtualGroupCursor = null;
        isPureVirtual = true;
    }

    protected boolean isVirtualGroup()
    {
        return virtualGroupCursor != null;
    }

    protected boolean isPhysicalGroup()
    {
        return isBound();
    }

    protected boolean isPureVirtualGroup()
    {
        return isVirtualGroup() && isPureVirtual;
    }

    protected VirtualRelationshipGroupCursor getOrCreateVirtualGroupCursor()
    {
        if ( virtualGroupCursor == null )
        {
            virtualGroupCursor = new VirtualRelationshipGroupCursor();
        }
        return virtualGroupCursor;
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
        if ( isPhysicalGroup() )
        {
            if ( isUnbound() )
            {
                return false;
            }
            if ( firstJump() )
            {
                if ( isVirtualGroup() )
                {
                    virtualGroupCursor.shadowPhysicalGroupAt( relationshipLabel() );
                }
                return true;
            }
            boolean hasNext = jumpToAddress( nextReference() );
            if ( hasNext )
            {
                if ( isVirtualGroup() )
                {
                    virtualGroupCursor.shadowPhysicalGroupAt( relationshipLabel() );
                }
                return true;
            }
        }
        if ( isVirtualGroup() )
        {
            assert isVirtualGroup();
            return virtualGroupCursor.next();
        }
        return false;
    }

    @Override
    public int relationshipLabel()
    {
        if ( isPhysicalGroup() )
        {
            return unsignedShort( 2 );
        }
        else // if ( isVirtualGroup() )
        {
            assert isVirtualGroup();
            return virtualGroupCursor.relationshipLabel();
        }
    }

    @Override
    public int outgoingCount()
    {
        int virtualCount = 0;
        int physicalCount = 0;

        if ( isVirtualGroup() )
        {
            virtualCount = virtualGroupCursor.outgoingCount();
        }
        if ( isPhysicalGroup() )
        {
            physicalCount = count( outgoingReference(), true );
        }
        return virtualCount + physicalCount;
    }

    @Override
    public int incomingCount()
    {
        int virtualCount = 0;
        int physicalCount = 0;

        if ( isVirtualGroup() )
        {
            virtualCount = virtualGroupCursor.incomingCount();
        }
        if ( isPhysicalGroup() )
        {
            physicalCount = count( incomingReference(), true );
        }
        return virtualCount + physicalCount;
    }

    @Override
    public int loopCount()
    {
        int virtualCount = 0;
        int physicalCount = 0;

        if ( isVirtualGroup() )
        {
            virtualCount = virtualGroupCursor.loopCount();
        }
        if ( isPhysicalGroup() )
        {
            physicalCount = count( loopsReference(), true );
        }
        return virtualCount + physicalCount;
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
        assert isPhysicalGroup();
        return combineReference( unsignedInt( 4 ), ((long) (unsignedByte( 0 ) & 0x0E)) << 31 );
    }

    @Override
    public long outgoingReference()
    {
        long reference = NO_RELATIONSHIP;

        if ( isVirtualGroup() )
        {
            reference = virtualGroupCursor.outgoingReference();
        }
        else if ( isPhysicalGroup() )
        {
            reference = combineReference( unsignedInt( 8 ), ((long) (unsignedByte( 0 ) & 0x70)) << 28 );
        }
        return reference;
    }

    @Override
    public long incomingReference()
    {
        long reference = NO_RELATIONSHIP;

        if ( isVirtualGroup() )
        {
            reference = virtualGroupCursor.incomingReference();
        }
        else if ( isPhysicalGroup() )
        {
            reference = combineReference( unsignedInt( 12 ), ((long) (unsignedByte( 1 ) & 0x0E)) << 31 );
        }
        return reference;
    }

    @Override
    public long loopsReference()
    {
        long reference = NO_RELATIONSHIP;

        if ( isVirtualGroup() )
        {
            reference = virtualGroupCursor.loopsReference();
        }
        else if ( isPhysicalGroup() )
        {
            reference = combineReference( unsignedInt( 16 ), ((long) (unsignedByte( 1 ) & 0x70)) << 28 );
        }
        return reference;
    }

    private long nodeReference()
    {
        assert isPhysicalGroup();
        return combineReference( unsignedInt( 20 ), ((long) unsignedByte( 24 )) << 32 );
    }

    protected long originNodeReference()
    {
        return originNodeReference;
    }

    @Override
    public void outgoing( RelationshipTraversalCursor cursor )
    {
        // TODO: We need to handle mixed groups here
        assert !(isVirtualGroup() && isPhysicalGroup()) : "Please implement support for mixed group";

        if ( isVirtualGroup() )
        {
            // This will only set the cursor to the virtual group
            // We still rely on the read call below to setup the page cursor
            virtualGroupCursor.outgoing( cursor );
        }
        else
        {
            // We need to make sure the given cursor is set to a reusable state, since it could have been used as a
            // virtual traversal cursor before
            ((VirtualGroupAwareRelationshipTraversalCursor) cursor).setVirtualCursor( null );
        }
        if ( !isPureVirtualGroup() )
        {
            long outgoingReference = outgoingReference();
            if ( outgoingReference != NO_RELATIONSHIP )
            {
                read.relationships( originNodeReference(), outgoingReference, cursor );
            }
        }
    }

    @Override
    public void incoming( RelationshipTraversalCursor cursor )
    {
        // TODO: We need to handle mixed groups here
        assert !(isVirtualGroup() && isPhysicalGroup()) : "Please implement support for mixed group";

        if ( isVirtualGroup() )
        {
            virtualGroupCursor.incoming( cursor );
        }
        else
        {
            // We need to make sure the given cursor is set to a reusable state, since it could have been used as a
            // virtual traversal cursor before
            ((VirtualGroupAwareRelationshipTraversalCursor) cursor).setVirtualCursor( null );
        }
        if ( !isPureVirtualGroup() )
        {
            long incomingReference = incomingReference();
            if ( incomingReference != NO_RELATIONSHIP )
            {
                read.relationships( originNodeReference(), incomingReference, cursor );
            }
        }
    }

    @Override
    public void loops( RelationshipTraversalCursor cursor )
    {
        // TODO: We need to handle mixed groups here
        assert !(isVirtualGroup() && isPhysicalGroup()) : "Please implement support for mixed group";

        if ( isVirtualGroup() )
        {
            virtualGroupCursor.loops( cursor );
        }
        else
        {
            // We need to make sure the given cursor is set to a reusable state, since it could have been used as a
            // virtual traversal cursor before
            ((VirtualGroupAwareRelationshipTraversalCursor) cursor).setVirtualCursor( null );
        }
        if ( !isPureVirtualGroup() )
        {
            long loopsReference = loopsReference();
            if ( loopsReference != NO_RELATIONSHIP )
            {
                read.relationships( originNodeReference(), loopsReference, cursor );
            }
        }
    }
}
