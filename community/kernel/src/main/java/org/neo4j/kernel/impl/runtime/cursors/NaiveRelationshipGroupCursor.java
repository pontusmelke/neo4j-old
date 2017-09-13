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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongStack;
import org.neo4j.graphdb.Relationship;
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
    private Read read;
    private long originNodeReference;
    private VirtualRelationshipGroupCursor virtualGroupCursor;

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

    /**
     * Use this init when there is no group record
     */
    public void initDirect( long originNodeReference, long initialAddress, Read read )
    {
        this.read = read;
        this.originNodeReference = originNodeReference;
        assert isDirectRelationshipReference( initialAddress );

        // Initialize inner cursor to relationship store and use it to build a virtual relationship group
        read.relationships( originNodeReference, initialAddress, innerHelper );
        virtualGroupCursor = new VirtualRelationshipGroupCursor();
        virtualGroupCursor.init( innerHelper );
    }

    private boolean isVirtualGroup()
    {
        return virtualGroupCursor != null;
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
        if ( isVirtualGroup() )
        {
            return virtualGroupCursor.next();
        }

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

    private boolean nextDirect()
    {
        return innerHelper.next();
    }

    @Override
    public int relationshipLabel()
    {
        if ( isVirtualGroup() )
        {
            return virtualGroupCursor.relationshipLabel();
        }
        return unsignedShort( 2 );
    }

    @Override
    public int outgoingCount()
    {
        if ( isVirtualGroup() )
        {
            return virtualGroupCursor.outgoingCount();
        }
        return count( outgoingReference(), true );
    }

    @Override
    public int incomingCount()
    {
        if ( isVirtualGroup() )
        {
            return virtualGroupCursor.incomingCount();
        }
        return count( incomingReference(), false );
    }

    @Override
    public int loopCount()
    {
        if ( isVirtualGroup() )
        {
            return virtualGroupCursor.loopCount();
        }
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
        if ( isVirtualGroup() )
        {
            return NaiveBitManipulation.encodeDirectRelationshipReference( virtualGroupCursor.outgoingReference() );
        }
        return combineReference( unsignedInt( 8 ), ((long) (unsignedByte( 0 ) & 0x70)) << 28 );
    }

    @Override
    public long incomingReference()
    {
        if ( isVirtualGroup() )
        {
            return NaiveBitManipulation.encodeDirectRelationshipReference( virtualGroupCursor.incomingReference() );
        }
        return combineReference( unsignedInt( 12 ), ((long) (unsignedByte( 1 ) & 0x0E)) << 31 );
    }

    @Override
    public long loopsReference()
    {
        if ( isVirtualGroup() )
        {
            return NaiveBitManipulation.encodeDirectRelationshipReference( virtualGroupCursor.loopsReference() );
        }
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
        if ( isVirtualGroup() )
        {
            // This will only set the cursor to the virtual group
            // We still rely on the read call below to setup the page cursor
            virtualGroupCursor.outgoing( cursor );
        }
        long outgoingReference = outgoingReference();
        if ( outgoingReference != NO_RELATIONSHIP )
        {
            read.relationships( originNodeReference(), outgoingReference, cursor );
        }
    }

    @Override
    public void incoming( RelationshipTraversalCursor cursor )
    {
        if ( isVirtualGroup() )
        {
            virtualGroupCursor.incoming( cursor );
        }
        long incomingReference = incomingReference();
        if ( incomingReference != NO_RELATIONSHIP )
        {
            read.relationships( originNodeReference(), incomingReference, cursor );
        }
    }

    @Override
    public void loops( RelationshipTraversalCursor cursor )
    {
        if ( isVirtualGroup() )
        {
            virtualGroupCursor.loops( cursor );
        }
        long loopsReference = loopsReference();
        if ( loopsReference != NO_RELATIONSHIP )
        {
            read.relationships( originNodeReference(), loopsReference, cursor );
        }
    }

    private static class VirtualRelationshipGroup
    {
        PrimitiveLongStack outgoing;
        PrimitiveLongStack incoming;
        PrimitiveLongStack loops;

        public VirtualRelationshipGroup()
        {
            // TODO: Calculate reasonable initial sizes based on dense node threshold setting
            outgoing = new PrimitiveLongStack( 8 );
            incoming = new PrimitiveLongStack( 8 );
            loops = new PrimitiveLongStack( 4 );
        }
    }

    private static int NOT_INITIALIZED = -1;

    private class VirtualRelationshipGroupCursor implements RelationshipGroupCursor
    {
        private PrimitiveIntObjectMap<VirtualRelationshipGroup> virtualGroups;
        private PrimitiveIntIterator virtualGroupIterator;
        private int currentVirtualGroupLabel;
        private VirtualRelationshipGroup currentVirtualGroup;


        public VirtualRelationshipGroupCursor()
        {
            virtualGroups = Primitive.intObjectMap();
            currentVirtualGroupLabel = NOT_INITIALIZED;
        }

        public void init( NaiveRelationshipTraversalCursor cursor )
        {
            readDirectRelationshipsIntoVirtualGroups( cursor );
        }

        private void readDirectRelationshipsIntoVirtualGroups( NaiveRelationshipTraversalCursor cursor )
        {
            VirtualRelationshipGroup group;

            while ( cursor.next() )
            {
                int label = cursor.label();
                group = virtualGroups.get( label );
                if ( group == null )
                {
                    // Create a new virtual group if it does not exist
                    group = new VirtualRelationshipGroup();
                    virtualGroups.put( label, group );
                }
                long reference = cursor.relationshipReference();

                // Categorize relationships in mutually exclusive buckets
                // NOTE: We have to check isLoop() first since isOutgoing() and isIncoming() is also true for loops
                if ( cursor.isLoop() )
                {
                    assert cursor.sourceNodeReference() == cursor.originNodeReference();
                    group.loops.push( reference );
                }
                else if ( cursor.isOutgoing() )
                {
                    group.outgoing.push( reference );
                }
                else // if ( cursor.isIncoming() )
                {
                    assert cursor.isIncoming();
                    group.incoming.push( reference );
                }
            }
        }

        @Override
        public int relationshipLabel()
        {
            assert currentVirtualGroupLabel != NOT_INITIALIZED;
            return currentVirtualGroupLabel;
        }

        @Override
        public int outgoingCount()
        {
            return currentVirtualGroup.outgoing.size();
        }

        @Override
        public int incomingCount()
        {
            return currentVirtualGroup.incoming.size();
        }

        @Override
        public int loopCount()
        {
            return currentVirtualGroup.loops.size();
        }

        @Override
        public void outgoing( RelationshipTraversalCursor cursor )
        {
            VirtualGroupAwareRelationshipTraversalCursor.VirtualRelationshipTraversalCursor virtualCursor =
                    new VirtualGroupAwareRelationshipTraversalCursor.VirtualRelationshipTraversalCursor();
            virtualCursor.setOutgoing();
            virtualCursor.setRelationships( currentVirtualGroup.outgoing );
            ((VirtualGroupAwareRelationshipTraversalCursor) cursor).initVirtual( virtualCursor );
        }

        @Override
        public void incoming( RelationshipTraversalCursor cursor )
        {
            VirtualGroupAwareRelationshipTraversalCursor.VirtualRelationshipTraversalCursor virtualCursor =
                    new VirtualGroupAwareRelationshipTraversalCursor.VirtualRelationshipTraversalCursor();
            virtualCursor.setIncoming();
            virtualCursor.setRelationships( currentVirtualGroup.incoming );
            ((VirtualGroupAwareRelationshipTraversalCursor) cursor).initVirtual( virtualCursor );
        }

        @Override
        public void loops( RelationshipTraversalCursor cursor )
        {
            VirtualGroupAwareRelationshipTraversalCursor.VirtualRelationshipTraversalCursor virtualCursor =
                    new VirtualGroupAwareRelationshipTraversalCursor.VirtualRelationshipTraversalCursor();
            virtualCursor.setLoops();
            virtualCursor.setRelationships( currentVirtualGroup.loops );
            ((VirtualGroupAwareRelationshipTraversalCursor) cursor).initVirtual( virtualCursor );
        }

        @Override
        public long outgoingReference()
        {
            if ( currentVirtualGroup.outgoing.size() > 0 )
            {
                return currentVirtualGroup.outgoing.peekAt( 0 );
            }
            return NO_RELATIONSHIP;
        }

        @Override
        public long incomingReference()
        {
            if ( currentVirtualGroup.incoming.size() > 0 )
            {
                return currentVirtualGroup.incoming.peekAt( 0 );
            }
            return NO_RELATIONSHIP;
        }

        @Override
        public long loopsReference()
        {
            if ( currentVirtualGroup.loops.size() > 0 )
            {
                return currentVirtualGroup.loops.peekAt( 0 );
            }
            return NO_RELATIONSHIP;
        }

        @Override
        public Position suspend()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resume( Position position )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean next()
        {
            if ( virtualGroupIterator == null )
            {
                virtualGroupIterator = virtualGroups.iterator();
            }
            if ( virtualGroupIterator.hasNext() )
            {
                // TODO: We would like to iterate over values directly, but the collection does not allow us to do that
                currentVirtualGroupLabel = virtualGroupIterator.next();
                currentVirtualGroup = virtualGroups.get( currentVirtualGroupLabel );
                return true;
            }
            return false;
        }

        @Override
        public boolean shouldRetry()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
        }
    }
}
