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

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.runtime.cursors.NaiveConstants.NO_NODE;

public class NaiveRelationshipTraversalCursor extends NaiveRelationshipCursor implements RelationshipTraversalCursor
{
    private long originNodeReference;

    public void init( PageCursor pageCursor, long startAddress, long originNodeReference, Read read )
    {
        this.originNodeReference = originNodeReference;
        initJumpingCursor( pageCursor, startAddress, read );
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
        return jumpToAddress( nextRelationshipReference() );
    }

    private long nextRelationshipReference()
    {
        final long source = sourceNodeReference(), target = targetNodeReference();
        if ( source == originNodeReference )
        {
            return sourceNextRelationshipReference();
        }
        if ( target == originNodeReference )
        {
            return targetNextRelationshipReference();
        }
        throw new IllegalStateException( format(
                "Relationship %d is not part of this chain! source=0x%x, target=0x%x, origin=0x%x",
                relationshipReference(), source, target, originNodeReference ) );
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    protected void onClose()
    {
        originNodeReference = NO_NODE;
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
    public void neighbour( NodeCursor cursor )
    {
        read.singleNode( neighbourNodeReference(), cursor );
    }

    @Override
    public long neighbourNodeReference()
    {
        final long source = sourceNodeReference();
        final long target = targetNodeReference();

        if ( source == originNodeReference )
        {
            return target;
        }
        if ( target == originNodeReference )
        {
            return source;
        }
        throw new IllegalStateException( format(
                "not part of this chain! source=0x%x, target=0x%x, origin=0x%x",
                source, target, originNodeReference ) );
    }

    @Override
    public long originNodeReference()
    {
        return originNodeReference;
    }
}
