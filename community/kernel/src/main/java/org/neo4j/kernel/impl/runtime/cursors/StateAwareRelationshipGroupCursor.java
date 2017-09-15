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
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.txstate.TxStateHolder;

public class StateAwareRelationshipGroupCursor extends NaiveRelationshipGroupCursor
{
    private TxStateHolder stateHolder;

    StateAwareRelationshipGroupCursor( NaiveRelationshipTraversalCursor innerHelper )
    {
        super( innerHelper );
    }

    public void init( PageCursor pageCursor, long originNodeReference, long initialAddress, Read read,
            TxStateHolder stateHolder )
    {
        super.init( pageCursor, originNodeReference, initialAddress, read );
        this.stateHolder = stateHolder;
        // TODO: In this case we need a way to
        augmentWithTransactionState();
    }

    public void initVirtual( long originNodeReference, long initialAddress, Read read,
            TxStateHolder stateHolder )
    {
        super.initVirtual( originNodeReference, initialAddress, read );
        this.stateHolder = stateHolder;
        augmentWithTransactionState();
    }

    private void augmentWithTransactionState()
    {
        if ( stateHolder.hasTxStateWithChanges() )
        {
            // When we access the groups from the page cache, we need to augment with the
            // virtual data from the transaction state.
            // We need to support both the case where we have a mixed cursor with both
            // physical (page cache backed) data and virtual data,
            // as well as the case where we have a pure virtual cursor.
            VirtualRelationshipGroupCursor virtualGroupCursor = getOrCreateVirtualGroupCursor();
            virtualGroupCursor.augmentWithTransactionState( stateHolder.txState(), originNodeReference() );
        }
    }

    @Override
    public boolean next()
    {
        // TODO: Check deleted relationships in transaction state.
        return super.next();
    }
}