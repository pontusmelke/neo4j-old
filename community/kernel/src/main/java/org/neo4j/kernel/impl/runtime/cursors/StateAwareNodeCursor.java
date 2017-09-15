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

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.storageengine.api.txstate.ReadableRelationshipDiffSets;

public class StateAwareNodeCursor extends NaiveNodeCursor
{
    private TxStateHolder stateHolder;

    public void init(
            PageCursor pageCursor,
            long startAddress,
            long maxAddress,
            Read read,
            TxStateHolder stateHolder )
    {
        super.init( pageCursor, startAddress, maxAddress, read );
        this.stateHolder = stateHolder;
    }

    @Override
    public long nodeReference()
    {
        return address();
    }

    @Override
    public boolean next()
    {
        // TODO: Needs to handle removed nodes. Write a test for this
        boolean hasNext = super.next();
        while ( hasNext && stateHolder.hasTxStateWithChanges() &&
                stateHolder.txState().nodeIsDeletedInThisTx( nodeReference() ) )
        {
            hasNext = super.next();
        }
        if ( hasNext )
        {
            return true;
        }
        long address = address();
        if ( (address < maxAddress() || isJumpingCursor() ) &&
             stateHolder.hasTxStateWithChanges() && stateHolder.txState().addedAndRemovedNodes().isAdded( address ) )
        {
            return true;
        }
        return false;
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    // DATA ACCESSOR METHODS

    @Override
    protected boolean inUse()
    {
        if ( stateHolder.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> nodes = stateHolder.txState().addedAndRemovedNodes();
            if ( nodes.isAdded( address() ) )
            {
                return true;
            }
            if ( nodes.isRemoved( address() ) )
            {
                return false;
            }
        }
        return super.inUse();
    }

    @Override
    public LabelSet labels()
    {
        if ( stateHolder.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> nodes = stateHolder.txState().addedAndRemovedNodes();
            ReadableDiffSets<Integer> labelDiff = stateHolder.txState().nodeStateLabelDiffSets( address() );

            if ( nodes.isAdded( address() ) )
            {
                return NaiveLabels.of( labelDiff.getAdded() );
            }
            if ( nodes.isRemoved( address() ) )
            {
                return LabelSet.NONE;
            }
            return NaiveLabels.augment( super.labels(), labelDiff );
        }
        return super.labels();
    }

    @Override
    public boolean hasProperties()
    {
        return super.hasProperties();
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        super.properties( cursor );
    }

    @Override
    public long propertiesReference()
    {
        return super.propertiesReference();
    }
}
