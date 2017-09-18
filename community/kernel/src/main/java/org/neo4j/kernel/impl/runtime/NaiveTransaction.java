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
package org.neo4j.kernel.impl.runtime;

import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.values.storable.Value;

public class NaiveTransaction extends NaiveRead implements Transaction
{
    private final StoreReadLayer storeReadLayer;
    private final KernelTransaction kernelTransaction;

    NaiveTransaction( PagedFile nodeStore, PagedFile relationshipStore, PagedFile relationshipGroupStore,
            PagedFile propertyStore, StoreReadLayer storeReadLayer, KernelTransaction kernelTransaction )
    {
        super( nodeStore, relationshipStore, relationshipGroupStore, propertyStore, kernelTransaction );
        this.storeReadLayer = storeReadLayer;
        this.kernelTransaction = kernelTransaction;
    }

    // TRANSACTION

    @Override
    public void success()
    {
        kernelTransaction.success();
    }

    @Override
    public void failure()
    {
        kernelTransaction.failure();
    }

    @Override
    public void close() throws Exception
    {
        kernelTransaction.close();
    }

    // WRITE

    @Override
    public long nodeCreate()
    {
        long nodeId = storeReadLayer.reserveNode();
        kernelTransaction.txState().nodeDoCreate( nodeId );
        return nodeId;
    }

    @Override
    public void nodeDelete( long node )
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    @Override
    public long relationshipCreate( long sourceNode, int relationshipLabel, long targetNode )
    {
        long relId = storeReadLayer.reserveRelationship();
        kernelTransaction.txState().relationshipDoCreate( relId, relationshipLabel, sourceNode, targetNode );
        return relId;
    }

    @Override
    public void relationshipDelete( long relationship )
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    @Override
    public void relationshipDelete( long relationship, int relationshipLabel, long sourceNode, long targetNode )
    {
        kernelTransaction.txState().relationshipDoDelete( relationship, relationshipLabel, sourceNode, targetNode );
    }

    @Override
    public void nodeAddLabel( long node, int nodeLabel )
    {
        kernelTransaction.txState().nodeDoAddLabel( nodeLabel, node );
    }

    @Override
    public void nodeRemoveLabel( long node, int nodeLabel )
    {
        kernelTransaction.txState().nodeDoRemoveLabel( nodeLabel, node );
    }

    @Override
    public void nodeSetProperty( long node, int propertyKey, Object value )
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    @Override
    public void nodeRemoveProperty( long node, int propertyKey )
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    @Override
    public void relationshipSetProperty( long relationship, int propertyKey, Value value )
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    @Override
    public void relationshipRemoveProperty( long node, int propertyKey )
    {
        throw new UnsupportedOperationException( "Please implement" );
    }
}
