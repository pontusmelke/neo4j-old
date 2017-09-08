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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import org.neo4j.graphdb.NotFoundException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class NodeWriteTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    public void shouldCreateNode() throws Exception
    {
        long node;
        try ( Transaction tx = kernel.beginTransaction() )
        {
            node = tx.nodeCreate();
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertEquals( node, graphDb.getNodeById( node ).getId() );
        }
    }

    @Test
    public void shouldRollbackOnFailure() throws Exception
    {
        long node;
        try ( Transaction tx = kernel.beginTransaction() )
        {
            node = tx.nodeCreate();
            tx.failure();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            graphDb.getNodeById( node );
            fail( "There should be no node" );
        }
        catch ( NotFoundException e )
        {
            // expected
        }
    }
}
