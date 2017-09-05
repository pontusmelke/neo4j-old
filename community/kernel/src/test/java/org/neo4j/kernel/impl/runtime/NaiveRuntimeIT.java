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

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Runtime;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;

@SuppressWarnings( "WeakerAccess" )
public class NaiveRuntimeIT
{
    static TemporaryFolder folder;
    static GraphDatabaseAPI graphDb;

    @BeforeClass
    public static void setup() throws IOException
    {
        folder = new TemporaryFolder();
        folder.create();
        graphDb = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( folder.getRoot() )
                .newGraphDatabase();
    }

    @Test
    public void shouldDoNodeAllScan()
    {
        // given
        long n1, n2, n3;
        try ( Transaction tx = graphDb.beginTx() )
        {
            n1 = graphDb.createNode().getId();
            n2 = graphDb.createNode().getId();
            n3 = graphDb.createNode().getId();
            tx.success();
        }

        // when
        Runtime r = graphDb.getDependencyResolver().resolveDependency( Runtime.class );
        NodeCursor cursor = r.cursorFactory().allocateNodeCursor();
        r.read().allNodesScan( cursor );

        List<Long> scannedSet = new ArrayList<>();
        while ( cursor.next() )
        {
            scannedSet.add( cursor.nodeReference() );
            assertFalse( cursor.hasProperties() );
            assertThat( cursor.labels().numberOfLabels(), equalTo( 0 ) );
        }

        // then
        assertThat( scannedSet, containsInAnyOrder( n1, n2, n3 ) );
    }
}

