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

import java.util.Arrays;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Iterables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;

public abstract class TransactionStateTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    public void shouldSeeNodeInTransaction() throws Exception
    {
        long nodeId;
        try ( Transaction tx = kernel.beginTransaction() )
        {
            nodeId = tx.nodeCreate();
            try ( NodeCursor node = kernel.cursors().allocateNodeCursor() )
            {
                tx.singleNode( nodeId, node );
                assertTrue( "should access node", node.next() );
                assertEquals( nodeId, node.nodeReference() );
                assertFalse( "should only find one node", node.next() );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertEquals( nodeId, graphDb.getNodeById( nodeId ).getId() );
        }
    }

    @Test
    public void shouldSeeNewLabelledNodeInTransaction() throws Exception
    {
        long nodeId;
        int labelId;
        final String labelName = "Town";

        try ( Transaction tx = kernel.beginTransaction() )
        {
            nodeId = tx.nodeCreate();
            labelId = kernel.token().labelGetOrCreateForName( labelName );
            tx.nodeAddLabel( nodeId, labelId );

            try ( NodeCursor node = kernel.cursors().allocateNodeCursor() )
            {
                tx.singleNode( nodeId, node );
                assertTrue( "should access node", node.next() );

                LabelSet labels = node.labels();
                assertEquals( 1, labels.numberOfLabels() );
                assertEquals( labelId, labels.label( 0 ) );
                assertFalse( "should only find one node", node.next() );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getLabels(),
                    equalTo( Iterables.iterable( label( labelName ) ) ) );
        }
    }

    @Test
    public void shouldSeeLabelChangesInTransaction() throws Exception
    {
        long nodeId;
        int toRetain, toDelete, toAdd;
        final String toRetainName = "ToRetain";
        final String toDeleteName = "ToDelete";
        final String toAddName = "ToAdd";

        try ( Transaction tx = kernel.beginTransaction() )
        {
            nodeId = tx.nodeCreate();
            toRetain = kernel.token().labelGetOrCreateForName( toRetainName );
            toDelete = kernel.token().labelGetOrCreateForName( toDeleteName );
            tx.nodeAddLabel( nodeId, toRetain );
            tx.nodeAddLabel( nodeId, toDelete );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getLabels(),
                    equalTo( Iterables.iterable( label( toRetainName ), label( toDeleteName ) ) ) );
        }

        try ( Transaction tx = kernel.beginTransaction() )
        {
            toAdd = kernel.token().labelGetOrCreateForName( toAddName );
            tx.nodeAddLabel( nodeId, toAdd );
            tx.nodeRemoveLabel( nodeId, toDelete );

            try ( NodeCursor node = kernel.cursors().allocateNodeCursor() )
            {
                tx.singleNode( nodeId, node );
                assertTrue( "should access node", node.next() );

                assertLabels( node.labels(), toRetain, toAdd );
                assertFalse( "should only find one node", node.next() );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getLabels(),
                    equalTo( Iterables.iterable( label( toRetainName ), label( toAddName ) ) ) );
        }
    }

    @Test
    public void shouldSeeNewRelationshipInTransaction() throws Exception
    {
        long startNodeId;
        long newNodeId;
        long newRelationshipId;
        int newRelTypeId;
        String newRelTypeName = "REL";

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            startNodeId = graphDb.createNodeId();
            tx.success();
        }

        try ( Transaction tx = kernel.beginTransaction() )
        {
            newNodeId = tx.nodeCreate();
            newRelTypeId = kernel.token().relationshipTypeGetOrCreateForName( newRelTypeName );
            newRelationshipId = tx.relationshipCreate( startNodeId, newRelTypeId, newNodeId );

            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
                  RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                tx.singleNode( startNodeId, node );
                assertTrue( "should access node", node.next() );
                assertEquals( startNodeId, node.nodeReference() );

                node.relationships( group );
                assertFalse( "should only find one node", node.next() );

                assertTrue( group.next() );
                assertEquals( group.relationshipLabel(), newRelTypeId );
                group.outgoing( relationship );
                assertFalse( group.next() );

                assertTrue( relationship.next() );
                assertEquals( relationship.relationshipReference(), newRelationshipId );
                assertEquals( relationship.originNodeReference(), startNodeId );
                assertEquals( relationship.neighbourNodeReference(), newNodeId );

                // Move node cursor to new node
                relationship.neighbour( node );
                assertTrue( node.next() );
                assertEquals( node.nodeReference(), newNodeId );

                assertFalse( relationship.next() );

                // Traverse the incoming relationship back to start node
                node.relationships( group );
                assertTrue( group.next() );
                assertEquals( group.relationshipLabel(), newRelTypeId );
                group.incoming( relationship );
                assertFalse( group.next() );

                assertFalse( node.next() );

                assertTrue( relationship.next() );
                assertEquals( relationship.relationshipReference(), newRelationshipId );
                assertEquals( relationship.originNodeReference(), newNodeId );
                assertEquals( relationship.neighbourNodeReference(), startNodeId );

                // Move node cursor to start node
                relationship.neighbour( node );
                assertTrue( node.next() );
                assertEquals( node.nodeReference(), startNodeId );
                assertFalse( node.next() );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertEquals( startNodeId, graphDb.getNodeById( startNodeId ).getId() );
        }
    }

    @Test
    public void shouldSeeNewAndExistingRelationshipInTransaction() throws Exception
    {
        long startNodeId, endNodeId;
        long existingRelationshipId;
        long newNodeId;
        long newRelationshipId;
        int newRelTypeId;
        String newRelTypeName = "REL";

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node startNode = graphDb.createNode();
            startNodeId = startNode.getId();
            Node endNode = graphDb.createNode();
            endNodeId = endNode.getId();

            Relationship existingRelationship =
                    startNode.createRelationshipTo( endNode, RelationshipType.withName( newRelTypeName ) );
            existingRelationshipId = existingRelationship.getId();

            tx.success();
        }

        try ( Transaction tx = kernel.beginTransaction() )
        {
            newNodeId = tx.nodeCreate();
            newRelTypeId = kernel.token().relationshipTypeGetOrCreateForName( newRelTypeName );
            newRelationshipId = tx.relationshipCreate( startNodeId, newRelTypeId, newNodeId );

            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
                  RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                tx.singleNode( startNodeId, node );
                assertTrue( "should access node", node.next() );
                assertEquals( startNodeId, node.nodeReference() );

                node.relationships( group );
                assertFalse( "should only find one node", node.next() );

                assertTrue( group.next() );
                assertEquals( group.relationshipLabel(), newRelTypeId );
                group.outgoing( relationship );
                assertFalse( group.next() );

                assertTrue( relationship.next() );
                assertEquals( relationship.relationshipReference(), existingRelationshipId );
                assertEquals( relationship.originNodeReference(), startNodeId );
                assertEquals( relationship.neighbourNodeReference(), endNodeId );

                assertTrue( relationship.next() );
                assertEquals( relationship.relationshipReference(), newRelationshipId );
                assertEquals( relationship.originNodeReference(), startNodeId );
                assertEquals( relationship.neighbourNodeReference(), newNodeId );

                // Move node cursor to new node
                relationship.neighbour( node );
                assertTrue( node.next() );
                assertEquals( node.nodeReference(), newNodeId );

                assertFalse( relationship.next() );

                // Traverse the incoming relationship back to start node
                node.relationships( group );
                assertTrue( group.next() );
                assertEquals( group.relationshipLabel(), newRelTypeId );
                group.incoming( relationship );
                assertFalse( group.next() );

                assertFalse( node.next() );

                assertTrue( relationship.next() );
                assertEquals( relationship.relationshipReference(), newRelationshipId );
                assertEquals( relationship.originNodeReference(), newNodeId );
                assertEquals( relationship.neighbourNodeReference(), startNodeId );

                // Move node cursor to start node
                relationship.neighbour( node );
                assertTrue( node.next() );
                assertEquals( node.nodeReference(), startNodeId );
                assertFalse( node.next() );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertEquals( startNodeId, graphDb.getNodeById( startNodeId ).getId() );
        }
    }

    @Test
    public void shouldSeeNewAndDeletedRelationshipInTransaction() throws Exception
    {
        long startNodeId, endNodeId;
        long existingRelationshipId;
        long newNodeId;
        long newRelationshipId;
        int newRelTypeId;
        String newRelTypeName = "REL";

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node startNode = graphDb.createNode();
            startNodeId = startNode.getId();
            Node endNode = graphDb.createNode();
            endNodeId = endNode.getId();

            Relationship existingRelationship =
                    startNode.createRelationshipTo( endNode, RelationshipType.withName( newRelTypeName ) );
            existingRelationshipId = existingRelationship.getId();

            tx.success();
        }

        try ( Transaction tx = kernel.beginTransaction() )
        {
            newNodeId = tx.nodeCreate();
            newRelTypeId = kernel.token().relationshipTypeGetOrCreateForName( newRelTypeName );
            newRelationshipId = tx.relationshipCreate( startNodeId, newRelTypeId, newNodeId );
            tx.relationshipDelete( existingRelationshipId, newRelTypeId, startNodeId, endNodeId );

            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
                  RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                tx.singleNode( startNodeId, node );
                assertTrue( "should access node", node.next() );
                assertEquals( startNodeId, node.nodeReference() );

                node.relationships( group );
                assertFalse( "should only find one node", node.next() );

                assertTrue( group.next() );
                assertEquals( group.relationshipLabel(), newRelTypeId );
                group.outgoing( relationship );
                assertFalse( group.next() );

                assertTrue( relationship.next() );
                assertEquals( relationship.relationshipReference(), newRelationshipId );
                assertEquals( relationship.originNodeReference(), startNodeId );
                assertEquals( relationship.neighbourNodeReference(), newNodeId );

                // Move node cursor to new node
                relationship.neighbour( node );
                assertTrue( node.next() );
                assertEquals( node.nodeReference(), newNodeId );

                assertFalse( relationship.next() );

                // Traverse the incoming relationship back to start node
                node.relationships( group );
                assertTrue( group.next() );
                assertEquals( group.relationshipLabel(), newRelTypeId );
                group.incoming( relationship );
                assertFalse( group.next() );

                assertFalse( node.next() );

                assertTrue( relationship.next() );
                assertEquals( relationship.relationshipReference(), newRelationshipId );
                assertEquals( relationship.originNodeReference(), newNodeId );
                assertEquals( relationship.neighbourNodeReference(), startNodeId );

                // Move node cursor to start node
                relationship.neighbour( node );
                assertTrue( node.next() );
                assertEquals( node.nodeReference(), startNodeId );
                assertFalse( node.next() );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertEquals( startNodeId, graphDb.getNodeById( startNodeId ).getId() );
        }
    }

    private void assertLabels( LabelSet labels, int... expected )
    {
        assertEquals( expected.length, labels.numberOfLabels() );
        Arrays.sort(expected);
        int[] labelArray = new int[labels.numberOfLabels()];
        for ( int i = 0; i < labels.numberOfLabels(); i++ )
        {
            labelArray[i] = labels.label( i );
        }
        Arrays.sort( labelArray );
        assertTrue( "labels match expected", Arrays.equals( expected, labelArray ) );
    }
}
