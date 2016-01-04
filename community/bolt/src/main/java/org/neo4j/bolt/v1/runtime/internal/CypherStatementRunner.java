/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.internal;

import java.util.Map;

import org.neo4j.bolt.v1.runtime.spi.RecordStream;
import org.neo4j.bolt.v1.runtime.spi.StatementRunner;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;

public class CypherStatementRunner implements StatementRunner
{
    private final GraphDatabaseService db;
    private final QueryExecutionEngine queryExecutionEngine;

    public CypherStatementRunner( GraphDatabaseService db, QueryExecutionEngine queryExecutionEngine )
    {
        this.db = db;
        this.queryExecutionEngine = queryExecutionEngine;
    }

    @Override
    public RecordStream run( final SessionState ctx, final String statement,
            final Map<String,Object> params ) throws KernelException
    {
        assertParameters( params );
        // Temporary until we move parsing to cypher, or run a parser up here
        if ( statement.equalsIgnoreCase( "begin" ) )
        {
            ctx.beginTransaction();
            return RecordStream.EMPTY;
        }
        else if ( statement.equalsIgnoreCase( "commit" ) )
        {
            ctx.commitTransaction();
            return RecordStream.EMPTY;
        }
        else if ( statement.equalsIgnoreCase( "rollback" ) )
        {
            ctx.rollbackTransaction();
            return RecordStream.EMPTY;
        }
        else if ( statement.equalsIgnoreCase( "foobar" ) )
        {
            throw new RuntimeException("Foobar occurred");
        }
        else
        {
            if ( ctx.hasTransaction() || queryExecutionEngine.isPeriodicCommit( statement ) )
            {
                return new CypherAdapterStream( db.execute( statement, params ) );
            }
            else
            {
                ctx.beginImplicitTransaction();
                return new CypherAdapterStream( db.execute( statement, params ) );
            }
        }
    }

    private void assertParameters( Map<String,Object> params ) throws KernelException
    {
        for ( Object value : params.values() )
        {
            if (value instanceof Node )
            {
                throw new QueryExecutionKernelException( Status.Request.Invalid, "Nodes can't be used as parameters");
            }
            if (value instanceof Relationship )
            {
                throw new QueryExecutionKernelException( Status.Request.Invalid, "Relationships can't be used as parameters");
            }
            if (value instanceof Path )
            {
                throw new QueryExecutionKernelException( Status.Request.Invalid, "Paths can't be used as parameters");
            }
        }

    }
}
