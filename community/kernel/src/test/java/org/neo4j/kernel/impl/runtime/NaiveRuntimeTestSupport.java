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

import java.io.File;
import java.util.function.Consumer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.internal.kernel.api.KernelAPITestSupport;
import org.neo4j.internal.kernel.api.Runtime;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class NaiveRuntimeTestSupport implements KernelAPITestSupport
{
    private GraphDatabaseAPI graphDb;
    private NaiveRuntime runtime;

    @Override
    public void setup( File storeDir, Consumer<GraphDatabaseService> create )
    {
        graphDb = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir )
                .newGraphDatabase();

        create.accept( graphDb );
        runtime = graphDb.getDependencyResolver().resolveDependency( NaiveRuntime.class );
    }

    @Override
    public Runtime runtimeToTest()
    {
        return runtime;
    }

    @Override
    public void tearDown()
    {
        graphDb.shutdown();
    }
}
