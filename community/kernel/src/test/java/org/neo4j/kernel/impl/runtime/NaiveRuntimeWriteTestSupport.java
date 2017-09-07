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
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.internal.kernel.api.KernelAPI;
import org.neo4j.internal.kernel.api.KernelAPIWriteTestSupport;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class NaiveRuntimeWriteTestSupport implements KernelAPIWriteTestSupport
{
    private GraphDatabaseAPI graphDb;
    private NaiveKernel kernel;

    @Override
    public void setup( File storeDir ) throws IOException
    {
        graphDb = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir )
                .newGraphDatabase();

        kernel = graphDb.getDependencyResolver().resolveDependency( NaiveKernel.class );
    }

    @Override
    public void beforeEachTest()
    {
        // nothing special to do
    }

    @Override
    public KernelAPI kernelToTest()
    {
        return kernel;
    }

    @Override
    public GraphDatabaseService graphBackdoor()
    {
        return graphDb;
    }

    @Override
    public void tearDown()
    {
        graphDb.shutdown();
    }
}
