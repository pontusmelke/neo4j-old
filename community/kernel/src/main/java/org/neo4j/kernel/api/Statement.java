/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;

/**
 * A statement which is a smaller coherent unit of work inside a {@link KernelTransaction}.
 * There are accessors for different types of operations. The operations are divided into
 * read and write operations.
 */
public interface Statement extends Resource, ResourceManager
{
    /**
     * @return interface exposing all read operations.
     */
    ReadOperations readOperations();

    /**
     * @return interface exposing all write operations about data such as nodes, relationships and properties.
     * @throws InvalidTransactionTypeKernelException if type of this transaction have already been decided
     * and it's of a different type..
     */
    DataWriteOperations dataWriteOperations() throws InvalidTransactionTypeKernelException;

    /**
     * @return interface exposing operations for associating metadata with this statement
     */
    QueryRegistryOperations queryRegistration();
}
