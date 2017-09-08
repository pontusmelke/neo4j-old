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

import org.neo4j.internal.kernel.api.Token;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.storageengine.api.StoreReadLayer;

public class NaiveToken implements Token
{
    final StoreReadLayer store;

    public NaiveToken( StoreReadLayer store )
    {
        this.store = store;
    }

    @Override
    public int labelGetOrCreateForName( String labelName ) throws SomeException
    {
        try
        {
            return store.labelGetOrCreateForName( labelName );
        }
        catch ( TooManyLabelsException e )
        {
            throw new SomeException();
        }
    }

    @Override
    public int propertyKeyGetOrCreateForName( String propertyKeyName )
    {
        return store.propertyKeyGetOrCreateForName( propertyKeyName );
    }

    @Override
    public int relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws SomeException
    {
        return store.relationshipTypeGetOrCreateForName( relationshipTypeName );
    }

    @Override
    public void labelCreateForName( String labelName, int id ) throws SomeException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void propertyKeyCreateForName( String propertyKeyName, int id ) throws SomeException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void relationshipTypeCreateForName( String relationshipTypeName, int id ) throws SomeException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
