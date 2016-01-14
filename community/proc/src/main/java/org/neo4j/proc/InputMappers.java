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
package org.neo4j.proc;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.proc.ProcedureSignature.FieldSignature;
import org.neo4j.proc.TypeMappers.ToNeoValue;
import org.neo4j.proc.TypeMappers.ToProcedureValue;

import static java.util.Arrays.asList;

public class InputMappers
{
    //TODO this need to be some sort of singleton injected here
    private final TypeMappers typeMappers = new TypeMappers();

    /**
     * Converts arguments coming from Neo4j to match the native signature of the procedure.
     */
    public static class InputMapper
    {
        private final List<FieldSignature> signature;
        private final ToProcedureValue[] mappers;


        public InputMapper( FieldSignature[] signature, ToProcedureValue[] mappers )
        {
            this.signature = asList( signature );
            this.mappers = mappers;
        }

        /**
         * In-place type conversion of args to match what is declared in the native procedure.
         * @param args The arguments to be converted
         * @throws ProcedureException If the conversion is not possible.
         */
        public void apply( Object[] args ) throws ProcedureException
        {
            assert args.length == mappers.length;
            for ( int i = 0; i < args.length; i++ )
            {
                args[i] = mappers[i].apply( args[i] );
            }
        }

        public List<FieldSignature> signature()
        {
            return signature;
        }
    }

    public InputMapper mapper( Method method ) throws ProcedureException
    {
        Parameter[] params = method.getParameters();
        FieldSignature[] signature = new FieldSignature[params.length];
        ToProcedureValue[] mappers = new ToProcedureValue[params.length];
        for ( int i = 0; i < params.length; i++ )
        {
            Parameter param = params[i];

            if ( !param.isAnnotationPresent( FieldName.class ) )
            {
                throw new ProcedureException( Status.Procedure.FailedRegistration,
                        "Argument at position %d in method `%s` is missing an `@%s` annotation. " +
                        "Please add the annotation, recompile the class and try again.", i, method.getName(),
                        FieldName.class.getSimpleName() );
            }
            String name = param.getAnnotation( FieldName.class ).value();

            try
            {
                ToNeoValue neoValue = typeMappers.javaToNeo( param.getType() );
                mappers[i]= typeMappers.neoToJava( param.getParameterizedType() );
                signature[i] = new FieldSignature( name, neoValue.type() );
            }
            catch ( ProcedureException e )
            {
                throw new ProcedureException( e.status(),
                        "Argument `%s` at position %d in `%s` with type `%s` cannot be converted to a Neo4j type: %s",
                        name, i, method.getName(), param.getType().getSimpleName(), e.getLocalizedMessage() );
            }

        }

        return new InputMapper( signature, mappers );
    }
}
