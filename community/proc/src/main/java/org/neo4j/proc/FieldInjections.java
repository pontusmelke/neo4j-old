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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Supplier;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;


/**
 * Injects annotated fields with appropriate values.
 */
public class FieldInjections
{
    private final TypeMappers typeMappers;

    public FieldInjections( TypeMappers typeMappers )
    {
        this.typeMappers = typeMappers;
    }

    /**
     * On calling apply, injects the `value` for the field `field` on the provided `object`.
     */
    public static class FieldSetter
    {
        private final Field field;
        private final Object value;

        public FieldSetter( Field field, Object value )
        {
            this.field = field;
            this.value = value;
        }

        void apply( Object object ) throws ProcedureException
        {
            {
                try
                {
                    field.set( object, value );
                }
                catch ( IllegalAccessException e )
                {
                    throw new ProcedureException( Status.Procedure.CallFailed,
                            "The field `%s` does not have public access, please change the access to public, recompile " +
                            "and try again,",
                            field.getName() );
                }
            }
        }
    }

    /**
     * For each annotated field in the provided class, creates a `FieldSetter`.
     * @param cls The class where injection should happen.
     * @return A list of `FieldSetters`
     * @throws ProcedureException if the type of the injected field does not match what has been registered.
     */
    public List<FieldSetter> setters( Class<?> cls ) throws ProcedureException
    {
        List<FieldSetter> setters = new LinkedList<>();
        for ( Entry<Class<? extends Annotation>,Supplier<?>> entry : typeMappers.components().entrySet() )
        {
            Class<? extends Annotation> annotation = entry.getKey();
            Object value = entry.getValue().get();
            for ( Field field : cls.getDeclaredFields() )
            {
                if ( field.isAnnotationPresent( annotation ) )
                {
                    if ( !field.getType().isAssignableFrom( value.getClass() ) )
                    {
                        throw new ProcedureException( Status.Procedure.FailedRegistration,
                                "Field `%s` has type `%s` which is not assignable from `%s`.", field.getName(),
                                field.getType(), value.getClass() );
                    }
                    setters.add( new FieldSetter( field, value ) );
                }
            }
        }

        return setters;
    }
}
