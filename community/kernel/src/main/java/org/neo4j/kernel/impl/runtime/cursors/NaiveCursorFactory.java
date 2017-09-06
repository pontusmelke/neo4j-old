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
package org.neo4j.kernel.impl.runtime.cursors;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeManualIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipManualIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

public class NaiveCursorFactory implements CursorFactory
{
    private final Read read;

    public NaiveCursorFactory( Read read )
    {
        this.read = read;
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return new NaiveNodeCursor();
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return new NaiveRelationshipScanCursor( read );
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public NodeManualIndexCursor allocateNodeManualIndexCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RelationshipManualIndexCursor allocateRelationshipManualIndexCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
