/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.runtime.cursors;

import org.neo4j.collection.primitive.PrimitiveLongStack;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.runtime.cursors.NaiveConstants.NO_NODE;
import static org.neo4j.kernel.impl.runtime.cursors.NaiveConstants.NO_RELATIONSHIP;

public class VirtualGroupAwareRelationshipTraversalCursor extends NaiveRelationshipTraversalCursor
{
    private static int NOT_INITIALIZED = -1;
    private VirtualRelationshipTraversalCursor virtualCursor;

    public void init( PageCursor pageCursor, long originNodeReference, long startAddress, Read read )
    {
        super.init( pageCursor, originNodeReference, startAddress, read );
    }

    public void setVirtualCursor( VirtualRelationshipTraversalCursor virtualCursor )
    {
        this.virtualCursor = virtualCursor;
    }

    @Override
    protected long nextRelationshipReference()
    {
        if ( isVirtual() )
        {
            return virtualCursor.nextRelationshipReference();
        }
        return super.nextRelationshipReference();
    }

    public boolean isVirtual()
    {
        return virtualCursor != null;
    }

    static class VirtualRelationshipTraversalCursor
    {
        private int index;
        private RelationshipChain relationshipChain;
        private PrimitiveLongStack relationships;

        public void setRelationships( PrimitiveLongStack relationships )
        {
            this.relationships = relationships;
            next(); // TODO: Cleanup
        }

        private enum RelationshipChain
        {
            NOT_INITIALIZED,
            OUTGOING,
            INCOMING,
            LOOPS
        };

        public VirtualRelationshipTraversalCursor()
        {
            index = NOT_INITIALIZED;
            relationshipChain = RelationshipChain.NOT_INITIALIZED;
        }

        public void setOutgoing()
        {
            relationshipChain = RelationshipChain.OUTGOING;
        }

        public void setIncoming()
        {
            relationshipChain = RelationshipChain.INCOMING;
        }

        public void setLoops()
        {
            relationshipChain = RelationshipChain.LOOPS;
        }

        private boolean next()
        {
            assert relationshipChain != RelationshipChain.NOT_INITIALIZED;
            // NOTE: This assumes NOT_INITIALIZED is -1
            if ( index < relationships.size() - 1 )
            {
                index++;
                return true;
            }
            else
            {
                return false;
            }
        }

        public long nextRelationshipReference()
        {
            if ( next() )
            {
                return relationships.peekAt( index );
            }
            return NO_RELATIONSHIP;
        }
    }
}
