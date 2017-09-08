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

import java.util.Set;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;

public class NaiveLabels implements LabelSet
{
    private final int[] labels;

    NaiveLabels( int[] labels )
    {
        this.labels = labels;
    }

    @Override
    public int numberOfLabels()
    {
        return labels.length;
    }

    @Override
    public int label( int offset )
    {
        return labels[offset];
    }

    static NaiveLabels of( Set<Integer> labelSet )
    {
        int i = 0;
        int[] array = new int[labelSet.size()];
        for ( Integer labelId : labelSet )
        {
            array[i++] = labelId;
        }
        return new NaiveLabels( array );
    }

    public static NaiveLabels augment( LabelSet labels, ReadableDiffSets<Integer> labelDiff )
    {
        int i = 0;
        int[] array = new int[labels.numberOfLabels() + labelDiff.delta()];

        for ( int j = 0; j < labels.numberOfLabels(); j++ )
        {
            int committedLabel = labels.label( j );
            if ( !labelDiff.isRemoved( committedLabel ) )
            {
                array[i++] = committedLabel;
            }
        }

        for ( Integer addedLabel : labelDiff.getAdded() )
        {
            array[i++] = addedLabel;
        }

        return new NaiveLabels( array );
    }
}
