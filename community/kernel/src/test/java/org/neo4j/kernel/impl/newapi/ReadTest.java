package org.neo4j.kernel.impl.newapi;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.newapi.Read.FILTER_MASK;
import static org.neo4j.kernel.impl.newapi.Read.addFilteringFlag;
import static org.neo4j.kernel.impl.newapi.Read.invertReference;
import static org.neo4j.kernel.impl.newapi.Read.needsFiltering;
import static org.neo4j.kernel.impl.newapi.Read.removeFilteringFlag;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

/**
 * Created by pontusmelke on 2017-09-28.
 */
public class ReadTest
{

    @Test
    public void shouldPreserveNoId()
    {
        assertThat( invertReference( NO_ID ), equalTo( (long) NO_ID ) );
    }

    @Test
    public void shouldInvertTheInvert()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            //TODO what is the real upper limit
            long reference = random.nextLong( FILTER_MASK );
            assertThat( invertReference( invertReference( reference ) ), equalTo( reference ) );
        }
    }

    @Test
    public void shouldSetFilterFlag()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            //TODO what is the real upper limit
            long reference = random.nextLong( FILTER_MASK );
            assertFalse( reference + " shouldn't need to be filtered " + FILTER_MASK, needsFiltering( reference ) );
            long needsFiltering = addFilteringFlag( reference );
            assertTrue( needsFiltering( needsFiltering ) );
            assertThat( removeFilteringFlag( needsFiltering ), equalTo( reference ) );
        }
    }

    @Test
    public void shouldOnlyFilterRealReferences()
    {
        assertThat( addFilteringFlag( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( addFilteringFlag( -1337L ), equalTo( -1337L ) );
    }


}
