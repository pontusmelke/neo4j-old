package org.neo4j.cypher.internal.codegen.collection;

import java.util.Arrays;

public abstract class LongArrayCollection
{
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
    protected long[] ids;
    private static final int DEFAULT_CAPACITY = 16;

    public LongArrayCollection()
    {
        this(DEFAULT_CAPACITY);
    }

    public LongArrayCollection(int capacity){
        this.ids = new long[capacity];
    }

    protected void ensureCapacity( int minCapacity )
    {
        if (minCapacity - ids.length > 0)
        {
            int oldCapacity = ids.length;
            int newCapacity = oldCapacity + (oldCapacity >> 1);
            //if overflow just pick the minCapacity
            if (newCapacity - minCapacity < 0)
            {
                newCapacity = minCapacity;
            }

            if (newCapacity - MAX_ARRAY_SIZE > 0)
            {
                if (newCapacity < 0)
                {
                    throw new OutOfMemoryError(  );
                }
                newCapacity = MAX_ARRAY_SIZE;
            }
            ids = Arrays.copyOf(ids, newCapacity);
        }
    }
}
