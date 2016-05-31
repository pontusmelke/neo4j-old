package org.neo4j.cypher.internal.codegen.collection;

import java.util.EmptyStackException;

public class LongStack extends LongArrayCollection
{
    private int cursor = 0;

    public LongStack()
    {
        super();
    }

    public LongStack(int capacity)
    {
        super(capacity);
    }

    public void push(long l)
    {
        ensureCapacity( cursor + 1 );
        ids[cursor++] = l;
    }

    public long pop()
    {
        if (--cursor < 0)
        {
            throw new EmptyStackException();
        }
        return ids[cursor];
    }

    public boolean nonEmpty()
    {
        return cursor != 0;
    }

    public boolean empty()
    {
        return cursor == 0;
    }
}
