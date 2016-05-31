package org.neo4j.cypher.internal.codegen.collection;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.core.NodeManager;

public class RelationshipList extends LongArrayCollection implements List<Relationship>
{
    private int size;

    private final NodeManager manager;

    public RelationshipList( NodeManager manager )
    {
        super();
        this.manager = manager;
    }

    public RelationshipList( int capacity, NodeManager manager )
    {
        super(capacity);
        this.manager = manager;
    }

    public boolean addId( long id)
    {
        if (!contains( id ))
        {
            ensureCapacity( size + 1 );
            ids[size++] = id;
            return true;
        }

        return false;
    }

    protected boolean contains( long id )
    {
        for ( int i = 0; i < size; i++ )
        {
            if ( ids[i] == id )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public boolean isEmpty()
    {
        return size == 0;
    }

    @Override
    public boolean contains( Object o )
    {
        return o instanceof Relationship && contains( ((Relationship) o).getId() );
    }

    @Override
    @Nonnull
    public Iterator<Relationship> iterator()
    {
        return new MyIterator();
    }

    @Override
    @Nonnull
    public Object[] toArray()
    {
        Object[] objects = new Object[size];
        Iterator<Relationship> iterator = iterator();
        int index = 0;
        while(iterator.hasNext())
        {
           objects[index++] = iterator.next();
        }

        return objects;
    }

    @SuppressWarnings("unchecked")
    @Override
    @Nonnull
    public <T> T[] toArray( T[] a )
    {
        if (!a.getClass().getComponentType().isAssignableFrom( Relationship.class ))
        {
            throw new ArrayStoreException(  );
        }
        if (a.length < size)
        {
            a = (T[]) Array.newInstance( a.getClass().getComponentType(), size );
        }

        Iterator<Relationship> iterator = iterator();
        int index = 0;
        while(iterator.hasNext())
        {
            a[index++] = (T)iterator.next();
        }

        if (a.length > size)
        {
            a[size] = null;
        }

        return a;
    }

    @Override
    public boolean add( Relationship relationship )
    {
        throw illegalModification();
    }

    @Override
    public boolean remove( Object o )
    {
        throw illegalModification();
    }

    @Override
    public boolean containsAll( Collection<?> c )
    {
        for ( Object o : c )
        {
            if (!contains( o ))
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean addAll( Collection<? extends Relationship> c )
    {
        throw illegalModification();
    }

    @Override
    public boolean addAll( int index, Collection<? extends Relationship> c )
    {
        throw illegalModification();
    }

    @Override
    public boolean removeAll( Collection<?> c )
    {
        throw illegalModification();
    }

    @Override
    public boolean retainAll( Collection<?> c )
    {
        throw illegalModification();
    }

    @Override
    public void clear()
    {
        throw illegalModification();
    }

    private static UnsupportedOperationException illegalModification()
    {
        return new UnsupportedOperationException( "This list is not allowed to be modified" );
    }

    @Override
    public Relationship get( int index )
    {
        return null;
    }

    @Override
    public Relationship set( int index, Relationship element )
    {
        throw illegalModification();
    }

    @Override
    public void add( int index, Relationship element )
    {
        throw illegalModification();
    }

    @Override
    public Relationship remove( int index )
    {
        throw illegalModification();
    }

    @Override
    public int indexOf( Object o )
    {
        return 0;
    }

    @Override
    public int lastIndexOf( Object o )
    {
        return 0;
    }

    @Override
    @Nonnull
    public ListIterator<Relationship> listIterator()
    {
        return new MyListIterator(0);
    }

    @Override
    @Nonnull
    public ListIterator<Relationship> listIterator( int index )
    {
        if ( index < 0 || index > size )
        {
            throw new IndexOutOfBoundsException( "Index: " + index );
        }
        return new MyListIterator(index);
    }

    @Override
    @Nonnull
    public List<Relationship> subList( int fromIndex, int toIndex )
    {
        if ( fromIndex < 0 )
        {
            throw new IndexOutOfBoundsException( "fromIndex = " + fromIndex );
        }
        if ( toIndex > size )
        {
            throw new IndexOutOfBoundsException( "toIndex = " + toIndex );
        }
        if ( fromIndex > toIndex )
        {
            throw new IllegalArgumentException( "fromIndex(" + fromIndex +
                                                ") > toIndex(" + toIndex + ")" );
        }

        int newSize = toIndex - fromIndex;
        RelationshipList list = new RelationshipList( newSize, this.manager);
        System.arraycopy( ids, fromIndex, list.ids, fromIndex, newSize );

        return  list;
    }

    private class MyIterator implements Iterator<Relationship>
    {
        int position = 0;

        @Override
        public boolean hasNext()
        {
            return position != size;
        }

        @Override
        public Relationship next()
        {
            if (position >= size)
            {
                throw new NoSuchElementException(  );
            }
            return manager.newRelationshipProxyById( ids[position++] );
        }
    }

    private class MyListIterator extends MyIterator implements ListIterator<Relationship>
    {

        public MyListIterator(int position)
        {
            this.position = position;
        }

        @Override
        public boolean hasPrevious()
        {
            return position != 0;
        }

        @Override
        public Relationship previous()
        {
            position -= 1;
            if (position < 0)
            {
                throw new NoSuchElementException();
            }
            return manager.newRelationshipProxyById( ids[position] );
        }

        @Override
        public int nextIndex()
        {
            return position;
        }

        @Override
        public int previousIndex()
        {
            return position - 1;
        }

        @Override
        public void remove()
        {
            throw illegalModification();
        }

        @Override
        public void set( Relationship relationship )
        {

            throw illegalModification();
        }

        @Override
        public void add( Relationship relationship )
        {
            throw illegalModification();
        }
    }
}
