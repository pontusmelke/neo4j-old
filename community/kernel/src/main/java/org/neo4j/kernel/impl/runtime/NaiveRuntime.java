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

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.Runtime;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.runtime.cursors.NaiveCursorFactory;
import org.neo4j.kernel.impl.store.StoreFile;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.io.pagecache.PageCacheOpenOptions.ANY_PAGE_SIZE;

public class NaiveRuntime implements Runtime, Lifecycle
{
    private final PageCache pageCache;
    private final File storeDir;
    private CursorFactory cursorFactory;
    private Read read;
    private List<PagedFile> pagedFiles;

    public static final int NODE_STORE_PAGE_SIZE = 8190;
    public static final int RELATIONSHIP_STORE_PAGE_SIZE = 8160;
    public static final int PROPERTY_STORE_PAGE_SIZE = 8159;

    public NaiveRuntime( PageCache pageCache, File storeDir )
    {
        this.pageCache = pageCache;
        this.storeDir = storeDir;
    }

    private PagedFile pagedStore( StoreFile store, int pageSize )
    {
        File storeFile = new File( storeDir, store.storeFileName() );

        try
        {
            return pageCache.map( storeFile, pageSize, ANY_PAGE_SIZE, StandardOpenOption.CREATE );
        }
        catch ( IOException e )
        {
            throw new PoorlyNamedException(
                    String.format( "Error while mapping '%s' to page cache", storeFile.getAbsolutePath() ), e );
        }
    }

    @Override
    public CursorFactory cursorFactory()
    {
        return cursorFactory;
    }

    @Override
    public Read read()
    {
        return read;
    }

    @Override
    public Write write()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void close() throws Exception
    {
    }

    // LIFE CYCLE

    @Override
    public void init() throws Throwable
    {
        this.pagedFiles = new ArrayList<>();
    }

    @Override
    public void start() throws Throwable
    {
        PagedFile nodeStore = pagedStore( StoreFile.NODE_STORE, NODE_STORE_PAGE_SIZE );
        pagedFiles.add( nodeStore );
        PagedFile relationshipStore = pagedStore( StoreFile.RELATIONSHIP_STORE, RELATIONSHIP_STORE_PAGE_SIZE );
        pagedFiles.add( relationshipStore );
        PagedFile propertyStore = pagedStore( StoreFile.PROPERTY_STORE, PROPERTY_STORE_PAGE_SIZE );
        pagedFiles.add( propertyStore );

        this.read = new NaiveRead( nodeStore, relationshipStore, propertyStore );
        this.cursorFactory = new NaiveCursorFactory( read );
    }

    @Override
    public void stop() throws Throwable
    {
        for ( PagedFile pagedFile : pagedFiles )
        {
            pagedFile.close();
        }
        pagedFiles.clear();
        this.cursorFactory = null;
        this.read = null;
    }

    @Override
    public void shutdown() throws Throwable
    {
        this.pagedFiles = null;
    }
}
