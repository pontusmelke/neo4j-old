package org.neo4j.cypher.internal;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.test.rule.EnterpriseDatabaseRule;

import static org.junit.Assert.assertTrue;

public class EnterpriseFoo
{
    private static final boolean FORSETI = false;
    private static final int POOL_SIZE=10;
    @Rule
    public final EnterpriseDatabaseRule db = new EnterpriseDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            if ( !FORSETI )
            {
                builder.setConfig( GraphDatabaseFacadeFactory.Configuration.lock_manager, "community" );
            }
        }
    };

    private static final String MERGE = "MERGE (n0:MyTestLabel {`id`: {param0}}) SET n0 = {param1}";
    private static final String ID = "05adbde2-2740-44f2-bb88-3ff049676f66";

    private static Map<String, Object> VALUES;

    static
    {
        VALUES = new HashMap<>();
        VALUES.put("id", ID);
        VALUES.put("value","whatever");
        VALUES.put("key", "settings");
    }

    @Test
    public void test() throws Exception
    {
        System.out.println("Starting");

        configureConstraint();

        ExecutorService executor = Executors.newCachedThreadPool();

        List<Callable<Object>> updaters = new ArrayList<>();
        for (int y = 0; y < 5; y++)
        {
            updaters.add(new Updater(y));
        }

        List<Future<Object>> futures = executor.invokeAll(updaters);
        boolean success = true;
        int successful = 0;
        for (Future future : futures)
        {
            try
            {
                future.get();
                successful++;
            } catch (ExecutionException e )
            {
                success = false;
                e.getCause().printStackTrace();
            }
        }
        System.out.println("successful: "+ successful);
        assertTrue("all threads should be successful", success);

        System.out.println("Done");
        System.exit(0);
    }

    private void configureConstraint()
    {
        try(Transaction transaction = db.beginTx())
        {
            db.execute("CREATE CONSTRAINT ON (p:MyTestLabel) ASSERT p.id IS UNIQUE");

            transaction.success();
        }
    }

    private class Updater implements Callable<Object>
    {
        private int number;

        Updater( int number )
        {
            this.number = number;
        }

        @Override
        public Object call() throws Exception
        {
            String name = Thread.currentThread().getName();
            Thread.currentThread().setName( "Runner" + number );
            try
            {
                System.out.println( "Running " + number );

                for ( int i = 0; i < 100; i++ )
                {
                    Transaction transaction = null;
                    try
                    {
                        transaction = db.beginTx();
                        Map<String,Object> params = new HashMap<>();
                        params.put( "param0", ID );
                        params.put( "param1", VALUES );
                        db.execute( MERGE, params );

                        transaction.success();

                    }
                    catch ( Exception e )
                    {
//                        System.out.printf( "Failure on %d after %d iterations.%n", number, i );
                        transaction.failure();
                        throw e;
                    }
                    finally
                    {
                        if ( transaction != null )
                        {
                            transaction.close();
                        }
                    }

                }
                return null;
            }
            finally
            {
                Thread.currentThread().setName( name );
            }
        }
    }
}
