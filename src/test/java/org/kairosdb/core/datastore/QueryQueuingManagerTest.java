//
//  QueryQueuingManagerTest.java
//
// Copyright 2016, KairosDB Authors
//        
package org.kairosdb.core.datastore;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.MockitoAnnotations.openMocks;

public class QueryQueuingManagerTest {
    private AtomicInteger runningCount;

    @Mock
    private PeriodicMetrics periodicMetrics;
    private AutoCloseable mocks;

    @Before
    public void setup() {
        mocks = openMocks(this);
        runningCount = new AtomicInteger();
    }

    @After
    public void tearDown() throws Exception {
        mocks.close();
    }

    @Test(timeout = 3000)
    public void test_onePermit() throws InterruptedException {
        final QueryQueuingManager manager = new QueryQueuingManager(periodicMetrics, 1);

        final List<Query> queries = new ArrayList<Query>();
        queries.add(new Query(manager, "1", 5));
        queries.add(new Query(manager, "2", 5));
        queries.add(new Query(manager, "3", 5));
        queries.add(new Query(manager, "4", 5));
        queries.add(new Query(manager, "5", 5));

        for (final Query query : queries) {
            query.start();
        }

        for (final Query query : queries) {
            query.join();
        }

        final Map<Long, Query> map = new HashMap<Long, Query>();
        for (final Query query : queries) {
            assertThat(query.didRun, equalTo(true));
            map.put(query.queriesWatiting, query);
        }

        // Not sure which thread gets the semaphore first  so we add them to a map and verify that some thread
        // had 4 threads waiting, 3 threads, etc.
        assertThat(map.size(), equalTo(5));
        assertThat(map, hasKey(4L));
        assertThat(map, hasKey(3L));
        assertThat(map, hasKey(2L));
        assertThat(map, hasKey(1L));
        assertThat(map, hasKey(0L));
    }

    @Test(timeout = 3000)
    public void test_onePermitSameHash() throws InterruptedException {
        final QueryQueuingManager manager = new QueryQueuingManager(periodicMetrics, 3);

        final Query query1 = new Query(manager, "1", 5);
        final Query query2 = new Query(manager, "1", 5);
        final Query query3 = new Query(manager, "1", 5);
        final Query query4 = new Query(manager, "1", 5);
        final Query query5 = new Query(manager, "1", 5);

        query1.start();
        query2.start();
        query3.start();
        query4.start();
        query5.start();

        query1.join();
        query2.join();
        query3.join();
        query4.join();
        query5.join();

        assertThat(query1.didRun, equalTo(true));
        assertThat(query2.didRun, equalTo(true));
        assertThat(query3.didRun, equalTo(true));
        assertThat(query4.didRun, equalTo(true));
        assertThat(query5.didRun, equalTo(true));
    }

    @Test(timeout = 3000)
    public void test_EnoughPermitsDifferentHashes() throws InterruptedException {
        final QueryQueuingManager manager = new QueryQueuingManager(periodicMetrics, 3);

        final Query query1 = new Query(manager, "1", 3);
        final Query query2 = new Query(manager, "2", 3);
        final Query query3 = new Query(manager, "3", 3);

        query1.start();
        query2.start();
        query3.start();

        query1.join();
        query2.join();
        query3.join();

        assertThat(query1.didRun, equalTo(true));
        assertThat(query2.didRun, equalTo(true));
        assertThat(query3.didRun, equalTo(true));
    }

    private class Query extends Thread {
        private final QueryQueuingManager manager;
        private final String hash;
        private final int waitCount;
        private boolean didRun = false;
        private long queriesWatiting;

        private Query(final QueryQueuingManager manager, final String hash, final int waitCount) {
            this.manager = manager;
            this.hash = hash;
            this.waitCount = waitCount;
        }

        @Override
        public void run() {
            try {
                runningCount.incrementAndGet();
                manager.waitForTimeToRun(hash);
                while (runningCount.get() < waitCount) {
                    Thread.sleep(100);
                }
                queriesWatiting = manager.getQueryWaitingCount();
            } catch (final InterruptedException e) {
                assertFalse("InterruptedException", false);
            }

            didRun = true;
            manager.done(hash);
        }
    }
}
