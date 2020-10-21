package org.kairosdb.util;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Stopwatch;
import org.kairosdb.eventbus.Subscribe;
import org.kairosdb.events.ShutdownEvent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;

/**
 * Created by bhawkins on 10/27/16.
 */
public class IngestExecutorService {
    public static final String PERMIT_COUNT = "kairosdb.ingest_executor.thread_count";


    private final PeriodicMetrics m_periodicMetrics;
    private final ExecutorService m_internalExecutor;
    private final ThreadGroup m_threadGroup;
    //Original idea behind this is that the number of threads could
    //adjust via incrementing or decrementing the semaphore count.
    private final CongestionSemaphore m_semaphore;
    private final Retryer<Integer> m_retryer;
    private int m_permitCount = 10;
    private final Stopwatch m_timer = Stopwatch.createStarted();

	/*private void increasePermitCount()
	{
		m_permitCount ++;
		//m_congestionTimer.setTaskPerBatch(m_permitCount);
		m_semaphore.release();
	}*/

    @Inject
    public IngestExecutorService(
            final PeriodicMetrics periodicMetrics,
            @Named(PERMIT_COUNT) final int permitCount) {
        m_periodicMetrics = periodicMetrics;
        m_permitCount = permitCount;
        //m_congestionTimer = new CongestionTimer(m_permitCount);
        m_semaphore = new CongestionSemaphore(m_permitCount);
        m_threadGroup = new ThreadGroup("KairosDynamic");
        m_internalExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
            private int m_count = 0;

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(m_threadGroup, r, "Ingest worker-" + m_count++);
                //t.setDaemon(true);
                return t;
            }
        });

        m_retryer = RetryerBuilder.<Integer>newBuilder()
                .retryIfExceptionOfType(NoHostAvailableException.class)
                .retryIfExceptionOfType(UnavailableException.class)
                .withWaitStrategy(WaitStrategies.fibonacciWait(1, TimeUnit.MINUTES))
                .build();
    }

    @Subscribe
    public void shutdown(final ShutdownEvent event) {
        shutdown();
    }

    public void shutdown() {
        m_internalExecutor.shutdown();
    }

    /**
     * Calls to submit will block until a permit is available to process the request
     *
     * @param callable
     */
    public void submit(final Callable<Integer> callable) {
        try {
            //System.out.println("Execute called");
            m_semaphore.acquire();
            //System.out.println("Submitting");
            m_internalExecutor.submit(
                    new IngestFutureTask(m_retryer.wrap(callable)));
            //System.out.println("Done submitting");
        }
        //Potentially thrown by acquire
        catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class CongestionSemaphore extends Semaphore {
        private static final long serialVersionUID = 3505799624122644396L;

        public CongestionSemaphore(final int permits) {
            super(permits);
        }

        public void reducePermits(final int reduction) {
            super.reducePermits(reduction);
        }
    }

    private class IngestFutureTask extends FutureTask<Integer> {
        private final Stopwatch m_stopwatch;

        public IngestFutureTask(final Callable<Integer> callable) {
            super(callable);
            m_stopwatch = Stopwatch.createUnstarted();
        }

        @Override
        public void run() {
            //System.out.println("DynamicFutureTask.run");
            try {
                m_stopwatch.start();
                super.run();
                m_stopwatch.stop();

                m_periodicMetrics.recordGauge("ingest_executor/write_time_micro", m_stopwatch.elapsed(TimeUnit.MICROSECONDS));
            } finally {
                m_semaphore.release();
            }
        }

        @Override
        public void set(final Integer retries) {
            //Todo Calculate time to run and adjust number of threads
			/*if (full)
			{
			}*/

            super.set(retries);
        }
    }
}
