package org.kairosdb.core.queue;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import javax.inject.Named;

/**
 * Created by bhawkins on 12/15/16.
 */
public class MemoryQueueProcessor extends QueueProcessor {
    public static final Logger logger = LoggerFactory.getLogger(MemoryQueueProcessor.class);
    private static final EventCompletionCallBack CALL_BACK = new VoidCompletionCallBack();
    private final BlockingQueue<DataPointEvent> m_queue;
    private final AtomicInteger m_readFromQueueCount = new AtomicInteger();

    @Inject
    public MemoryQueueProcessor(
            @Named(QUEUE_PROCESSOR) final ExecutorService executor,
            final PeriodicMetrics periodicMetrics,
            @Named(BATCH_SIZE) final int batchSize,
            @Named(MEMORY_QUEUE_SIZE) final int memoryQueueSize,
            @Named(MINIMUM_BATCH_SIZE) final int minimumBatchSize,
            @Named(MINIMUM_BATCH_WAIT) final int minBatchWait) {
        super(executor, periodicMetrics, batchSize, minimumBatchSize, minBatchWait);

        m_queue = new ArrayBlockingQueue<>(memoryQueueSize, true);
    }

    @Override
    public void addReportedMetrics(final PeriodicMetrics periodicMetrics) {
        periodicMetrics.recordGauge("queue/process_count", m_readFromQueueCount.getAndSet(0));
        periodicMetrics.recordGauge("queue/memory_queue.size", m_queue.size());
    }

    @Override
    public void put(final DataPointEvent dataPointEvent) {
        try {
            m_queue.put(dataPointEvent);
        } catch (final InterruptedException e) {
            logger.error("Error putting data", e);
        }
    }

    @Override
    protected int getAvailableDataPointEvents() {
        return m_queue.size();
    }

    @Override
    protected List<DataPointEvent> get(final int batchSize) {
        final List<DataPointEvent> ret = new ArrayList<>(batchSize / 4);
        try {
            ret.add(m_queue.take());
            //Thread.sleep(50);
        } catch (final InterruptedException e) {
            logger.error("Error taking from queue", e);
        }
        m_queue.drainTo(ret, batchSize - 1);

        //System.out.println(ret.size());
        m_readFromQueueCount.getAndAdd(ret.size());
        return ret;
    }

    @Override
    protected EventCompletionCallBack getCompletionCallBack() {
        return CALL_BACK;
    }


    private static class VoidCompletionCallBack implements EventCompletionCallBack {
        @Override
        public void complete() {
            //does nothing
        }
    }
}
