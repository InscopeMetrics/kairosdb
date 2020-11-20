package org.kairosdb.core.queue;

import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.kairosdb.core.reporting.NoTagsTagger;
import org.kairosdb.core.reporting.Tagger;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Named;

/**
 * Created by bhawkins on 12/15/16.
 */
public class MemoryQueueProcessor extends QueueProcessor {
    public static final Logger logger = LoggerFactory.getLogger(MemoryQueueProcessor.class);
    private static final String QUEUE_PERSISTED_SAMPLES_TAGGER = "queue.persisted";
    private static final String QUEUE_DROPPED_SAMPLES_TAGGER ="queue.dropped";

    @Inject(optional = true)
    @Named(QUEUE_PERSISTED_SAMPLES_TAGGER)
    private Tagger m_persistedSamplesTagger = new NoTagsTagger.Builder().build();
    @Inject(optional = true)
    @Named(QUEUE_DROPPED_SAMPLES_TAGGER)
    private Tagger m_droppedSamplesTagger = new NoTagsTagger.Builder().build();

    private final BlockingQueue<DataPointEvent> m_queue;
    private final MetricsFactory m_metricsFactory;
    private final AtomicInteger m_readFromQueueCount = new AtomicInteger();

    @Inject
    public MemoryQueueProcessor(
            @Named(QUEUE_PROCESSOR) final ExecutorService executor,
            final MetricsFactory metricsFactory,
            final PeriodicMetrics periodicMetrics,
            @Named(BATCH_SIZE) final int batchSize,
            @Named(MEMORY_QUEUE_SIZE) final int memoryQueueSize,
            @Named(MINIMUM_BATCH_SIZE) final int minimumBatchSize,
            @Named(MINIMUM_BATCH_WAIT) final int minBatchWait) {
        super(executor, periodicMetrics, batchSize, minimumBatchSize, minBatchWait);
        m_queue = new ArrayBlockingQueue<>(memoryQueueSize, true);
        m_metricsFactory = metricsFactory;
    }

    @Override
    public void addReportedMetrics(final PeriodicMetrics periodicMetrics) {
        periodicMetrics.recordGauge("queue/process_count", m_readFromQueueCount.getAndSet(0));
        periodicMetrics.recordGauge("queue/memory_queue_size", m_queue.size());
    }

    @Override
    public void put(final DataPointEvent dataPointEvent) {
        if (!m_queue.offer(dataPointEvent)) {
            try (Metrics metrics = m_metricsFactory.create()) {
                m_droppedSamplesTagger.applyTagsToMetrics(
                        metrics,
                        dataPointEvent::getMetricName,
                        () -> dataPointEvent.getTags().asMultimap());
                metrics.incrementCounter("queue/dropped_samples", dataPointEvent.getDataPoint().getSampleCount());
            }
            logger.error("Error putting data; memory queue full");
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
        } catch (final InterruptedException e) {
            logger.error("Error taking from queue", e);
        }
        m_queue.drainTo(ret, batchSize - 1);

        m_readFromQueueCount.getAndAdd(ret.size());
        return ret;
    }

    @Override
    protected EventCompletionCallBack getCompletionCallBack(final List<DataPointEvent> batch) {
        return () -> {
            // TODO: Optimize periodic recording with tags.
            // Step 1: PeriodicMetrics supports tagging we can cache these at the class level
            // Step 2: PeriodicMetrics supports sub-scopes we just need a single instance
            final Map<Multimap<String, String>, Metrics> metricsByTag = Maps.newHashMap();
            for (final DataPointEvent dpe :batch) {
                final Multimap<String, String> tags = m_persistedSamplesTagger.createTags(
                        dpe::getMetricName,
                        () -> dpe.getTags().asMultimap());

                Metrics metrics = metricsByTag.get(tags);
                if (metrics == null) {
                    final Metrics newMetrics = m_metricsFactory.create();
                    tags.entries().forEach(e -> newMetrics.addAnnotation(e.getKey(), e.getValue()));
                    metricsByTag.put(tags, newMetrics);
                    metrics = newMetrics;
                }

                metrics.incrementCounter("queue/persisted_samples", dpe.getDataPoint().getSampleCount());
            }
            metricsByTag.values().forEach(Metrics::close);
        };
    }
}
