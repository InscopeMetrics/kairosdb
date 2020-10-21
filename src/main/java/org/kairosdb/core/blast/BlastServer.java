package org.kairosdb.core.blast;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.commons.lang3.RandomUtils;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import javax.inject.Named;

/**
 * Created by bhawkins on 5/16/14.
 */
public class BlastServer implements KairosDBService, Runnable {
    public static final Logger logger = LoggerFactory.getLogger(BlastServer.class);
    public static final String NUMBER_OF_ROWS = "kairosdb.blast.number_of_rows";
    public static final String DURATION_SECONDS = "kairosdb.blast.duration_seconds";
    public static final String METRIC_NAME = "kairosdb.blast.metric_name";
    public static final String TTL = "kairosdb.blast.ttl";

    private final Publisher<DataPointEvent> m_publisher;
    private final LongDataPointFactory m_longDataPointFactory;
    private final int m_ttl;
    private final int m_numberOfRows;
    private final long m_duration;  //in seconds
    private final String m_metricName;
    private final PeriodicMetrics m_periodicMetrics;
    private final AtomicLong m_counter = new AtomicLong(0L);

    private Thread m_serverThread;
    private boolean m_keepRunning = true;

    @Inject
    @Named("HOSTNAME")
    private final String m_hostName = "none";

    @Inject
    private final LongDataPointFactory m_dataPointFactory = new LongDataPointFactoryImpl();

    @Inject
    public BlastServer(
            final FilterEventBus evenBus,
            final LongDataPointFactory longDataPointFactory,
            final PeriodicMetrics periodicMetrics,
            @Named(NUMBER_OF_ROWS) final int numberOfRows,
            @Named(DURATION_SECONDS) final long duration,
            @Named(METRIC_NAME) final String metricName,
            @Named(TTL) final int ttl) {
        m_publisher = evenBus.createPublisher(DataPointEvent.class);
        m_longDataPointFactory = longDataPointFactory;
        m_ttl = ttl;
        m_numberOfRows = numberOfRows;
        m_duration = duration;
        m_metricName = metricName;
        m_periodicMetrics = periodicMetrics;
    }

    @Override
    public void start() {
        m_serverThread = new Thread(this);
        m_serverThread.start();
        m_periodicMetrics.registerPolledMetric(m -> m.recordGauge("blast/submission_count", m_counter.getAndSet(0)));
    }

    @Override
    public void stop() {
        m_keepRunning = false;
    }

    @Override
    public void run() {
        logger.info("Blast Server Running");
        final Stopwatch timer = Stopwatch.createStarted();

        while (m_keepRunning) {
            final long now = System.currentTimeMillis();
            final DataPoint dataPoint = m_longDataPointFactory.createDataPoint(now, 42);
            final int row = RandomUtils.nextInt(0, m_numberOfRows);
            final ImmutableSortedMap<String, String> tags = ImmutableSortedMap.of("row",
                    String.valueOf(row), "host", "blast_server");

            final DataPointEvent dataPointEvent = new DataPointEvent(m_metricName, tags, dataPoint, m_ttl);
            m_publisher.post(dataPointEvent);
            m_counter.incrementAndGet();

            if ((m_counter.get() % 100000 == 0) && (timer.elapsed(TimeUnit.SECONDS) > m_duration)) {
                m_keepRunning = false;
            }
        }
    }
}
