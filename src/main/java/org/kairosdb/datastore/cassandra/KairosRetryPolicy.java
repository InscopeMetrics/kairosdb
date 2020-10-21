package org.kairosdb.datastore.cassandra;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Named;

public class KairosRetryPolicy implements RetryPolicy {
    public static final Logger logger = LoggerFactory.getLogger(KairosRetryPolicy.class);

    private final int m_retryCount;

    private final AtomicInteger m_readRetries = new AtomicInteger(0);
    private final AtomicInteger m_writeRetries = new AtomicInteger(0);
    private final AtomicInteger m_unavailableRetries = new AtomicInteger(0);
    private final AtomicInteger m_errorRetries = new AtomicInteger(0);

    @Inject
    public KairosRetryPolicy(@Named("request_retry_count") final int retryCount, final PeriodicMetrics periodicMetrics) {
        m_retryCount = retryCount;
        periodicMetrics.registerPolledMetric(this::recordMetrics);
    }

    @Override
    public RetryDecision onReadTimeout(final Statement statement, final ConsistencyLevel cl,
                                       final int requiredResponses, final int receivedResponses, final boolean dataRetrieved, final int nbRetry) {
        if (nbRetry == m_retryCount)
            return RetryDecision.rethrow();
        else {
            final int count = m_readRetries.incrementAndGet();
            return RetryDecision.tryNextHost(cl);
        }
    }

    @Override
    public RetryDecision onWriteTimeout(final Statement statement, final ConsistencyLevel cl,
                                        final WriteType writeType, final int requiredAcks, final int receivedAcks, final int nbRetry) {
        if (nbRetry == m_retryCount)
            return RetryDecision.rethrow();
        else {
            m_writeRetries.incrementAndGet();
            return RetryDecision.tryNextHost(cl);
        }
    }

    @Override
    public RetryDecision onUnavailable(final Statement statement, final ConsistencyLevel cl,
                                       final int requiredReplica, final int aliveReplica, final int nbRetry) {
        if (nbRetry == m_retryCount)
            return RetryDecision.rethrow();
        else {
            m_unavailableRetries.incrementAndGet();
            return RetryDecision.tryNextHost(cl);
        }
    }

    @Override
    public RetryDecision onRequestError(final Statement statement, final ConsistencyLevel cl,
                                        final DriverException e, final int nbRetry) {
        if (nbRetry == m_retryCount)
            return RetryDecision.rethrow();
        else {
            m_errorRetries.incrementAndGet();
            return RetryDecision.tryNextHost(cl);
        }
    }

    @Override
    public void init(final Cluster cluster) {
        logger.info("Initializing KairosRetryPolicy: retry count set to " + m_retryCount);
    }

    @Override
    public void close() {
        logger.info("Closing KairosRetryPolicy");
    }

    private void recordMetrics(final PeriodicMetrics periodicMetrics) {
        final int readTimeouts = m_readRetries.getAndSet(0);
        final int writeTimeouts = m_writeRetries.getAndSet(0);
        final int unavailable = m_unavailableRetries.getAndSet(0);
        final int requestError = m_errorRetries.getAndSet(0);
        final long totalRetries = readTimeouts + writeTimeouts + unavailable + requestError;
        periodicMetrics.recordGauge("datastore/cassandra/retry_count", totalRetries);
        periodicMetrics.recordGauge("datastore/cassandra/retry_count/read_timeout", readTimeouts);
        periodicMetrics.recordGauge("datastore/cassandra/retry_count/write_timeout", writeTimeouts);
        periodicMetrics.recordGauge("datastore/cassandra/retry_count/unavailable", unavailable);
        periodicMetrics.recordGauge("datastore/cassandra/retry_count/request_error", requestError);
    }
}
