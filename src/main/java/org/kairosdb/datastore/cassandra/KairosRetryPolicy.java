package org.kairosdb.datastore.cassandra;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.google.inject.Inject;
import edu.umd.cs.findbugs.annotations.NonNull;
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
    private final AtomicInteger m_abortedRetries = new AtomicInteger(0);

    @Inject
    public KairosRetryPolicy(@Named("kairosdb.datastore.cassandra.request_retry_count") final int retryCount, final PeriodicMetrics periodicMetrics) {
        m_retryCount = retryCount;
        periodicMetrics.registerPolledMetric(this::recordMetrics);
    }

    @Override
    public RetryDecision onReadTimeout(@NonNull final Request request, @NonNull final ConsistencyLevel cl,
                                       final int requiredResponses, final int receivedResponses, final boolean dataRetrieved, final int nbRetry) {
        if (nbRetry == m_retryCount)
            return RetryDecision.RETHROW;
        else {
            final int count = m_readRetries.incrementAndGet();
            return RetryDecision.RETRY_NEXT;
        }
    }

    @Override
    public RetryDecision onWriteTimeout(@NonNull final Request request, @NonNull final ConsistencyLevel cl,
                                        @NonNull final WriteType writeType, final int requiredAcks, final int receivedAcks, final int nbRetry) {
        if (nbRetry == m_retryCount)
            return RetryDecision.RETHROW;
        else {
            m_writeRetries.incrementAndGet();
            return RetryDecision.RETRY_NEXT;
        }
    }

    @Override
    public RetryDecision onUnavailable(@NonNull final Request request, @NonNull final ConsistencyLevel cl,
                                       final int requiredReplica, final int aliveReplica, final int nbRetry) {
        if (nbRetry == m_retryCount)
            return RetryDecision.RETHROW;
        else {
            m_unavailableRetries.incrementAndGet();
            return RetryDecision.RETRY_NEXT;
        }
    }

    @Override
    public RetryDecision onErrorResponse(@NonNull final Request request, final CoordinatorException e, final int nbRetry) {
        if (nbRetry == m_retryCount)
            return RetryDecision.RETHROW;
        else {
            m_errorRetries.incrementAndGet();
            return RetryDecision.RETRY_NEXT;
        }
    }

    @Override
    public RetryDecision onRequestAborted(@NonNull Request request, @NonNull Throwable error, int nbRetry) {
        if (nbRetry == m_retryCount)
            return RetryDecision.RETHROW;
        else {
            m_abortedRetries.incrementAndGet();
            return RetryDecision.RETRY_NEXT;
        }
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
        final int aborts = m_abortedRetries.getAndSet(0);
        final long totalRetries = readTimeouts + writeTimeouts + unavailable + requestError;
        periodicMetrics.recordGauge("datastore/cassandra/retry_count", totalRetries);
        periodicMetrics.recordGauge("datastore/cassandra/retry_count/read_timeout", readTimeouts);
        periodicMetrics.recordGauge("datastore/cassandra/retry_count/write_timeout", writeTimeouts);
        periodicMetrics.recordGauge("datastore/cassandra/retry_count/unavailable", unavailable);
        periodicMetrics.recordGauge("datastore/cassandra/retry_count/request_error", requestError);
        periodicMetrics.recordGauge("datastore/cassandra/retry_count/request_aborted", requestError);
    }
}
