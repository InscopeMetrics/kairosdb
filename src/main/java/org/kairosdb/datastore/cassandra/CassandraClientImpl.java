package org.kairosdb.datastore.cassandra;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.codahale.metrics.*;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Map;

/**
 * Created by bhawkins on 3/4/15.
 */
public class CassandraClientImpl implements CassandraClient {
    public static final Logger logger = LoggerFactory.getLogger(CassandraClientImpl.class);
    private final String m_keyspace;
    private final String m_replication;
    private final CassandraConfiguration m_configuration;
    private LoadBalancingPolicy m_writeLoadBalancingPolicy;
    private CqlSession m_session;
    private CqlSession m_keyspaceSession;

    private final RetryPolicy m_retryPolicy;

    @Inject(optional = true)
    private AuthProvider m_authProvider = null;

    @Inject
    public CassandraClientImpl(final CassandraConfiguration configuration, final PeriodicMetrics periodicMetrics) {
        m_configuration = configuration;
        m_keyspace = configuration.getKeyspaceName();
        m_replication = configuration.getReplication();
        periodicMetrics.registerPolledMetric(this::recordMetrics);
        m_retryPolicy = new KairosRetryPolicy(1, periodicMetrics);
    }

    public void init() {
        m_session = createSqlSessionBuilder().build();
        m_keyspaceSession = createSqlSessionBuilder().withKeyspace(m_keyspace).build();
    }

    private CqlSessionBuilder createSqlSessionBuilder() {
        final DriverConfigLoader loader =
                DriverConfigLoader.programmaticBuilder()
                        .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofMillis(m_configuration.getConnectionTimeout()))
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(m_configuration.getReadTimeout()))
                        .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, m_configuration.getLocalMaxConnections())
                        .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, m_configuration.getRemoteMaxConnections())
                        .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, m_configuration.getLocalMaxReqPerConn())
                        .withInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE, m_configuration.getMaxQueueSize())
                        .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, "lz4")
                        .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofMillis(100))
                        .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(5))
                        .withString(DefaultDriverOption.REQUEST_CONSISTENCY, m_configuration.getDataReadLevel().name())
                        .withBoolean(DefaultDriverOption.TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK, true)
                        .build();



        final CqlSessionBuilder builder = CqlSession.builder()
                .withConfigLoader(loader)
                .withLocalDatacenter(m_configuration.getLocalDatacenter());
//                .withRetryPolicy(m_retryPolicy);  TODO: Implement the retry policy

        if (m_authProvider != null) {
            builder.withAuthProvider(m_authProvider);
        } else if (m_configuration.getAuthUserName() != null && m_configuration.getAuthPassword() != null) {
            builder.withCredentials(m_configuration.getAuthUserName(),
                    m_configuration.getAuthPassword());
        }


        for (final Map.Entry<String, Integer> hostPort : m_configuration.getHostList().entrySet()) {
            logger.info("Connecting to " + hostPort.getKey() + ":" + hostPort.getValue());
            builder.addContactPoint(InetSocketAddress.createUnresolved(hostPort.getKey(), hostPort.getValue()));
        }

        if (m_configuration.isUseSsl()) {
            final SSLContext sslContext;
            try {
                sslContext = SSLContext.getDefault();
            } catch (final NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            builder.withSslContext(sslContext);
        }
        return builder;
    }

    public LoadBalancingPolicy getWriteLoadBalancingPolicy() {
        return m_writeLoadBalancingPolicy;
    }

    @Override
    public CqlSession getKeyspaceSession() {
        return m_keyspaceSession;
    }

    @Override
    public CqlSession getSession() {
        return m_session;
    }

    @Override
    public String getKeyspace() {
        return m_keyspace;
    }

    @Override
    public String getReplication() {
        return m_replication;
    }

    @Override
    public void close() {
        m_session.close();
        m_keyspaceSession.close();
    }

    private void recordMetrics(final PeriodicMetrics periodicMetrics) {
        String prefix = "datastore/cassandra/client/";
        if (m_keyspaceSession.getMetrics().isPresent()) {
            final Metrics metrics = m_keyspaceSession.getMetrics().get();

            final MetricRegistry registry = metrics.getRegistry();
            for (Map.Entry<String, Counter> entry : registry.getCounters().entrySet()) {
                final long value = entry.getValue().getCount();
                periodicMetrics.recordGauge(prefix + entry.getKey(), value);
            }
            for (Map.Entry<String, Gauge> entry : registry.getGauges().entrySet()) {
                final long value = (Long) entry.getValue().getValue();
                periodicMetrics.recordGauge(prefix + entry.getKey(), value);
            }
            for (Map.Entry<String, Timer> entry : registry.getTimers().entrySet()) {
                final Snapshot snapshot = entry.getValue().getSnapshot();
                final String timerPrefix = prefix + entry.getKey() + "/";

                periodicMetrics.recordGauge(timerPrefix + "max", snapshot.getMax());
                periodicMetrics.recordGauge(timerPrefix + "min", snapshot.getMin());
                periodicMetrics.recordGauge(timerPrefix + "avg", snapshot.getMean());
                periodicMetrics.recordGauge(timerPrefix + "count", snapshot.size());
            }
        }
    }
}
