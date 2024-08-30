package org.kairosdb.datastore.cassandra;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.codahale.metrics.Snapshot;
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
    private CqlSession m_cluster;
    private LoadBalancingPolicy m_writeLoadBalancingPolicy;

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
        //Passing shuffleReplicas = false so we can properly batch data to
        //instances.
        // When connecting to Cassandra notes in different datacenters, the local datacenter should be provided.
        // Not doing this will select the datacenter from the first connected Cassandra node, which is not guaranteed to be the correct one.
        DefaultLoadBalancingPolicy
        m_writeLoadBalancingPolicy = new TokenAwarePolicy((m_configuration.getLocalDatacenter() == null) ? new RoundRobinPolicy() : DCAwareRoundRobinPolicy.builder().withLocalDc(m_configuration.getLocalDatacenter()).build(), TokenAwarePolicy.ReplicaOrdering.TOPOLOGICAL);
        final TokenAwarePolicy readLoadBalancePolicy = new TokenAwarePolicy((m_configuration.getLocalDatacenter() == null) ? new RoundRobinPolicy() : DCAwareRoundRobinPolicy.builder().withLocalDc(m_configuration.getLocalDatacenter()).build(), TokenAwarePolicy.ReplicaOrdering.RANDOM);
        DefaultDriverOption.LOAD
        final DriverConfigLoader loader =
                DriverConfigLoader.programmaticBuilder()
                        .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofMillis(m_configuration.getConnectionTimeout()))
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(m_configuration.getReadTimeout()))
                        .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, m_configuration.getLocalMaxConnections())
                        .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, m_configuration.getRemoteMaxConnections())
                        .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, m_configuration.getLocalMaxReqPerConn())
                        .withInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE, m_configuration.getMaxQueueSize())
                        .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, "lz4")
                        .build();



        final CqlSessionBuilder builder = CqlSession.builder()
                .withLocalDatacenter(m_configuration.getLocalDatacenter())
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(100, 5 * 1000))
                .withLoadBalancingPolicy(new SelectiveLoadBalancingPolicy(readLoadBalancePolicy, m_writeLoadBalancingPolicy))
                .withoutJMXReporting()
                .withQueryOptions(new QueryOptions().setConsistencyLevel(m_configuration.getDataReadLevel()))
                .withTimestampGenerator(new TimestampGenerator() //todo need to remove this and put it only on the datapoints call
                {
                    @Override
                    public long next() {
                        return System.currentTimeMillis();
                    }
                })
                .withRetryPolicy(m_retryPolicy);

        if (m_authProvider != null) {
            builder.withAuthProvider(m_authProvider);
        } else if (m_configuration.getAuthUserName() != null && m_configuration.getAuthPassword() != null) {
            builder.withCredentials(m_configuration.getAuthUserName(),
                    m_configuration.getAuthPassword());
        }


        for (final Map.Entry<String, Integer> hostPort : m_configuration.getHostList().entrySet()) {
            logger.info("Connecting to " + hostPort.getKey() + ":" + hostPort.getValue());
            builder.addContactPoint(hostPort.getKey())
                    .withPort(hostPort.getValue());
        }

        if (m_configuration.isUseSsl()) {
            final SSLContext sslContext = SSLContext.getDefault();
            builder.withSslContext(sslContext);
        }

        m_cluster = builder.build();

    }

    public LoadBalancingPolicy getWriteLoadBalancingPolicy() {
        return m_writeLoadBalancingPolicy;
    }

    @Override
    public CqlSession getKeyspaceSession() {
        return m_cluster.connect(m_keyspace);
    }

    @Override
    public CqlSession getSession() {
        return m_cluster.connect();
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
        m_cluster.close();
    }

    private void recordMetrics(final PeriodicMetrics periodicMetrics) {
        String prefix = "datastore/cassandra/client";
        final Metrics metrics = m_cluster.getMetrics();

        periodicMetrics.recordGauge(prefix + "/connection_errors",
                metrics.getErrorMetrics().getConnectionErrors().getCount());

        periodicMetrics.recordGauge(prefix + "/blocking_executor_queue_depth",
                metrics.getBlockingExecutorQueueDepth().getValue());

        periodicMetrics.recordGauge(prefix + "/connected_to_hosts",
                metrics.getConnectedToHosts().getValue());

        periodicMetrics.recordGauge(prefix + "/executor_queue_depth",
                metrics.getExecutorQueueDepth().getValue());

        periodicMetrics.recordGauge(prefix + "/known_hosts",
                metrics.getKnownHosts().getValue());

        periodicMetrics.recordGauge(prefix + "/open_connections",
                metrics.getOpenConnections().getValue());

        periodicMetrics.recordGauge(prefix + "/reconnection_scheduler_queue_size",
                metrics.getReconnectionSchedulerQueueSize().getValue());

        periodicMetrics.recordGauge(prefix + "/task_scheduler_queue_size",
                metrics.getTaskSchedulerQueueSize().getValue());

        periodicMetrics.recordGauge(prefix + "/trashed_connections",
                metrics.getTrashedConnections().getValue());

        final Snapshot snapshot = metrics.getRequestsTimer().getSnapshot();
        prefix = prefix + "/requests_timer";
        periodicMetrics.recordGauge(prefix + "/max", snapshot.getMax());

        periodicMetrics.recordGauge(prefix + "/min", snapshot.getMin());

        periodicMetrics.recordGauge(prefix + "/avg", snapshot.getMean());

        periodicMetrics.recordGauge(prefix + "/count", snapshot.size());
    }
}
