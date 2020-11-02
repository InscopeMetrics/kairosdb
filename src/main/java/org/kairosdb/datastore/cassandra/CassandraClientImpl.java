package org.kairosdb.datastore.cassandra;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.codahale.metrics.Snapshot;
import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TimestampGenerator;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by bhawkins on 3/4/15.
 */
public class CassandraClientImpl implements CassandraClient {
    public static final Logger logger = LoggerFactory.getLogger(CassandraClientImpl.class);
    private final String m_keyspace;
    private final String m_replication;
    private final CassandraConfiguration m_configuration;
    private Cluster m_cluster;
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
        m_writeLoadBalancingPolicy = new TokenAwarePolicy((m_configuration.getLocalDatacenter() == null) ? new RoundRobinPolicy() : DCAwareRoundRobinPolicy.builder().withLocalDc(m_configuration.getLocalDatacenter()).build(), false);
        final TokenAwarePolicy readLoadBalancePolicy = new TokenAwarePolicy((m_configuration.getLocalDatacenter() == null) ? new RoundRobinPolicy() : DCAwareRoundRobinPolicy.builder().withLocalDc(m_configuration.getLocalDatacenter()).build(), true);
        final Cluster.Builder builder = new Cluster.Builder()
                .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(m_configuration.getConnectionTimeout())
                        .setReadTimeoutMillis(m_configuration.getReadTimeout()))
                .withPoolingOptions(new PoolingOptions().setConnectionsPerHost(HostDistance.LOCAL,
                        m_configuration.getLocalCoreConnections(), m_configuration.getLocalMaxConnections())
                        .setConnectionsPerHost(HostDistance.REMOTE,
                                m_configuration.getRemoteCoreConnections(), m_configuration.getRemoteMaxConnections())
                        .setMaxRequestsPerConnection(HostDistance.LOCAL, m_configuration.getLocalMaxReqPerConn())
                        .setMaxRequestsPerConnection(HostDistance.REMOTE, m_configuration.getRemoteMaxReqPerConn())
                        .setMaxQueueSize(m_configuration.getMaxQueueSize()))
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(100, 5 * 1000))
                .withLoadBalancingPolicy(new SelectiveLoadBalancingPolicy(readLoadBalancePolicy, m_writeLoadBalancingPolicy))
                .withCompression(ProtocolOptions.Compression.LZ4)
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

        if (m_configuration.isUseSsl())
            builder.withSSL();

        m_cluster = builder.build();

    }

    public LoadBalancingPolicy getWriteLoadBalancingPolicy() {
        return m_writeLoadBalancingPolicy;
    }

    @Override
    public Session getKeyspaceSession() {
        return m_cluster.connect(m_keyspace);
    }

    @Override
    public Session getSession() {
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
