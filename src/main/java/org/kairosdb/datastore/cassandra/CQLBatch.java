package org.kairosdb.datastore.cassandra;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.kairosdb.core.DataPoint;
import org.kairosdb.util.KDataOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import javax.inject.Inject;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.ROW_KEY_METRIC_NAMES;

/**
 * Created by bhawkins on 1/11/17.
 */
public class CQLBatch {
    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private final CqlSession m_session;
    private final Schema m_schema;
    private final PeriodicMetrics m_periodicMetrics;
    private final ConsistencyLevel m_consistencyLevel;
    private final long m_now;
    private final LoadBalancingPolicy m_loadBalancingPolicy;
    private final List<String> m_metricNames = new ArrayList<>();
    private final List<DataPointsRowKey> m_rowKeys = new ArrayList<>();
    private final Map<Node, BatchStatement> m_batchMap = new HashMap<>();
    private final BatchStatement metricNamesBatch = BatchStatement.newInstance(BatchType.UNLOGGED);
    private final BatchStatement dataPointBatch = BatchStatement.newInstance(BatchType.UNLOGGED);
    private final BatchStatement rowKeyBatch = BatchStatement.newInstance(BatchType.UNLOGGED);

    @Inject
    public CQLBatch(
            final ConsistencyLevel consistencyLevel,
            final CqlSession session,
            final PeriodicMetrics periodicMetrics,
            final Schema schema) {
        m_consistencyLevel = consistencyLevel;
        m_session = session;
        m_periodicMetrics = periodicMetrics;
        m_schema = schema;
        m_now = System.currentTimeMillis();
        m_loadBalancingPolicy = m_session.getContext().getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME);
    }

    public void addRowKey(final String metricName, final DataPointsRowKey rowKey, final int rowKeyTtl) {
        m_rowKeys.add(rowKey);

        final ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(0, rowKey.getTimestamp());

        BoundStatement bs = m_schema.psRowKeyTimeInsert.bind()
                .setString(0, metricName)
                .setLong(1, rowKey.getTimestamp())
                .setInt(2, rowKeyTtl)
                .setIdempotent(true)
                .setConsistencyLevel(m_consistencyLevel);

        rowKeyBatch.add(bs);

        bs = m_schema.psRowKeyInsert.bind()
                .setString(0, metricName)
                .setLong(1, rowKey.getTimestamp())
                .setString(2, rowKey.getDataType())
                .setMap(3, rowKey.getTags(), String.class, String.class)
                .setInt(4, rowKeyTtl)
                .setIdempotent(true)
                .setConsistencyLevel(m_consistencyLevel);

        rowKeyBatch.add(bs);
    }

    public void addMetricName(final String metricName) {
        m_metricNames.add(metricName);

        final BoundStatement bs = m_schema.psStringIndexInsert.bind()
                .setBytesUnsafe(0, ByteBuffer.wrap(ROW_KEY_METRIC_NAMES.getBytes(UTF_8)))
                .setString(1, metricName)
                .setConsistencyLevel(m_consistencyLevel);
        metricNamesBatch.add(bs);
    }

    private void addBoundStatement(final BoundStatement boundStatement) {
        final Queue<Node> hosts = m_loadBalancingPolicy.newQueryPlan(boundStatement, m_session);
        if (!hosts.isEmpty()) {
            final Node hostKey = hosts.poll();

            BatchStatement batchStatement = m_batchMap.get(hostKey);
            if (batchStatement == null) {
                batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                m_batchMap.put(hostKey, batchStatement);
            }
            batchStatement.add(boundStatement);
        } else {
            dataPointBatch.add(boundStatement);
        }
    }

    public void deleteDataPoint(final DataPointsRowKey rowKey, final int columnTime) {
        final ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(columnTime);
        b.rewind();

        final BoundStatement boundStatement = m_schema.psDataPointsDelete.bind()
                .setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey))
                .setBytesUnsafe(1, b)
                .setConsistencyLevel(m_consistencyLevel)
                .setIdempotent(true);

        addBoundStatement(boundStatement);
    }

    public void addDataPoint(final DataPointsRowKey rowKey, final int columnTime, final DataPoint dataPoint, final int ttl) throws IOException {
        final KDataOutput kDataOutput = new KDataOutput();
        dataPoint.writeValueToBuffer(kDataOutput);

        final ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(columnTime);
        b.rewind();
        final BoundStatement boundStatement = m_schema.psDataPointsInsert.bind()
                .setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey))
                .setBytesUnsafe(1, b)
                .setBytesUnsafe(2, ByteBuffer.wrap(kDataOutput.getBytes()))
                .setInt(3, ttl)
                .setLong(4, m_now)
                .setConsistencyLevel(m_consistencyLevel)
                .setIdempotent(true);

        addBoundStatement(boundStatement);
    }

    public void submitBatch() {
        if (metricNamesBatch.size() != 0) {
            m_session.execute(metricNamesBatch, GenericType.of(ResultSet.class));
            m_periodicMetrics.recordGauge(
                    "datastore/cassandra/string_index/write_batch_size",
                    metricNamesBatch.size());
        }

        if (rowKeyBatch.size() != 0) {
            m_session.execute(rowKeyBatch, GenericType.of(ResultSet.class));
            m_periodicMetrics.recordGauge(
                    "datastore/cassandra/row_keys/write_batch_size",
                    rowKeyBatch.size());
        }

        for (final BatchStatement batchStatement : m_batchMap.values()) {
            if (batchStatement.size() != 0) {
                m_session.execute(batchStatement, GenericType.of(ResultSet.class));
                m_periodicMetrics.recordGauge(
                        "datastore/cassandra/data_points/write_batch_size",
                        batchStatement.size());
            }
        }

        //Catch all in case of a load balancing problem
        if (dataPointBatch.size() != 0) {
            m_session.execute(dataPointBatch, GenericType.of(ResultSet.class));
            m_periodicMetrics.recordGauge(
                    "datastore/cassandra/data_points/write_batch_size",
                    dataPointBatch.size());
        }
    }

    public Collection<String> getMetricNames() {
        return Collections.unmodifiableCollection(m_metricNames);
    }

    public Collection<DataPointsRowKey> getRowKeys() {
        return Collections.unmodifiableCollection(m_rowKeys);
    }
}
