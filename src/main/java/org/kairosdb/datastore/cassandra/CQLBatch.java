package org.kairosdb.datastore.cassandra;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.kairosdb.core.DataPoint;
import org.kairosdb.util.KDataOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Named;

import static org.kairosdb.datastore.cassandra.CassandraConfiguration.KEYSPACE_PROPERTY;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.ROW_KEY_METRIC_NAMES;

/**
 * Created by bhawkins on 1/11/17.
 */
public class CQLBatch {
    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private final Session m_session;
    private final Schema m_schema;
    private final PeriodicMetrics m_periodicMetrics;
    private final ConsistencyLevel m_consistencyLevel;
    private final long m_now;
    private final LoadBalancingPolicy m_loadBalancingPolicy;
    private final List<String> m_metricNames = new ArrayList<>();
    private final List<DataPointsRowKey> m_rowKeys = new ArrayList<>();
    private final Map<Host, BatchStatement> m_batchMap = new HashMap<>();
    private final BatchStatement metricNamesBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
    private final BatchStatement dataPointBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
    private final BatchStatement rowKeyBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
    @Inject
    @Named(KEYSPACE_PROPERTY)
    private String m_keyspace = "kairosdb";

    @Inject
    public CQLBatch(
            final ConsistencyLevel consistencyLevel,
            final Session session,
            final PeriodicMetrics periodicMetrics,
            final Schema schema,
            final LoadBalancingPolicy loadBalancingPolicy) {
        m_consistencyLevel = consistencyLevel;
        m_session = session;
        m_periodicMetrics = periodicMetrics;
        m_schema = schema;
        m_now = System.currentTimeMillis();
        m_loadBalancingPolicy = loadBalancingPolicy;
    }

    public void addRowKey(final String metricName, final DataPointsRowKey rowKey, final int rowKeyTtl) {
        m_rowKeys.add(rowKey);

        final ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(0, rowKey.getTimestamp());

        Statement bs = m_schema.psRowKeyTimeInsert.bind()
                .setString(0, metricName)
                .setTimestamp(1, new Date(rowKey.getTimestamp()))
                .setInt(2, rowKeyTtl)
                .setIdempotent(true);

        bs.setConsistencyLevel(m_consistencyLevel);

        rowKeyBatch.add(bs);

        bs = m_schema.psRowKeyInsert.bind()
                .setString(0, metricName)
                .setTimestamp(1, new Date(rowKey.getTimestamp()))
                .setString(2, rowKey.getDataType())
                .setMap(3, rowKey.getTags())
                .setInt(4, rowKeyTtl)
                .setIdempotent(true);

        bs.setConsistencyLevel(m_consistencyLevel);

        rowKeyBatch.add(bs);
    }

    public void addMetricName(final String metricName) {
        m_metricNames.add(metricName);

        final BoundStatement bs = new BoundStatement(m_schema.psStringIndexInsert);
        bs.setBytesUnsafe(0, ByteBuffer.wrap(ROW_KEY_METRIC_NAMES.getBytes(UTF_8)));
        bs.setString(1, metricName);
        bs.setConsistencyLevel(m_consistencyLevel);
        metricNamesBatch.add(bs);
    }

    private void addBoundStatement(final BoundStatement boundStatement) {
        final Iterator<Host> hosts = m_loadBalancingPolicy.newQueryPlan(m_keyspace, boundStatement);
        if (hosts.hasNext()) {
            final Host hostKey = hosts.next();

            BatchStatement batchStatement = m_batchMap.get(hostKey);
            if (batchStatement == null) {
                batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                m_batchMap.put(hostKey, batchStatement);
            }
            batchStatement.add(boundStatement);
        } else {
            dataPointBatch.add(boundStatement);
        }
    }

    public void deleteDataPoint(final DataPointsRowKey rowKey, final int columnTime) throws IOException {
        final BoundStatement boundStatement = new BoundStatement(m_schema.psDataPointsDelete);
        boundStatement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
        final ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(columnTime);
        b.rewind();
        boundStatement.setBytesUnsafe(1, b);

        boundStatement.setConsistencyLevel(m_consistencyLevel);
        boundStatement.setIdempotent(true);

        addBoundStatement(boundStatement);
    }

    public void addDataPoint(final DataPointsRowKey rowKey, final int columnTime, final DataPoint dataPoint, final int ttl) throws IOException {
        final KDataOutput kDataOutput = new KDataOutput();
        dataPoint.writeValueToBuffer(kDataOutput);

        final BoundStatement boundStatement = new BoundStatement(m_schema.psDataPointsInsert);
        boundStatement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
        final ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(columnTime);
        b.rewind();
        boundStatement.setBytesUnsafe(1, b);
        boundStatement.setBytesUnsafe(2, ByteBuffer.wrap(kDataOutput.getBytes()));
        boundStatement.setInt(3, ttl);
        boundStatement.setLong(4, m_now);
        boundStatement.setConsistencyLevel(m_consistencyLevel);
        boundStatement.setIdempotent(true);

        addBoundStatement(boundStatement);
    }

    public void submitBatch() {
        if (metricNamesBatch.size() != 0) {
            m_session.execute(metricNamesBatch);
            m_periodicMetrics.recordGauge(
                    "datastore/cassandra/string_index/write_batch_size",
                    metricNamesBatch.size());
        }

        if (rowKeyBatch.size() != 0) {
            m_session.execute(rowKeyBatch);
            m_periodicMetrics.recordGauge(
                    "datastore/cassandra/row_keys/write_batch_size",
                    rowKeyBatch.size());
        }

        for (final BatchStatement batchStatement : m_batchMap.values()) {
            if (batchStatement.size() != 0) {
                m_session.execute(batchStatement);
                m_periodicMetrics.recordGauge(
                        "datastore/cassandra/data_points/write_batch_size",
                        batchStatement.size());
            }
        }

        //Catch all in case of a load balancing problem
        if (dataPointBatch.size() != 0) {
            m_session.execute(dataPointBatch);
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
