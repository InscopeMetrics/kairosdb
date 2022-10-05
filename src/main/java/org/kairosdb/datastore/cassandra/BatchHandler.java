package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.assistedinject.Assisted;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.BatchReductionEvent;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.events.RowKeyEvent;
import org.kairosdb.util.RetryCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.ROW_WIDTH;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.calculateRowTime;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.getColumnName;

/**
 * Created by bhawkins on 1/11/17.
 */
public class BatchHandler extends RetryCallable {
    public static final Logger logger = LoggerFactory.getLogger(BatchHandler.class);
    public static final Logger failedLogger = LoggerFactory.getLogger("failed_logger");

    private final List<DataPointEvent> m_events;
    private final EventCompletionCallBack m_callBack;
    private final int m_defaultTtl;
    private final boolean m_allignDatapointTtl;
    private final boolean m_forceDefaultDatapointTtl;
    private final DataCache<DataPointsRowKey> m_rowKeyCache;
    private final DataCache<String> m_metricNameCache;
    private final CassandraModule.CQLBatchFactory m_cqlBatchFactory;
    private final Publisher<RowKeyEvent> m_rowKeyPublisher;
    private final Publisher<BatchReductionEvent> m_batchReductionPublisher;
    private final String m_clusterName;

    @Inject
    public BatchHandler(
            @Assisted final List<DataPointEvent> events,
            @Assisted final EventCompletionCallBack callBack,
            final CassandraConfiguration configuration,
            final DataCache<DataPointsRowKey> rowKeyCache,
            final DataCache<String> metricNameCache,
            final FilterEventBus eventBus,
            final CassandraModule.CQLBatchFactory cqlBatchFactory) {
        m_events = events;
        m_callBack = callBack;
        m_defaultTtl = configuration.getDatapointTtl();
        m_clusterName = configuration.getWriteCluster().getClusterName();
        m_allignDatapointTtl = configuration.isAlignDatapointTtlWithTimestamp();
        m_forceDefaultDatapointTtl = configuration.isForceDefaultDatapointTtl();
        m_rowKeyCache = rowKeyCache;
        m_metricNameCache = metricNameCache;

        m_cqlBatchFactory = cqlBatchFactory;

        m_rowKeyPublisher = eventBus.createPublisher(RowKeyEvent.class);
        m_batchReductionPublisher = eventBus.createPublisher(BatchReductionEvent.class);
    }


    private void loadBatch(final int limit, final CQLBatch batch, final Iterator<DataPointEvent> events) throws Exception {
        int count = 0;
        while (events.hasNext() && count < limit) {
            final DataPointEvent event = events.next();
            count++;

            final String metricName = event.getMetricName();

            final ImmutableSortedMap<String, String> tags = event.getTags();
            final DataPoint dataPoint = event.getDataPoint();

            // force default ttl if property is set, use event's ttl otherwise
            int ttl = m_forceDefaultDatapointTtl ? m_defaultTtl : event.getTtl();
            logger.trace("ttl (seconds): {}", ttl);

            DataPointsRowKey rowKey = null;
            //time the data is written.
            final long writeTime = System.currentTimeMillis();

            // set ttl to default if the event's ttl is not present or 0 (which actually can be 0 as well)
            if (0 == ttl)
                ttl = m_defaultTtl;

            // check if datapoint ttl alignment should be used
            if (m_allignDatapointTtl) {
                // determine the datapoint's "age" comparing it's timestamp and now
                final int datapointAgeInSeconds = (int) ((writeTime - dataPoint.getTimestamp()) / 1000);
                logger.trace("datapointAgeInSeconds: {}", datapointAgeInSeconds);

                // the resulting aligned ttl is the former calculated ttl minus the datapoint's age
                ttl = ttl - datapointAgeInSeconds;
                logger.trace("aligned ttl (seconds): {}", ttl);
                // if the aligned ttl is negative, the datapoint is already dead
                if (ttl <= 0) {
                    logger.warn("aligned ttl for {} with tags {} is negative, so the datapoint is already dead, no need to store it", metricName, tags);
                    continue;
                }
            }

            //Row key will expire 3 weeks after the data in the row expires
            final int rowKeyTtl = (ttl == 0) ? 0 : ttl + ((int) (ROW_WIDTH / 1000));

            final long rowTime = calculateRowTime(dataPoint.getTimestamp());

            rowKey = new DataPointsRowKey(metricName, m_clusterName, rowTime, dataPoint.getDataStoreDataType(),
                    tags);

            //Write out the row key if it is not cached
            final DataPointsRowKey cachedKey = m_rowKeyCache.get(rowKey);
            if (cachedKey == null) {
                batch.addRowKey(metricName, rowKey, rowKeyTtl);

                m_rowKeyPublisher.post(new RowKeyEvent(metricName, rowKey, rowKeyTtl));
            } else
                rowKey = cachedKey;

            //Write metric name if not in cache
            final String cachedName = m_metricNameCache.get(metricName);
            if (cachedName == null) {
                if (metricName.length() == 0) {
                    logger.warn(
                            "Attempted to add empty metric name to string index. Row looks like: " + dataPoint
                    );
                }
                batch.addMetricName(metricName);
            }


            final int columnTime = getColumnName(rowTime, dataPoint.getTimestamp());

            batch.addDataPoint(rowKey, columnTime, dataPoint, ttl);
        }
    }

    @Override
    public void retryCall() throws Exception {
        int divisor = 1;
        boolean retry = false;
        int limit = Integer.MAX_VALUE;

        do {
            retry = false;

            //Used to reduce batch size with each retry
            limit = m_events.size() / divisor;
            try {
                final Iterator<DataPointEvent> events = m_events.iterator();

                while (events.hasNext()) {
                    final CQLBatch batch = m_cqlBatchFactory.create();

                    loadBatch(limit, batch, events);

                    batch.submitBatch();

                    // The writes went through, so populate the caches accordingly.
                    batch.getMetricNames().forEach(m_metricNameCache::put);
                    batch.getRowKeys().forEach(m_rowKeyCache::put);
                }

            }
            //If More exceptions are added to retry they need to be added to AdaptiveExecutorService
            catch (final NoHostAvailableException nae) {
                //Throw this out so the back off retry can happen
                logger.error(nae.getMessage());
                throw nae;
            } catch (final UnavailableException ue) {
                //Throw this out so the back off retry can happen
                logger.error(ue.getMessage());
                throw ue;
            } catch (final Exception e) {
                if ("Batch too large".equals(e.getMessage()))
                    logger.warn("Batch size is too large");
                else
                    logger.error("Error sending data points", e);

                if (limit > 10) {
                    retry = true;
                    logger.info("Retrying batch with smaller limit");
                } else {
                    logger.error("Failed to send data points", e);
                    if (failedLogger.isTraceEnabled()) {
                        for (final DataPointEvent event : m_events) {
                            final StringWriter sw = new StringWriter();
                            final JSONWriter jsonWriter = new JSONWriter(sw);
                            jsonWriter.object();
                            jsonWriter.key("name").value(event.getMetricName());
                            jsonWriter.key("timestamp").value(event.getDataPoint().getTimestamp());
                            jsonWriter.key("value");
                            event.getDataPoint().writeValueToJson(jsonWriter);

                            jsonWriter.key("tags").object();
                            final ImmutableSortedMap<String, String> tags = event.getTags();
                            for (final Map.Entry<String, String> entry : tags.entrySet()) {
                                jsonWriter.key(entry.getKey()).value(entry.getValue());
                            }
                            jsonWriter.endObject();

                            jsonWriter.key("ttl").value(event.getTtl());

                            jsonWriter.endObject();

                            failedLogger.trace(sw.toString());
                        }
                    }
                }
            }
            divisor++;

        } while (retry);

        if (limit < m_events.size()) {
            m_batchReductionPublisher.post(new BatchReductionEvent(limit));
        }

        m_callBack.complete();
    }
}
