package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.util.RetryCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.calculateRowTime;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.getColumnName;

/**
 * Created by bhawkins on 1/11/17.
 */
public class DeleteBatchHandler extends RetryCallable {
    public static final Logger logger = LoggerFactory.getLogger(BatchHandler.class);
    public static final Logger failedLogger = LoggerFactory.getLogger("failed_logger");

    private final List<DataPoint> m_events;
    private final EventCompletionCallBack m_callBack;
    private final CassandraModule.CQLBatchFactory m_cqlBatchFactory;
    private final String m_metricName;
    private final SortedMap<String, String> m_tags;

    @Inject
    public DeleteBatchHandler(
            @Assisted final String metricName,
            @Assisted final SortedMap<String, String> tags,
            @Assisted final List<DataPoint> events,
            @Assisted final EventCompletionCallBack callBack,
            final CassandraModule.CQLBatchFactory CQLBatchFactory) {
        m_metricName = metricName;
        m_tags = tags;
        m_events = events;
        m_callBack = callBack;

        m_cqlBatchFactory = CQLBatchFactory;
    }

    private void loadBatch(final int limit, final CQLBatch batch, final Iterator<DataPoint> events) throws Exception {
        int count = 0;
        while (events.hasNext() && count < limit) {
            final DataPoint dataPoint = events.next();
            count++;

            final long rowTime = calculateRowTime(dataPoint.getTimestamp());

            final DataPointsRowKey rowKey = new DataPointsRowKey(m_metricName, rowTime, dataPoint.getDataStoreDataType(),
                    m_tags);


            final int columnTime = getColumnName(rowTime, dataPoint.getTimestamp());

            batch.deleteDataPoint(rowKey, columnTime);
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
                final Iterator<DataPoint> events = m_events.iterator();

                while (events.hasNext()) {
                    final CQLBatch batch = m_cqlBatchFactory.create();

                    loadBatch(limit, batch, events);

                    batch.submitBatch();

                }

            }
            //If More exceptions are added to retry they need to be added to AdaptiveExecutorService
            catch (final NoHostAvailableException | UnavailableException nae) {
                //Throw this out so the back off retry can happen
                logger.error(nae.getMessage());
                throw nae;
            } catch (final Exception e) {
                if ("Batch too large".equals(e.getMessage()))
                    logger.warn("Batch size is too large");
                else
                    logger.error("Error sending data points", e);

                if (limit > 10) {
                    retry = true;
                    logger.info("Retrying batch with smaller limit");
                } else {
                    logger.error("Failed to delete data points", e);
                    if (failedLogger.isTraceEnabled()) {
                        for (final DataPoint event : m_events) {
                            final StringWriter sw = new StringWriter();
                            final JSONWriter jsonWriter = new JSONWriter(sw);
                            jsonWriter.object();
                            jsonWriter.key("name").value(m_metricName);
                            jsonWriter.key("timestamp").value(event.getTimestamp());

                            jsonWriter.key("tags").object();
                            for (final Map.Entry<String, String> entry : m_tags.entrySet()) {
                                jsonWriter.key(entry.getKey()).value(entry.getValue());
                            }
                            jsonWriter.endObject();
                            jsonWriter.endObject();

                            failedLogger.trace(sw.toString());
                        }
                    }
                }
            }
            divisor++;

        } while (retry);

        m_callBack.complete();
    }
}
