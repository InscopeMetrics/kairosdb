package org.kairosdb.rollup;

import com.arpnetworking.metrics.MetricsFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.Order;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.http.rest.json.RelativeTime;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.core.scheduler.KairosDBSchedulerImpl;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.plugin.Aggregator;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RollUpJob implements InterruptableJob {
    private static final Logger log = LoggerFactory.getLogger(KairosDBSchedulerImpl.class);

    private static final String ROLLUP_TIME = "kairosdb.rollup.execution-time";

    private static final int TOO_OLD_MULTIPLIER = 4;
    private boolean interrupted;

    public RollUpJob() {
    }

    /**
     * Returns the last data point the rollup created
     */
    static DataPoint getLastRollupDataPoint(final KairosDatastore datastore, final String rollupName, final long now) throws DatastoreException {
        final QueryMetric rollupQuery = new QueryMetric(0, now, 0, rollupName);
        rollupQuery.setLimit(1);
        rollupQuery.setOrder(Order.DESC);

        return performQuery(datastore, rollupQuery);
    }

    /**
     * Returns the next data point for the metric given a starting data point
     */
    static DataPoint getFutureDataPoint(final KairosDatastore datastore, final String metricName, final long now, final DataPoint startPoint) throws DatastoreException {
        final QueryMetric rollupQuery = new QueryMetric(startPoint.getTimestamp() + 1, now, 0, metricName);
        rollupQuery.setLimit(1);
        rollupQuery.setOrder(Order.ASC);

        return performQuery(datastore, rollupQuery);
    }

    private static DataPoint performQuery(final KairosDatastore datastore, final QueryMetric rollupQuery) throws DatastoreException {
        DatastoreQuery query = null;
        try {
            query = datastore.createQuery(rollupQuery);
            final List<DataPointGroup> rollupResult = query.execute();

            DataPoint dataPoint = null;
            for (final DataPointGroup dataPointGroup : rollupResult) {
                while (dataPointGroup.hasNext()) {
                    dataPoint = dataPointGroup.next();
                }
            }

            return dataPoint;
        } finally {
            if (query != null)
                query.close();
        }
    }

    /**
     * Returns the time stamp of the specified data point. If the data point is
     * null then it returns the start time for one sampling period before now.
     */
    static long calculateStartTime(final DataPoint dataPoint, final Sampling lastSampling, final long now) {
        checkNotNull(lastSampling, "At least one aggregators in the query must be a RangeAggregator.");

        if (dataPoint == null) {
            // go back one unit of time
            final RelativeTime samplingTime = new RelativeTime((int) lastSampling.getValue(), lastSampling.getUnit());
            return samplingTime.getTimeRelativeTo(now);
        } else {
            return dataPoint.getTimestamp();
        }
    }

    /**
     * Returns now if the data point is null. If the data point is not null
     * and its time stamp is too old, return a time that is 4 intervals from
     * the data point time.
     */
    static long calculateEndTime(final DataPoint datapoint, final Duration executionInterval, final long now) {
        long endTime = now;

        final RelativeTime relativeTime = new RelativeTime((int) (TOO_OLD_MULTIPLIER * executionInterval.getValue()), executionInterval.getUnit());
        if (datapoint != null && datapoint.getTimestamp() < relativeTime.getTimeRelativeTo(now)) {
            // last time was too old. Only do part of the rollup
            endTime = relativeTime.getFutureTimeRelativeTo(datapoint.getTimestamp());
        }
        return endTime;
    }

    /**
     * Returns the sampling from the last RangeAggregator in the aggregators list
     * or null if no sampling is found
     */
    static Sampling getLastSampling(final List<Aggregator> aggregators) {
        for (int i = aggregators.size() - 1; i >= 0; i--) {
            final Aggregator aggregator = aggregators.get(i);
            if (aggregator instanceof RangeAggregator) {
                return ((RangeAggregator) aggregator).getSampling();
            }
        }
        return null;
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public void execute(final JobExecutionContext jobExecutionContext) throws JobExecutionException {
        try {
            final JobDataMap dataMap = jobExecutionContext.getMergedJobDataMap();
            final RollupTask task = (RollupTask) dataMap.get("task");
            final FilterEventBus eventBus = (FilterEventBus) dataMap.get("eventBus");
            final KairosDatastore datastore = (KairosDatastore) dataMap.get("datastore");
            final MetricsFactory metricsFactory = (MetricsFactory) dataMap.get("metricsFactory");
            final String hostName = (String) dataMap.get("hostName");
            final RollupTaskStatusStore statusStore = (RollupTaskStatusStore) dataMap.get("statusStore");
            checkState(task != null, "Task was null");
            checkState(eventBus != null, "EventBus was null");
            checkState(datastore != null, "Datastore was null");
            checkState(hostName != null, "hostname was null");
            checkState(statusStore != null, "statusStore was null");

            for (final Rollup rollup : task.getRollups()) {
                log.info("Executing Rollup Task: " + task.getName() + " for Rollup  " + rollup.getSaveAs());

                if (interrupted)
                    break;

                final RollupTaskStatus status = new RollupTaskStatus(jobExecutionContext.getNextFireTime(), hostName);
                for (final QueryMetric queryMetric : rollup.getQueryMetrics()) {
                    ThreadReporter.initialize(metricsFactory);
                    ThreadReporter.addTag("rollup", rollup.getSaveAs());
                    ThreadReporter.addTag("rollup-task", task.getName());
                    boolean success = true;
                    final long startQueryTime = System.currentTimeMillis();
                    try {
                        if (interrupted)
                            break;

                        final DataPoint rollupDataPoint = getLastRollupDataPoint(datastore, rollup.getSaveAs(), startQueryTime);
                        queryMetric.setStartTime(calculateStartTime(rollupDataPoint, getLastSampling(queryMetric.getAggregators()), startQueryTime));
                        queryMetric.setEndTime(calculateEndTime(rollupDataPoint, task.getExecutionInterval(), startQueryTime));
                        long executionStartTime = System.currentTimeMillis();
                        long dpCount = executeRollup(datastore, queryMetric);
                        long executionLength = System.currentTimeMillis() - executionStartTime;
                        log.info("Rollup Task: " + task.getName() + " for Rollup " + rollup.getSaveAs() + " data point count of " + dpCount);

                        if (dpCount == 0 && rollupDataPoint != null) {
                            // Advance forward if a data point exists for the query metric
                            final DataPoint dataPoint = getFutureDataPoint(datastore, queryMetric.getName(), startQueryTime, rollupDataPoint);
                            queryMetric.setStartTime(calculateStartTime(dataPoint, getLastSampling(queryMetric.getAggregators()), startQueryTime));
                            queryMetric.setEndTime(calculateEndTime(dataPoint, task.getExecutionInterval(), startQueryTime));
                            executionStartTime = System.currentTimeMillis();
                            dpCount = executeRollup(datastore, queryMetric);
                            executionLength = System.currentTimeMillis() - executionStartTime;
                            log.info("Datapoint exists for time range, advancing forward for Rollup Task: " + task.getName() + " for Rollup " + rollup.getSaveAs() + " data point count of " + dpCount);
                        }

                        status.addStatus(RollupTaskStatus.createQueryMetricStatus(queryMetric.getName(), System.currentTimeMillis(), dpCount, executionLength));
                    } catch (final DatastoreException e) {
                        success = false;
                        log.error("Failed to execute query for roll-up task: " + task.getName() + " roll-up: " + rollup.getSaveAs(), e);
                        status.addStatus(RollupTaskStatus.createErrorQueryMetricStatus(queryMetric.getName(), System.currentTimeMillis(), ExceptionUtils.getStackTrace(e), 0));
                    } catch (final Exception e) {
                        success = false;
                        log.error("Failed to roll-up task: " + task.getName() + " roll-up: " + rollup.getSaveAs(), e);
                        status.addStatus(RollupTaskStatus.createErrorQueryMetricStatus(queryMetric.getName(), System.currentTimeMillis(), ExceptionUtils.getStackTrace(e), 0));
                    } finally {
                        ThreadReporter.addTag("status", success ? "success" : "failure");
                        ThreadReporter.addDataPoint(ROLLUP_TIME, System.currentTimeMillis() - startQueryTime);
                        ThreadReporter.close();

                        try {
                            statusStore.write(task.getId(), status);
                        } catch (final RollUpException e) {
                            log.error("Could not write status to status store", e);
                        }
                    }
                }
            }
        } catch (final Throwable t) {
            log.error("Failed to execute job " + jobExecutionContext.toString(), t);
        }
    }

    private long executeRollup(final KairosDatastore datastore, final QueryMetric query) throws DatastoreException {
        log.info("Execute Rollup: Start time: " + new Date(query.getStartTime()) + " End time: " + new Date(query.getEndTime()));

        int dpCount = 0;
        final DatastoreQuery dq = datastore.createQuery(query);
        try {
            final List<DataPointGroup> result = dq.execute();

            for (final DataPointGroup dataPointGroup : result) {
                while (dataPointGroup.hasNext()) {
                    dataPointGroup.next();
                    dpCount++;
                }
            }
        } finally {
            if (dq != null)
                dq.close();
        }

        return dpCount;
    }

    @Override
    public void interrupt() {
        interrupted = true;
    }
}
