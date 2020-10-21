package org.kairosdb.rollup;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.google.common.collect.ImmutableSortedMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.TestDataPointFactory;
import org.kairosdb.core.aggregator.DiffAggregator;
import org.kairosdb.core.aggregator.DivideAggregator;
import org.kairosdb.core.aggregator.MaxAggregator;
import org.kairosdb.core.aggregator.MinAggregator;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.eventbus.Subscribe;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.testing.ListDataPointGroup;
import org.mockito.Mockito;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class RollUpJobTest {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss.SS", Locale.ENGLISH);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private long lastTimeStamp;
    private KairosDatastore datastore;
    private TestDatastore testDataStore;

    @Before
    public void setup() throws ParseException, DatastoreException {
        lastTimeStamp = dateFormat.parse("2013-JAN-18 4:55:12.22").getTime();

        final PeriodicMetrics periodicMetrics = Mockito.mock(PeriodicMetrics.class);
        testDataStore = new TestDatastore();
        datastore = new KairosDatastore(testDataStore, new QueryQueuingManager(periodicMetrics, 1),
                new TestDataPointFactory(), false);
    datastore.init();
	}

    @Test
    public void test_getLastSampling() {
        final Sampling sampling1 = new Sampling(1, TimeUnit.DAYS);
        final Sampling sampling2 = new Sampling(2, TimeUnit.MINUTES);

        final DoubleDataPointFactory dataPointFactory = mock(DoubleDataPointFactory.class);

        final MinAggregator minAggregator = new MinAggregator(dataPointFactory);
        minAggregator.setSampling(sampling1);
        final MaxAggregator maxAggregator = new MaxAggregator(dataPointFactory);
        maxAggregator.setSampling(sampling2);

        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(minAggregator);
        aggregators.add(maxAggregator);
        aggregators.add(new DivideAggregator(dataPointFactory));
        aggregators.add(new DiffAggregator(dataPointFactory));

        Sampling lastSampling = RollUpJob.getLastSampling(aggregators);

        assertThat(lastSampling, equalTo(sampling2));

        aggregators = new ArrayList<>();
        aggregators.add(maxAggregator);
        aggregators.add(new DivideAggregator(dataPointFactory));
        aggregators.add(new DiffAggregator(dataPointFactory));
        aggregators.add(minAggregator);

        lastSampling = RollUpJob.getLastSampling(aggregators);

        assertThat(lastSampling, equalTo(sampling1));
    }

    @Test
    public void test_getLastSampling_no_sampling() {
        final DoubleDataPointFactory dataPointFactory = mock(DoubleDataPointFactory.class);
        final List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new DivideAggregator(dataPointFactory));
        aggregators.add(new DiffAggregator(dataPointFactory));

        final Sampling lastSampling = RollUpJob.getLastSampling(aggregators);

        assertThat(lastSampling, equalTo(null));
    }

    @Test
    public void test_getLastRollupDataPoint() throws ParseException, DatastoreException {
        final long now = dateFormat.parse("2013-Jan-18 4:59:12.22").getTime();
        final String metricName = "foo";

        final ImmutableSortedMap<String, String> localHostTags = ImmutableSortedMap.of("host", "localhost");
        final List<DataPoint> localhostDataPoints = new ArrayList<>();
        localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 1, 10));
        localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 2, 11));
        localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 3, 12));
        localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 4, 13));
        localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 5, 14));

        final ImmutableSortedMap<String, String> remoteTags = ImmutableSortedMap.of("host", "remote");
        final List<DataPoint> remoteDataPoints = new ArrayList<>();
        remoteDataPoints.add(new DoubleDataPoint(lastTimeStamp + 1, 10));
        remoteDataPoints.add(new DoubleDataPoint(lastTimeStamp + 2, 11));

        testDataStore.clear();
        testDataStore.putDataPoints(metricName, localHostTags, localhostDataPoints);
        testDataStore.putDataPoints(metricName, remoteTags, remoteDataPoints);

        final DataPoint lastDataPoint = RollUpJob.getLastRollupDataPoint(datastore, metricName, now);

        // Look back from now and find last data point [4]
        assertThat(lastDataPoint, equalTo(localhostDataPoints.get(4)));
    }

    @Test
    public void test_getLastRollupDataPoint_noDataPoints() throws ParseException, DatastoreException {
        final long now = dateFormat.parse("2013-Jan-18 4:59:12.22").getTime();
        final String metricName = "foo";

        testDataStore.clear();

        final DataPoint lastDataPoint = RollUpJob.getLastRollupDataPoint(datastore, metricName, now);

        assertThat(lastDataPoint, equalTo(null));
    }

    @Test
    public void test_getgetFutureDataPoint() throws ParseException, DatastoreException {
        final long now = dateFormat.parse("2013-Jan-18 4:59:12.22").getTime();
        final String metricName = "foo";

        final ImmutableSortedMap<String, String> localHostTags = ImmutableSortedMap.of("host", "localhost");
        final List<DataPoint> localhostDataPoints = new ArrayList<>();
        localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 1, 10));
        localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 2, 11));
        localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 3, 12));
        localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 4, 13));
        localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 5, 14));

        final ImmutableSortedMap<String, String> remoteTags = ImmutableSortedMap.of("host", "remote");
        final List<DataPoint> remoteDataPoints = new ArrayList<>();
        remoteDataPoints.add(new DoubleDataPoint(lastTimeStamp + 1, 10));
        remoteDataPoints.add(new DoubleDataPoint(lastTimeStamp + 2, 11));

        testDataStore.clear();
        testDataStore.putDataPoints(metricName, localHostTags, localhostDataPoints);
        testDataStore.putDataPoints(metricName, remoteTags, remoteDataPoints);

        // Look from data point [1] forward and return [2]
        final DataPoint futureDataPoint = RollUpJob.getFutureDataPoint(datastore, metricName, now, localhostDataPoints.get(1));

        assertThat(futureDataPoint, equalTo(localhostDataPoints.get(2)));
    }

    @Test
    public void test_calculatStartTime_datapointTime() {
        final Sampling sampling = new Sampling();
        final DoubleDataPoint dataPoint = new DoubleDataPoint(123456L, 10);

        final long time = RollUpJob.calculateStartTime(dataPoint, sampling, System.currentTimeMillis());

        assertThat(time, equalTo(123456L));
    }

    @Test
    public void test_calculatStartTime_samplingTime() throws ParseException {
        final long now = dateFormat.parse("2013-Jan-18 4:59:12.22").getTime();
        final Sampling sampling = new Sampling(1, TimeUnit.HOURS);

        final long time = RollUpJob.calculateStartTime(null, sampling, now);

        assertThat(time, equalTo(dateFormat.parse("2013-Jan-18 3:59:12.22").getTime()));
    }

    @Test(expected = NullPointerException.class)
    public void test_calculatStartTime_samplingNull_invalid() {
        final DoubleDataPoint dataPoint = new DoubleDataPoint(123456L, 10);

        RollUpJob.calculateStartTime(dataPoint, null, System.currentTimeMillis());
    }

    @Test
    public void test_calculatEndTime_datapoint_null() {
        final long now = System.currentTimeMillis();
        final Duration executionInterval = new Duration();

        final long time = RollUpJob.calculateEndTime(null, executionInterval, now);

        assertThat(time, equalTo(now));
    }

    @Test
    public void test_calculatEndTime_datapointNotNull_recentTime() {
        final long now = System.currentTimeMillis();
        final Duration executionInterval = new Duration();
        final DoubleDataPoint dataPoint = new DoubleDataPoint(now - 2000, 10);

        final long time = RollUpJob.calculateEndTime(dataPoint, executionInterval, now);

        assertThat(time, equalTo(dataPoint.getTimestamp()));
    }

    @Test
    public void test_calculatEndTime_datapointNotNull_tooOld() throws ParseException {
        final long datapointTime = dateFormat.parse("2013-Jan-18 4:59:12.22").getTime();
        final long now = System.currentTimeMillis();
        final Duration executionInterval = new Duration(1, TimeUnit.DAYS);
        final DoubleDataPoint dataPoint = new DoubleDataPoint(datapointTime, 10);

        final long time = RollUpJob.calculateEndTime(dataPoint, executionInterval, now);

        assertThat(time, equalTo(dateFormat.parse("2013-Jan-22 4:59:12.22").getTime()));
    }

    public static class TestDatastore implements Datastore {
        List<ListDataPointGroup> dataPointGroups = new ArrayList<>();

         void clear() {
            dataPointGroups = new ArrayList<>();
        }

		@Override
		public void close() {
		}

		void putDataPoints(final String metricName, final ImmutableSortedMap<String, String> tags, final List<DataPoint> dataPoints) {
			final ListDataPointGroup dataPointGroup = new ListDataPointGroup(metricName);

            for (final Map.Entry<String, String> tag : tags.entrySet()) {
                dataPointGroup.addTag(tag.getKey(), tag.getValue());
            }

            for (final DataPoint dataPoint : dataPoints) {
                dataPointGroup.addDataPoint(dataPoint);
            }

            dataPointGroups.add(dataPointGroup);
        }

        @Subscribe
        public void putDataPoint(final String metricName, final ImmutableSortedMap<String, String> tags, final DataPoint dataPoint, final int ttl) {
            final ListDataPointGroup dataPointGroup = new ListDataPointGroup(metricName);
            dataPointGroup.addDataPoint(dataPoint);

            for (final Map.Entry<String, String> tag : tags.entrySet()) {
                dataPointGroup.addTag(tag.getKey(), tag.getValue());
            }

            dataPointGroups.add(dataPointGroup);
        }

        @Override
        public Iterable<String> getMetricNames(final String prefix)  {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<String> getTagNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<String> getTagValues() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void queryDatabase(final DatastoreMetricQuery query, final QueryCallback queryCallback) throws DatastoreException {
            for (final ListDataPointGroup dataPointGroup : dataPointGroups) {
                try {
                    dataPointGroup.sort(query.getOrder());

                    final SortedMap<String, String> tags = new TreeMap<>();
                    for (final String tagName : dataPointGroup.getTagNames()) {
                        tags.put(tagName, dataPointGroup.getTagValues(tagName).iterator().next());
                    }

                    final DataPoint dataPoint = getNext(dataPointGroup, query);
                    if (dataPoint != null) {
                        try (final QueryCallback.DataPointWriter dataPointWriter = queryCallback.startDataPointSet(dataPoint.getDataStoreDataType(), tags)) {
                            dataPointWriter.addDataPoint(dataPoint);

                            while (dataPointGroup.hasNext()) {
                                final DataPoint next = getNext(dataPointGroup, query);
                                if (next != null) {
                                    dataPointWriter.addDataPoint(next);
                                }
                            }
                        }
                    }
                } catch (final IOException e) {
                    throw new DatastoreException(e);
                }
            }
        }

        private DataPoint getNext(final DataPointGroup group, final DatastoreMetricQuery query) {
            DataPoint dataPoint = null;
            while (group.hasNext()) {
                final DataPoint dp = group.next();
                if (dp.getTimestamp() >= query.getStartTime()) {
                    dataPoint = dp;
                    break;
                }
            }

            return dataPoint;
        }

        @Override
        public void deleteDataPoints(final DatastoreMetricQuery deleteQuery)  {
            throw new UnsupportedOperationException();
        }

        @Override
        public TagSet queryMetricTags(final DatastoreMetricQuery query)  {
            throw new UnsupportedOperationException();
        }

        @Override
        public long queryCardinality(final DatastoreMetricQuery query) {
            throw new UnsupportedOperationException();
        }
    }
}
