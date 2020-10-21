package org.kairosdb.core.aggregator;

import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.ListDataPointGroup;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Created by bhawkins on 1/8/15.
 */
public class DiffAggregatorTest {
    @Test(expected = NullPointerException.class)
    public void test_nullSet_invalid() {
        new DiffAggregator(new DoubleDataPointFactoryImpl()).aggregate(null);
    }

    @Test
    public void test_steadyRate() {
        final ListDataPointGroup group = new ListDataPointGroup("rate");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(2, 20));
        group.addDataPoint(new LongDataPoint(3, 30));
        group.addDataPoint(new LongDataPoint(4, 40));

        final DiffAggregator DiffAggregator = new DiffAggregator(new DoubleDataPointFactoryImpl());
        final DataPointGroup results = DiffAggregator.aggregate(group);

        DataPoint dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(2L));
        assertThat(dp.getDoubleValue(), equalTo(10.0));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(3L));
        assertThat(dp.getDoubleValue(), equalTo(10.0));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(4L));
        assertThat(dp.getDoubleValue(), equalTo(10.0));

        assertThat(results.hasNext(), equalTo(false));
    }

    @Test
    public void test_changingRate() {
        final ListDataPointGroup group = new ListDataPointGroup("rate");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(2, 10));
        group.addDataPoint(new LongDataPoint(3, 5));
        group.addDataPoint(new LongDataPoint(4, 20));

        final DiffAggregator DiffAggregator = new DiffAggregator(new DoubleDataPointFactoryImpl());
        final DataPointGroup results = DiffAggregator.aggregate(group);

        DataPoint dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(2L));
        assertThat(dp.getDoubleValue(), equalTo(0.0));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(3L));
        assertThat(dp.getDoubleValue(), equalTo(-5.0));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(4L));
        assertThat(dp.getDoubleValue(), equalTo(15.0));

        assertThat(results.hasNext(), equalTo(false));
    }


    @Test
    public void test_steadyRateOver2Sec() {
        final ListDataPointGroup group = new ListDataPointGroup("rate");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(3, 20));
        group.addDataPoint(new LongDataPoint(5, 30));
        group.addDataPoint(new LongDataPoint(7, 40));

        final DiffAggregator rateAggregator = new DiffAggregator(new DoubleDataPointFactoryImpl());
        final DataPointGroup results = rateAggregator.aggregate(group);

        DataPoint dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(3L));
        assertThat(dp.getDoubleValue(), equalTo(10.0));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(5L));
        assertThat(dp.getDoubleValue(), equalTo(10.0));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(7L));
        assertThat(dp.getDoubleValue(), equalTo(10.0));

        assertThat(results.hasNext(), equalTo(false));
    }


    @Test
    public void test_dataPointsAtSameTime() {
        final ListDataPointGroup group = new ListDataPointGroup("rate");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(1, 15));
        group.addDataPoint(new LongDataPoint(2, 5));
        group.addDataPoint(new LongDataPoint(2, 20));
        group.addDataPoint(new LongDataPoint(3, 30));


        final DiffAggregator DiffAggregator = new DiffAggregator(new DoubleDataPointFactoryImpl());
        final DataPointGroup results = DiffAggregator.aggregate(group);

        assertThat(results.hasNext(), equalTo(true));
        DataPoint dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(1L));
        assertThat(dp.getDoubleValue(), equalTo(5.0));

        assertThat(results.hasNext(), equalTo(true));
        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(2L));
        assertThat(dp.getDoubleValue(), equalTo(-10.0));

        assertThat(results.hasNext(), equalTo(true));
        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(2L));
        assertThat(dp.getDoubleValue(), equalTo(15.0));

        assertThat(results.hasNext(), equalTo(true));
        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(3L));
        assertThat(dp.getDoubleValue(), equalTo(10.0));

        assertThat(results.hasNext(), equalTo(false));
    }
}
