package org.kairosdb.core.aggregator;

import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.ListDataPointGroup;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Created by bhawkins on 8/28/15.
 */
public class TrimAggregatorTest {
    @Test
    public void test_oneDataPointWithTrimFirst() {
        final ListDataPointGroup group = new ListDataPointGroup("trim_test");
        group.addDataPoint(new LongDataPoint(1, 10));

        final TrimAggregator trimAggregator = new TrimAggregator();
        trimAggregator.setTrim(TrimAggregator.Trim.FIRST);
        final DataPointGroup results = trimAggregator.aggregate(group);

        assertThat(results.hasNext(), equalTo(false));
    }

    @Test
    public void test_twoDataPointsWithTrimFirst() {
        final ListDataPointGroup group = new ListDataPointGroup("trim_test");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(2, 20));

        final TrimAggregator trimAggregator = new TrimAggregator();
        trimAggregator.setTrim(TrimAggregator.Trim.FIRST);
        final DataPointGroup results = trimAggregator.aggregate(group);

        assertThat(results.hasNext(), equalTo(true));
        final DataPoint dp = results.next();

        assertThat(dp.getTimestamp(), equalTo(2L));
        assertThat(dp.getLongValue(), equalTo(20L));

        assertThat(results.hasNext(), equalTo(false));
    }

    @Test
    public void test_threeDataPointWithTrimFirst() {
        final ListDataPointGroup group = new ListDataPointGroup("trim_test");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(2, 20));
        group.addDataPoint(new LongDataPoint(3, 30));

        final TrimAggregator trimAggregator = new TrimAggregator();
        trimAggregator.setTrim(TrimAggregator.Trim.FIRST);
        final DataPointGroup results = trimAggregator.aggregate(group);

        assertThat(results.hasNext(), equalTo(true));

        DataPoint dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(2L));
        assertThat(dp.getLongValue(), equalTo(20L));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(3L));
        assertThat(dp.getLongValue(), equalTo(30L));

        assertThat(results.hasNext(), equalTo(false));
    }

    @Test
    public void test_oneDataPointWithTrimLast() {
        final ListDataPointGroup group = new ListDataPointGroup("trim_test");
        group.addDataPoint(new LongDataPoint(1, 10));

        final TrimAggregator trimAggregator = new TrimAggregator();
        trimAggregator.setTrim(TrimAggregator.Trim.LAST);
        final DataPointGroup results = trimAggregator.aggregate(group);

        assertThat(results.hasNext(), equalTo(false));
    }

    @Test
    public void test_twoDataPointsWithTrimLast() {
        final ListDataPointGroup group = new ListDataPointGroup("trim_test");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(2, 20));

        final TrimAggregator trimAggregator = new TrimAggregator();
        trimAggregator.setTrim(TrimAggregator.Trim.LAST);
        final DataPointGroup results = trimAggregator.aggregate(group);

        assertThat(results.hasNext(), equalTo(true));
        final DataPoint dp = results.next();

        assertThat(dp.getTimestamp(), equalTo(1L));
        assertThat(dp.getLongValue(), equalTo(10L));

        assertThat(results.hasNext(), equalTo(false));
    }

    @Test
    public void test_threeDataPointWithTrimLast() {
        final ListDataPointGroup group = new ListDataPointGroup("trim_test");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(2, 20));
        group.addDataPoint(new LongDataPoint(3, 30));

        final TrimAggregator trimAggregator = new TrimAggregator();
        trimAggregator.setTrim(TrimAggregator.Trim.LAST);
        final DataPointGroup results = trimAggregator.aggregate(group);

        assertThat(results.hasNext(), equalTo(true));

        DataPoint dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(1L));
        assertThat(dp.getLongValue(), equalTo(10L));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(2L));
        assertThat(dp.getLongValue(), equalTo(20L));

        assertThat(results.hasNext(), equalTo(false));
    }

    @Test
    public void test_oneDataPointWithTrimBoth() {
        final ListDataPointGroup group = new ListDataPointGroup("trim_test");
        group.addDataPoint(new LongDataPoint(1, 10));

        final TrimAggregator trimAggregator = new TrimAggregator();
        trimAggregator.setTrim(TrimAggregator.Trim.BOTH);
        final DataPointGroup results = trimAggregator.aggregate(group);

        assertThat(results.hasNext(), equalTo(false));
    }

    @Test
    public void test_twoDataPointsWithTrimBoth() {
        final ListDataPointGroup group = new ListDataPointGroup("trim_test");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(2, 20));

        final TrimAggregator trimAggregator = new TrimAggregator();
        trimAggregator.setTrim(TrimAggregator.Trim.BOTH);
        final DataPointGroup results = trimAggregator.aggregate(group);

        assertThat(results.hasNext(), equalTo(false));
    }

    @Test
    public void test_threeDataPointWithTrimBoth() {
        final ListDataPointGroup group = new ListDataPointGroup("trim_test");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(2, 20));
        group.addDataPoint(new LongDataPoint(3, 30));

        final TrimAggregator trimAggregator = new TrimAggregator();
        trimAggregator.setTrim(TrimAggregator.Trim.BOTH);
        final DataPointGroup results = trimAggregator.aggregate(group);

        assertThat(results.hasNext(), equalTo(true));

        final DataPoint dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(2L));
        assertThat(dp.getLongValue(), equalTo(20L));

        assertThat(results.hasNext(), equalTo(false));
    }
}
