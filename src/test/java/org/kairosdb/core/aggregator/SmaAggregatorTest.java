/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.core.aggregator;

import org.assertj.core.data.Offset;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.testing.ListDataPointGroup;

import static org.assertj.core.api.Assertions.assertThat;

public class SmaAggregatorTest {
    private SmaAggregator aggregator;

    @Before
    public void setup() throws KairosDBException {
        aggregator = new SmaAggregator(new DoubleDataPointFactoryImpl());
    }

    @Test(expected = NullPointerException.class)
    public void test_nullSet_invalid() {
        aggregator.setSize(3);
        aggregator.aggregate(null);
    }

    @Test
    public void test_longValues() {
        final ListDataPointGroup group = new ListDataPointGroup("group");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(1, 20));
        group.addDataPoint(new LongDataPoint(1, 3));
        group.addDataPoint(new LongDataPoint(2, 1));
        group.addDataPoint(new LongDataPoint(2, 5));
        group.addDataPoint(new LongDataPoint(3, 6));

        aggregator.setSize(3);
        final DataPointGroup results = aggregator.aggregate(group);

        DataPoint dataPoint = results.next();
        assertThat(dataPoint.getTimestamp()).isEqualTo(1L);
        assertThat(dataPoint.getLongValue()).isEqualTo(11L);

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp()).isEqualTo(2L);
        assertThat(dataPoint.getLongValue()).isEqualTo(8L);

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp()).isEqualTo(2L);
        assertThat(dataPoint.getLongValue()).isEqualTo(3L);

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp()).isEqualTo(3L);
        assertThat(dataPoint.getLongValue()).isEqualTo(4L);

        assertThat(results.hasNext()).isEqualTo(false);
    }

    @Test
    public void test_doubleValues() {
        final ListDataPointGroup group = new ListDataPointGroup("group");
        group.addDataPoint(new DoubleDataPoint(1, 10.0));
        group.addDataPoint(new DoubleDataPoint(1, 20.3));
        group.addDataPoint(new DoubleDataPoint(1, 3.0));
        group.addDataPoint(new DoubleDataPoint(2, 1.0));
        group.addDataPoint(new DoubleDataPoint(2, 5.2));
        group.addDataPoint(new DoubleDataPoint(3, 6.0));

        aggregator.setSize(3);
        final DataPointGroup results = aggregator.aggregate(group);

        DataPoint dataPoint = results.next();
        assertThat(dataPoint.getTimestamp()).isEqualTo(1L);
        assertThat(dataPoint.getDoubleValue()).isEqualTo(11.1);

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp()).isEqualTo(2L);
        assertThat(dataPoint.getDoubleValue()).isEqualTo(8.1);

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp()).isEqualTo(2L);
        assertThat(dataPoint.getDoubleValue()).isCloseTo(3.067, Offset.offset(2.0));

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp()).isEqualTo(3L);
        assertThat(dataPoint.getDoubleValue()).isCloseTo(4.067, Offset.offset(2.0));

        assertThat(results.hasNext()).isEqualTo(false);
    }

    @Test
    public void test_mixedTypeValues() {
        final ListDataPointGroup group = new ListDataPointGroup("group");
        group.addDataPoint(new DoubleDataPoint(1, 10.0));
        group.addDataPoint(new DoubleDataPoint(1, 20.3));
        group.addDataPoint(new LongDataPoint(1, 3));
        group.addDataPoint(new LongDataPoint(2, 1));
        group.addDataPoint(new DoubleDataPoint(2, 5.2));
        group.addDataPoint(new DoubleDataPoint(3, 6.0));

        aggregator.setSize(3);
        final DataPointGroup results = aggregator.aggregate(group);

        DataPoint dataPoint = results.next();
        assertThat(dataPoint.getTimestamp()).isEqualTo(1L);
        assertThat(dataPoint.getDoubleValue()).isEqualTo(11.1);

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp()).isEqualTo(2L);
        assertThat(dataPoint.getDoubleValue()).isEqualTo(8.1);

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp()).isEqualTo(2L);
        assertThat(dataPoint.getDoubleValue()).isCloseTo(3.067, Offset.offset(2.0));

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp()).isEqualTo(3L);
        assertThat(dataPoint.getDoubleValue()).isCloseTo(4.067, Offset.offset(2.0));

        assertThat(results.hasNext()).isEqualTo(false);
    }

}
