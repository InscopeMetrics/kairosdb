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

import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;

public class StdAggregatorTest {

    private static double naiveStdDev(final long[] values) {
        double sum = 0;
        double mean = 0;

        for (final double value : values) {
            sum += value;
        }
        mean = sum / values.length;

        double squaresum = 0;
        for (final double value : values) {
            squaresum += Math.pow(value - mean, 2);
        }
        final double variance = squaresum / values.length;
        return Math.sqrt(variance);
    }

    @Test
    public void test() {
        final ListDataPointGroup group = new ListDataPointGroup("group");
        for (int i = 0; i < 10000; i++) {
            group.addDataPoint(new LongDataPoint(1, i));
        }

        final StdAggregator aggregator = new StdAggregator(new DoubleDataPointFactoryImpl());

        final DataPointGroup dataPointGroup = aggregator.aggregate(group);

        final DataPoint stdev = dataPointGroup.next();
        assertThat(stdev.getDoubleValue(), closeTo(2886.462, 0.44));
    }

    @Test
    public void test_random() {
        final long seed = System.nanoTime();
        final Random random = new Random(seed);

        final long[] values = new long[1000];
        final ListDataPointGroup group = new ListDataPointGroup("group");
        for (int i = 0; i < values.length; i++) {
            final long randomValue = random.nextLong();
            group.addDataPoint(new LongDataPoint(1, randomValue));
            values[i] = randomValue;
        }

        final StdAggregator aggregator = new StdAggregator(new DoubleDataPointFactoryImpl());

        final DataPointGroup dataPointGroup = aggregator.aggregate(group);

        final DataPoint stdev = dataPointGroup.next();
        final double expected = naiveStdDev(values);
        final double epsilon = 0.001 * expected;
        assertThat(stdev.getDoubleValue(), closeTo(expected, epsilon));
    }
}