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

import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;

import java.util.Collections;
import java.util.Iterator;

/**
 * Converts all longs to double. This will cause a loss of precision for very large long values.
 */
@FeatureComponent(
        name = "last",
        description = "Returns the last value data point for the time range.")
public class LastAggregator extends RangeAggregator {
    private final DoubleDataPointFactory m_dataPointFactory;

    @Inject
    public LastAggregator(final DoubleDataPointFactory dataPointFactory) {
        m_dataPointFactory = dataPointFactory;
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return true;
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return m_dataPointFactory.getGroupType();
    }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return (new LastDataPointAggregator());
    }

    private class LastDataPointAggregator implements RangeSubAggregator {
        @Override
        public Iterable<DataPoint> getNextDataPoints(final long returnTime, final Iterator<DataPoint> dataPointRange) {
            DataPoint last = null;
            while (dataPointRange.hasNext()) {
                last = dataPointRange.next();
            }

            if (last != null) {
                if (m_alignStartTime || m_alignEndTime)
                    last.setTimestamp(returnTime);

                return Collections.singletonList(last);
            }

            return Collections.emptyList();
        }
    }
}
