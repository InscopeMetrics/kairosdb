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
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.annotation.ValidationProperty;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.http.rest.validation.NonZero;
import org.kairosdb.util.Reservoir;
import org.kairosdb.util.UniformReservoir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static java.lang.Math.floor;

@FeatureComponent(
        name = "percentile",
        description = "Finds the percentile of the data range.")
public class PercentileAggregator extends RangeAggregator {
    public static final Logger logger = LoggerFactory.getLogger(PercentileAggregator.class);

    private final DoubleDataPointFactory m_dataPointFactory;
    @NonZero
    @FeatureProperty(
            label = "Percentile",
            description = "Data points returned will be in this percentile.",
            default_value = "0.1",
            validations = {
                    @ValidationProperty(
                            expression = "value > 0",
                            message = "Percentile must be greater than 0."
                    ),
                    @ValidationProperty(
                            expression = "value < 1",
                            message = "Percentile must be smaller than 1."
                    )
            }
    )
    private double percentile;

    @Inject
    public PercentileAggregator(final DoubleDataPointFactory dataPointFactory) {
        m_dataPointFactory = dataPointFactory;
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return DataPoint.GROUP_NUMBER.equals(groupType);
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return m_dataPointFactory.getGroupType();
    }

    public void setPercentile(final double percentile) {
        this.percentile = percentile;
    }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return (new PercentileDataPointAggregator());
    }

    private class PercentileDataPointAggregator implements RangeSubAggregator {
        private double[] values;


        @Override
        public Iterable<DataPoint> getNextDataPoints(final long returnTime, final Iterator<DataPoint> dataPointRange) {
            final Reservoir reservoir = new UniformReservoir();

            while (dataPointRange.hasNext()) {
                reservoir.update(dataPointRange.next().getDoubleValue());
            }
            getAndSortValues(reservoir.getValues());
            final double percentileValue = getValue(percentile);

            if (logger.isDebugEnabled()) {
                logger.debug("Aggregating the " + percentile + " percentile");
            }

            return Collections.singletonList(m_dataPointFactory.createDataPoint(returnTime, percentileValue));
        }

        private void getAndSortValues(final double[] values) {
            this.values = values;
            Arrays.sort(this.values);
        }

        /**
         * Returns the value at the given quantile.
         *
         * @param quantile a given quantile, in {@code [0..1]}
         * @return the value in the distribution at {@code quantile}
         */
        private double getValue(final double quantile) {
            if (quantile < 0.0 || quantile > 1.0) {
                throw new IllegalArgumentException(quantile + " is not in [0..1]");
            }

            if (values.length == 0) {
                return 0.0;
            }

            final double pos = quantile * (values.length + 1);

            if (pos < 1) {
                return values[0];
            }

            if (pos >= values.length) {
                return values[values.length - 1];
            }

            final double lower = values[(int) pos - 1];
            final double upper = values[(int) pos];
            return lower + (pos - floor(pos)) * (upper - lower);
        }
    }
}
