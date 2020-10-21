package org.kairosdb.core.aggregator;

import com.google.inject.Inject;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@FeatureComponent(
        name = "least_squares",
        description = "Returns a best fit line through the datapoints using the least squares algorithm.")
public class LeastSquaresAggregator extends RangeAggregator {
    private final DoubleDataPointFactory m_dataPointFactory;

    @Inject
    public LeastSquaresAggregator(final DoubleDataPointFactory dataPointFactory) {
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

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return new LeastSquaresDataPointAggregator();
    }

    private class LeastSquaresDataPointAggregator implements RangeSubAggregator {
        LeastSquaresDataPointAggregator() {
        }

        @Override
        public Iterable<DataPoint> getNextDataPoints(final long returnTime, final Iterator<DataPoint> dataPointRange) {
            long start = -1L;
            long stop = -1L;
            DataPoint first = null;
            DataPoint second = null;
            int count = 0;
            final SimpleRegression simpleRegression = new SimpleRegression(true);

            while (dataPointRange.hasNext()) {
                count++;
                final DataPoint dp = dataPointRange.next();
                if (second == null) {
                    if (first == null)
                        first = dp;
                    else
                        second = dp;
                }

                stop = dp.getTimestamp();
                if (start == -1L)
                    start = dp.getTimestamp();

                simpleRegression.addData(dp.getTimestamp(), dp.getDoubleValue());
            }

            final List<DataPoint> ret = new ArrayList<>();

            if (count == 1) {
                ret.add(first);
            } else if (count == 2) {
                ret.add(first);
                ret.add(second);
            } else if (count != 0) {
                ret.add(m_dataPointFactory.createDataPoint(start, simpleRegression.predict(start)));
                ret.add(m_dataPointFactory.createDataPoint(stop, simpleRegression.predict(stop)));
            }

            return (ret);
        }
    }
}
