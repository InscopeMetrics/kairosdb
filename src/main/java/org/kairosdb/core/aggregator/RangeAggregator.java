/*
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

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.GregorianChronology;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.FeatureCompoundProperty;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.plugin.Aggregator;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.TimeZone;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class RangeAggregator implements Aggregator, TimezoneAware {
    @NotNull
    @Valid
    @FeatureCompoundProperty(
            name = "sampling",
            label = "Sampling",
            order = {"Value", "Unit"})
    protected Sampling m_sampling = new Sampling(1, TimeUnit.MILLISECONDS);
    @FeatureProperty(
            name = "align_start_time",
            label = "Align start time",
            description = "Setting this to true will cause the aggregation range to be aligned based on the sampling"
                    + " size. For example if your sample size is either milliseconds, seconds, minutes or hours then the"
                    + " start of the range will always be at the top of the hour. The effect of setting this to true is"
                    + " that your data will take the same shape when graphed as you refresh the data. Note that"
                    + " align_sampling, align_start_time, and align_end_time are mutually exclusive. If more than one"
                    + " are set, unexpected results will occur.",
            default_value = "false")
    protected boolean m_alignStartTime;
    @FeatureProperty(
            name = "align_end_time",
            label = "Align end time",
            description = "Setting this to true will cause the aggregation range to be aligned based on the sampling"
                    + " size. For example if your sample size is either milliseconds, seconds, minutes or hours then the"
                    + " start of the range will always be at the top of the hour. The difference between align_start_time"
                    + " and align_end_time is that align_end_time sets the timestamp for the datapoint to the beginning of"
                    + " the following period versus the beginning of the current period. As with align_start_time, setting"
                    + " this to true will cause your data to take the same shape when graphed as you refresh the data. Note"
                    + " that align_sampling, align_start_time, and align_end_time are mutually exclusive. If more than one"
                    + " are set, unexpected results will occur.",
            default_value = "false")
    protected boolean m_alignEndTime;
    private long m_startTime = 0L;
    private long m_queryStartTime = 0L;
    private long m_queryEndTime = 0L;
    //	private boolean m_started = false;
    private final boolean m_exhaustive;
    private DateTimeZone m_timeZone = DateTimeZone.UTC;
    @FeatureProperty(
            name = "align_sampling",
            label = "Align sampling",
            description = "When set to true the time for the aggregated data point for each range will fall on the start"
                    + " of the range instead of being the value for the first data point within that range. Note that"
                    + " align_sampling, align_start_time, and align_end_time are mutually exclusive. If more than one"
                    + " are set, unexpected results will occur.",
            default_value = "true")
    private boolean m_alignSampling;

    public RangeAggregator() {
        this(false);
    }

    public RangeAggregator(final boolean exhaustive) {
        m_exhaustive = exhaustive;
    }

    public DataPointGroup aggregate(final DataPointGroup dataPointGroup) {
        checkNotNull(dataPointGroup);

        if (m_alignSampling)
            m_startTime = alignRangeBoundary(m_startTime);

        if (m_exhaustive)
            return (new ExhaustiveRangeDataPointAggregator(dataPointGroup, getSubAggregator()));
        else
            return (new RangeDataPointAggregator(dataPointGroup, getSubAggregator()));
    }

    /**
     * For YEARS, MONTHS, WEEKS, DAYS:
     * Computes the timestamp of the first millisecond of the day
     * of the timestamp.
     * For HOURS,
     * Computes the timestamp of the first millisecond of the hour
     * of the timestamp.
     * For MINUTES,
     * Computes the timestamp of the first millisecond of the minute
     * of the timestamp.
     * For SECONDS,
     * Computes the timestamp of the first millisecond of the second
     * of the timestamp.
     * For MILLISECONDS,
     * returns the timestamp
     *
     * @param timestamp
     * @return
     */
    @SuppressWarnings("fallthrough")
    private long alignRangeBoundary(final long timestamp) {
        DateTime dt = new DateTime(timestamp, m_timeZone);
        TimeUnit tu = m_sampling.getUnit();
        switch (tu) {
            case YEARS:
                dt = dt.withDayOfYear(1).withMillisOfDay(0);
                break;
            case MONTHS:
                dt = dt.withDayOfMonth(1).withMillisOfDay(0);
                break;
            case WEEKS:
                dt = dt.withDayOfWeek(1).withMillisOfDay(0);
                break;
            case DAYS:
            case HOURS:
            case MINUTES:
            case SECONDS:
                dt = dt.withHourOfDay(0);
                dt = dt.withMinuteOfHour(0);
                dt = dt.withSecondOfMinute(0);
            default:
                dt = dt.withMillisOfSecond(0);
                break;
        }
        return dt.getMillis();
    }

    /**
     * When set to true the time for the aggregated data point for each range will
     * fall on the start of the range instead of being the value for the first
     * data point within that range.
     *
     * @param align
     */
    public void setAlignStartTime(final boolean align) {
        m_alignStartTime = align;
    }

    /**
     * When set to true the time for the aggregated data point for each range will
     * fall on the end of the range instead of being the value for the first
     * data point within that range.
     *
     * @param align
     */
    public void setAlignEndTime(final boolean align) {
        m_alignEndTime = align;
    }

    /**
     * Setting this to true will cause the aggregation range to be aligned based on
     * the sampling size.  For example if your sample size is either milliseconds,
     * seconds, minutes or hours then the start of the range will always be at the top
     * of the hour.  The effect of setting this to true is that your data will
     * take the same shape when graphed as you refresh the data.
     *
     * @param align Set to true to align the range on fixed points instead of
     *              the start of the query.
     */
    public void setAlignSampling(final boolean align) {
        m_alignSampling = align;
    }

    /**
     * Start time to calculate the ranges from.  Typically this is the start
     * of the query
     *
     * @param startTime
     */
    public void setStartTime(final long startTime) {
        m_startTime = startTime;
        m_queryStartTime = startTime;
    }

    public void setEndTime(final long endTime) {
        //This is to tell the exhaustive agg when to stop.
        //If the end time is not specified in the query the end time is
        //set to MAX_LONG which causes exhaustive agg to go on forever.
        long now = System.currentTimeMillis();
        if (endTime > now)
            m_queryEndTime = now;
        else
            m_queryEndTime = endTime;
    }

    /**
     * Return a RangeSubAggregator that will be used to aggregate data over a
     * discrete range of data points.  This is called once per grouped data series.
     * <p>
     * For example, if one metric is queried and no grouping is done this method is
     * called once and the resulting object is called over and over for each range
     * within the results.
     * <p>
     * If the query were grouping by the host tag and host has values of 'A' and 'B'
     * this method will be called twice, once to aggregate results for 'A' and once
     * to aggregate results for 'B'.
     *
     * @return
     */
    protected abstract RangeSubAggregator getSubAggregator();

    /**
     * Sets the time zone to use for range calculations
     *
     * @param timeZone
     */
    public void setTimeZone(final DateTimeZone timeZone) {
        m_timeZone = timeZone;
    }

    public Sampling getSampling() {
        return m_sampling;
    }

    public void setSampling(final Sampling sampling) {
        m_sampling = sampling;
    }

    //===========================================================================

    /**
     * Instances of this object are created once per grouped data series.
     */
    public interface RangeSubAggregator {
        /**
         * Returns an aggregated data point from a range that is passed in
         * as dataPointRange.
         *
         * @param returnTime     Timestamp to use on return data point.  This is currently
         *                       passing the timestamp of the first data point in the range.
         * @param dataPointRange Range to aggregate over.
         * @return
         */
        Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange);
    }

    /**
     *
     */
    private class RangeDataPointAggregator extends AggregatedDataPointGroupWrapper {
        protected RangeSubAggregator m_subAggregator;
        protected Calendar m_calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        protected Iterator<DataPoint> m_dpIterator;
        /* used for generic range computations */
        private final DateTimeField m_unitField;

        public RangeDataPointAggregator(
                final DataPointGroup innerDataPointGroup,
                final RangeSubAggregator subAggregator) {
            super(innerDataPointGroup);
            m_subAggregator = subAggregator;
            m_dpIterator = Collections.emptyIterator();

            final Chronology chronology = GregorianChronology.getInstance(m_timeZone);

            final TimeUnit tu = m_sampling.getUnit();
            switch (tu) {
                case YEARS:
                    m_unitField = chronology.year();
                    break;
                case MONTHS:
                    m_unitField = chronology.monthOfYear();
                    break;
                case WEEKS:
                    m_unitField = chronology.weekOfWeekyear();
                    break;
                case DAYS:
                    m_unitField = chronology.dayOfMonth();
                    break;
                case HOURS:
                    m_unitField = chronology.hourOfDay();
                    break;
                case MINUTES:
                    m_unitField = chronology.minuteOfHour();
                    break;
                case SECONDS:
                    m_unitField = chronology.secondOfDay();
                    break;
                default:
                    m_unitField = chronology.millisOfSecond();
                    break;
            }
        }


        protected long getStartRange(final long timestamp) {
            final long samplingValue = m_sampling.getValue();
            final long numberOfPastPeriods = m_unitField.getDifferenceAsLong(timestamp/*getDataPointTime()*/, m_startTime) / samplingValue;
            return m_unitField.add(m_startTime, numberOfPastPeriods * samplingValue);
        }

        protected long getEndRange(final long timestamp) {
            final long samplingValue = m_sampling.getValue();
            final long numberOfPastPeriods = m_unitField.getDifferenceAsLong(timestamp/*getDataPointTime()*/, m_startTime) / samplingValue;
            return m_unitField.add(m_startTime, (numberOfPastPeriods + 1) * samplingValue);
        }

        @Override
        public DataPoint next() {
            if (!m_dpIterator.hasNext()) {
                //We calculate start and end ranges as the ranges may not be
                //consecutive if data does not show up in each range.
                final long startRange = getStartRange(currentDataPoint.getTimestamp());
                final long endRange = getEndRange(currentDataPoint.getTimestamp());

                final SubRangeIterator subIterator = new SubRangeIterator(
                        endRange);

                m_dpIterator = m_subAggregator.getNextDataPoints(getDataPointTime(),
                        subIterator).iterator();
            }

            return (m_dpIterator.next());
        }


        /**
         * Computes the data point time for the aggregated value.
         * Different strategies could be added here such as
         * datapoint time = range start time
         * = range end time
         * = range median
         * = current datapoint time
         *
         * @return
         */
        private long getDataPointTime() {
            long datapointTime = currentDataPoint.getTimestamp();
            if (m_alignStartTime) {
                datapointTime = getStartRange(datapointTime);
            } else if (m_alignEndTime) {
                datapointTime = getEndRange(datapointTime);
            }
            return datapointTime;
        }

        /**
         * @return true if there is a subrange left
         */
        @Override
        public boolean hasNext() {
            return (m_dpIterator.hasNext() || super.hasNext());
        }

        //========================================================================

        /**
         * This class provides an iterator over a discrete range of data points
         */
        protected class SubRangeIterator implements Iterator<DataPoint> {
            private final long m_endRange;

            public SubRangeIterator(final long endRange) {
                m_endRange = endRange;
            }

            @Override
            public boolean hasNext() {
                return ((currentDataPoint != null) && (currentDataPoint.getTimestamp() < m_endRange));
            }

            @Override
            public DataPoint next() {
                final DataPoint ret = currentDataPoint;
                if (hasNextInternal())
                    currentDataPoint = nextInternal();

                return ret;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }
    }

    //===========================================================================

    //========================================================================
    private class ExhaustiveRangeDataPointAggregator extends RangeDataPointAggregator {
        private long m_nextExpectedRangeStartTime;

        public ExhaustiveRangeDataPointAggregator(
                final DataPointGroup innerDataPointGroup,
                final RangeSubAggregator subAggregator) {
            super(innerDataPointGroup, subAggregator);
            m_nextExpectedRangeStartTime = m_queryStartTime;
        }

        private void setNextStartTime(final long timeStamp) {
            m_nextExpectedRangeStartTime = timeStamp;
        }

        @Override
        public boolean hasNext() {
            return (super.hasNext() || m_nextExpectedRangeStartTime < m_queryEndTime);
        }

        @Override
        public DataPoint next() {
            if (!m_dpIterator.hasNext()) {
                //We calculate start and end ranges as the ranges may not be
                //consecutive if data does not show up in each range.
                final long startTime = m_nextExpectedRangeStartTime;
                /*if (!m_started)
                {
					m_started = true;
					startTime = currentDataPoint.getTimestamp();
				}*/
                final long startRange = getStartRange(startTime);
                final long endRange = getEndRange(startTime);

                // Next expected range starts just after this end range
                setNextStartTime(endRange);
                final SubRangeIterator subIterator = new SubRangeIterator(
                        endRange);

                long dataPointTime = Long.MAX_VALUE;
                if (currentDataPoint != null)
                    dataPointTime = currentDataPoint.getTimestamp();

                if (m_alignStartTime || endRange <= dataPointTime)
                    dataPointTime = startRange;

                m_dpIterator = m_subAggregator.getNextDataPoints(dataPointTime,
                        subIterator).iterator();
            }

            return (m_dpIterator.next());
        }
    }
}
