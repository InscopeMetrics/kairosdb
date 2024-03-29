package org.kairosdb.core.datapoints;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;

/**
 * Created with IntelliJ IDEA.
 * User: bhawkins
 * Date: 8/31/13
 * Time: 7:16 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class DataPointHelper implements DataPoint {
    protected long m_timestamp;
    private DataPointGroup m_dataPointGroup;

    public DataPointHelper(final long timestamp) {
        m_timestamp = timestamp;
    }

    /**
     * Get the timestamp for this data point in milliseconds
     *
     * @return timestamp
     */
    public long getTimestamp() {
        return m_timestamp;
    }

    public void setTimestamp(final long timestamp) {
        m_timestamp = timestamp;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof DataPointHelper)) return false;

        final DataPointHelper that = (DataPointHelper) o;

        return m_timestamp == that.m_timestamp;
    }

    @Override
    public int hashCode() {
        return (int) (m_timestamp ^ (m_timestamp >>> 32));
    }

    @Override
    public String toString() {
        return "DataPointHelper{" +
                "m_timestamp=" + m_timestamp +
                "}";
    }

    /**
     * Returns the data point group for this data point if one is set.
     * Some aggregators may strip off this information
     *
     * @return The DataPointGroup or null if one is not set.
     */
    @Override
    public DataPointGroup getDataPointGroup() {
        return m_dataPointGroup;
    }

    @Override
    public void setDataPointGroup(final DataPointGroup dataPointGroup) {
        m_dataPointGroup = dataPointGroup;
    }
}
