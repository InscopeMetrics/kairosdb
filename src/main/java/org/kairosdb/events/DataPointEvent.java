package org.kairosdb.events;

import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.core.DataPoint;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;


/**
 * Created by bhawkins on 9/17/16.
 * <p>
 * Any listener that take a long time should use a separate thread
 * to do any work.  The calling thread is the protocol thread.
 */
public class DataPointEvent {
    private final String m_metricName;
    private final ImmutableSortedMap<String, String> m_tags;
    private final DataPoint m_dataPoint;
    private final int m_ttl;

    public DataPointEvent(final String metricName, final ImmutableSortedMap<String, String> tags, final DataPoint dataPoint, final int ttl) {
        m_metricName = checkNotNullOrEmpty(metricName);
        m_tags = checkNotNull(tags);
        m_dataPoint = checkNotNull(dataPoint);
        m_ttl = ttl;
    }

    public DataPointEvent(final String metricName, final ImmutableSortedMap<String, String> tags, final DataPoint dataPoint) {
        m_metricName = checkNotNullOrEmpty(metricName);
        m_tags = checkNotNull(tags);
        m_dataPoint = checkNotNull(dataPoint);
        m_ttl = 0;
    }


    public String getMetricName() {
        return m_metricName;
    }

    public ImmutableSortedMap<String, String> getTags() {
        return m_tags;
    }

    public DataPoint getDataPoint() {
        return m_dataPoint;
    }

    public int getTtl() {
        return m_ttl;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DataPointEvent that = (DataPointEvent) o;

        if (m_ttl != that.m_ttl) return false;
        if (!m_metricName.equals(that.m_metricName)) return false;
        if (!m_tags.equals(that.m_tags)) return false;
        return m_dataPoint.equals(that.m_dataPoint);

    }

    @Override
    public int hashCode() {
        int result = m_metricName.hashCode();
        result = 31 * result + m_tags.hashCode();
        result = 31 * result + m_dataPoint.hashCode();
        result = 31 * result + m_ttl;
        return result;
    }
}
