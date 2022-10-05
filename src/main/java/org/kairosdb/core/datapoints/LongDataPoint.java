package org.kairosdb.core.datapoints;


import org.joda.time.DateTime;
import org.json.JSONException;
import org.json.JSONWriter;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: bhawkins
 * Date: 8/31/13
 * Time: 7:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class LongDataPoint extends DataPointHelper {
    private final long m_value;

    public LongDataPoint(final long timestamp, final long value) {
        super(timestamp);
        m_value = value;
    }

    public long getValue() {
        return (m_value);
    }

    @Override
    public double getDoubleValue() {
        return (double) m_value;
    }

    @Override
    public void writeValueToBuffer(final DataOutput buffer) throws IOException {
        LongDataPointFactoryImpl.writeToByteBuffer(buffer, this);
    }

    @Override
    public void writeValueToJson(final JSONWriter writer) throws JSONException {
        writer.value(m_value);
    }

    @Override
    public String getApiDataType() {
        return API_LONG;
    }

    @Override
    public String getDataStoreDataType() {
        return LongDataPointFactoryImpl.DST_LONG;
    }

    @Override
    public boolean isLong() {
        return true;
    }

    @Override
    public long getLongValue() {
        return m_value;
    }

    @Override
    public boolean isDouble() {
        return true;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        final LongDataPoint that = (LongDataPoint) o;

        return m_value == that.m_value;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (m_value ^ (m_value >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LongDataPoint{" +
                "m_timestamp=" + new DateTime(m_timestamp) +
                " m_value=" + m_value +
                '}';
    }
}
