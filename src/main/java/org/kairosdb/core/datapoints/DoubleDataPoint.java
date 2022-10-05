package org.kairosdb.core.datapoints;


import org.json.JSONException;
import org.json.JSONWriter;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: bhawkins
 * Date: 8/31/13
 * Time: 7:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class DoubleDataPoint extends DataPointHelper {
    private final double m_value;

    public DoubleDataPoint(final long timestamp, final double value) {
        super(timestamp);
        m_value = value;
    }

    @Override
    public double getDoubleValue() {
        return (m_value);
    }

    @Override
    public void writeValueToBuffer(final DataOutput buffer) throws IOException {
        DoubleDataPointFactoryImpl.writeToByteBuffer(buffer, this);
    }

    @Override
    public void writeValueToJson(final JSONWriter writer) throws JSONException {
        //m_value will not equal itself if it is Double.NaN.  Weird I know but that is how it is.
        if (Double.isFinite(m_value)) {
            writer.value(m_value);
        } else {
            writer.value(null);
        }
    }

    @Override
    public String getApiDataType() {
        return API_DOUBLE;
    }

    @Override
    public String getDataStoreDataType() {
        return DoubleDataPointFactoryImpl.DST_DOUBLE;
    }

    @Override
    public boolean isLong() {
        return false;
    }

    @Override
    public long getLongValue() {
        return (long) m_value;
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

        final DoubleDataPoint that = (DoubleDataPoint) o;

        return Double.compare(that.m_value, m_value) == 0;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        final long temp = Double.doubleToLongBits(m_value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
