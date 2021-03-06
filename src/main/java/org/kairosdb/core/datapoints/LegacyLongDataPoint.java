package org.kairosdb.core.datapoints;

import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: bhawkins
 * Date: 12/9/13
 * Time: 1:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class LegacyLongDataPoint extends LegacyDataPoint {
    private final long m_value;

    public LegacyLongDataPoint(final long timestamp, final long l) {
        super(timestamp);
        m_value = l;
    }

    @Override
    public void writeValueToBuffer(final DataOutput buffer) throws IOException {
        LegacyDataPointFactory.writeToByteBuffer(buffer, this);
    }

    @Override
    public void writeValueToJson(final JSONWriter writer) throws JSONException {
        writer.value(m_value);
    }

    @Override
    public String getApiDataType() {
        return DataPoint.API_LONG;
    }

    @Override
    public boolean isLong() {
        return true;
    }

    @Override
    public boolean isDouble() {
        return true;
    }

    @Override
    public long getLongValue() {
        return m_value;
    }

    @Override
    public double getDoubleValue() {
        return (double) m_value;
    }
}
