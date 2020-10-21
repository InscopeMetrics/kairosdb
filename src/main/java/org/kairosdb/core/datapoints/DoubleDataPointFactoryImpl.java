package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import org.kairosdb.core.DataPoint;
import org.kairosdb.util.KDataInput;

import java.io.DataOutput;
import java.io.IOException;

import static org.kairosdb.core.DataPoint.GROUP_NUMBER;

public class DoubleDataPointFactoryImpl implements DoubleDataPointFactory {
    public static final String DST_DOUBLE = "kairos_double";

    public static void writeToByteBuffer(final DataOutput buffer, final DoubleDataPoint dataPoint) throws IOException {
        buffer.writeDouble(dataPoint.getDoubleValue());
    }

    @Override
    public DataPoint createDataPoint(final long timestamp, final double value) {
        return new DoubleDataPoint(timestamp, value);
    }

    @Override
    public String getDataStoreType() {
        return DST_DOUBLE;
    }

    @Override
    public String getGroupType() {
        return GROUP_NUMBER;
    }

    @Override
    public DataPoint getDataPoint(final long timestamp, final JsonElement json) {
        double value = 0.0;
        if (!json.isJsonNull())
            value = json.getAsDouble();
        return new DoubleDataPoint(timestamp, value);
    }

    @Override
    public DataPoint getDataPoint(final long timestamp, final KDataInput buffer) throws IOException {
        final double value = buffer.readDouble();

        return new DoubleDataPoint(timestamp, value);
    }
}
