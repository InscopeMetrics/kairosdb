package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import org.kairosdb.core.DataPoint;
import org.kairosdb.util.KDataInput;

import java.io.DataOutput;
import java.io.IOException;

import static org.kairosdb.core.DataPoint.GROUP_NUMBER;
import static org.kairosdb.util.Util.packLong;
import static org.kairosdb.util.Util.unpackLong;

/**
 * Created with IntelliJ IDEA.
 * User: bhawkins
 * Date: 12/9/13
 * Time: 12:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class LegacyDataPointFactory implements DataPointFactory {
    public static final int LONG_VALUE = 0;
    public static final int DOUBLE_VALUE = 1;

    public static final String DATASTORE_TYPE = "kairos_legacy";

    public static void writeToByteBuffer(final DataOutput buffer, final LegacyLongDataPoint dataPoint) throws IOException {
        final long value = dataPoint.getLongValue();
        buffer.writeByte(LONG_VALUE);
        packLong(value, buffer);
    }

    public static void writeToByteBuffer(final DataOutput buffer, final LegacyDoubleDataPoint dataPoint) throws IOException {
        buffer.writeByte(DOUBLE_VALUE);
        buffer.writeDouble(dataPoint.getDoubleValue());
    }

    @Override
    public String getDataStoreType() {
        return DATASTORE_TYPE;
    }

    @Override
    public String getGroupType() {
        return GROUP_NUMBER;
    }

    @Override
    public DataPoint getDataPoint(final long timestamp, final JsonElement json) {
        // Should never be called for this factory
        return null;
    }

    @Override
    public DataPoint getDataPoint(final long timestamp, final KDataInput buffer) throws IOException {
        final DataPoint ret;

        final int type = buffer.readByte();
        if (type == LONG_VALUE) {
            ret = new LegacyLongDataPoint(timestamp, unpackLong(buffer));
        } else {
            ret = new LegacyDoubleDataPoint(timestamp, buffer.readDouble());
        }

        return ret;
    }
}
