package org.kairosdb.core.queue;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import javax.inject.Inject;

/**
 * Created by bhawkins on 10/25/16.
 */
public class DataPointEventSerializer {
    public static final Logger logger = LoggerFactory.getLogger(DataPointEventSerializer.class);

    private final KairosDataPointFactory m_kairosDataPointFactory;

    @Inject
    public DataPointEventSerializer(final KairosDataPointFactory kairosDataPointFactory) {
        m_kairosDataPointFactory = kairosDataPointFactory;
    }

    public byte[] serializeEvent(final DataPointEvent dataPointEvent) {
        //Todo: Create some adaptive value here, keep stats on if the buffer increases and slowely increase it
        final ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput(64);
        dataOutput.writeUTF(dataPointEvent.getMetricName());
        dataOutput.writeInt(dataPointEvent.getTtl());
        dataOutput.writeLong(dataPointEvent.getDataPoint().getTimestamp());
        dataOutput.writeUTF(dataPointEvent.getDataPoint().getDataStoreDataType());
        try {
            dataPointEvent.getDataPoint().writeValueToBuffer(dataOutput);
        } catch (final IOException e) {
            logger.error("Error serializing DataPoint", e);
        }

        dataOutput.writeInt(dataPointEvent.getTags().size());
        for (final Map.Entry<String, String> entry : dataPointEvent.getTags().entrySet()) {
            dataOutput.writeUTF(entry.getKey());
            dataOutput.writeUTF(entry.getValue());
        }

        return dataOutput.toByteArray();
    }

    DataPointEvent deserializeEvent(final byte[] bytes) {
        DataPointEvent ret = null;
        try {
            final ByteArrayDataInput dataInput = ByteStreams.newDataInput(bytes);
            final String metricName = dataInput.readUTF();
            final int ttl = dataInput.readInt();
            final long timestamp = dataInput.readLong();
            final String storeType = dataInput.readUTF();

            final DataPoint dataPoint = m_kairosDataPointFactory.createDataPoint(storeType, timestamp, dataInput);

            final int tagCount = dataInput.readInt();
            final ImmutableSortedMap.Builder<String, String> builder = ImmutableSortedMap.naturalOrder();
            for (int I = 0; I < tagCount; I++) {
                builder.put(dataInput.readUTF(), dataInput.readUTF());
            }

            ret = new DataPointEvent(metricName, builder.build(), dataPoint, ttl);

        } catch (final IOException e) {
            logger.error("Unable to deserialize event", e);
        }

        return ret;
    }
}
