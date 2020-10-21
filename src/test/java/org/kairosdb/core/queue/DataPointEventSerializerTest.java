package org.kairosdb.core.queue;

import com.google.common.collect.ImmutableSortedMap;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.TestDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.events.DataPointEvent;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Created by bhawkins on 10/25/16.
 */
public class DataPointEventSerializerTest {
    private final LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

    @Test
    public void test_serializeDeserialize() {
        final KairosDataPointFactory dataPointFactory = new TestDataPointFactory();
        final DataPointEventSerializer serializer = new DataPointEventSerializer(dataPointFactory);

        final ImmutableSortedMap<String, String> tags =
                ImmutableSortedMap.<String, String>naturalOrder()
                        .put("tag1", "val1")
                        .put("tag2", "val2")
                        .put("tag3", "val3").build();

        final DataPoint dataPoint = m_longDataPointFactory.createDataPoint(123L, 43);
        final DataPointEvent original = new DataPointEvent("new_metric", tags, dataPoint, 500);

        final byte[] bytes = serializer.serializeEvent(original);

        final DataPointEvent processedEvent = serializer.deserializeEvent(bytes);

        assertThat(original, equalTo(processedEvent));
    }
}
