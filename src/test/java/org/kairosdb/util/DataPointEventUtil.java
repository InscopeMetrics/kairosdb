package org.kairosdb.util;

import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.core.DataPoint;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Created by bhawkins on 10/3/16.
 */
public class DataPointEventUtil {
    @SuppressWarnings("unchecked")
    private static DataPointEvent verifyPost(final Publisher<DataPointEvent> eventBus) {
        final ArgumentCaptor<DataPointEvent> event = ArgumentCaptor.forClass(DataPointEvent.class);
        verify(eventBus, timeout(5000).times(1)).post(event.capture());
        reset(eventBus);

        return event.getValue();
    }

    public static void verifyEvent(final Publisher<DataPointEvent> eventBus, final String metricName,
                                   final ImmutableSortedMap<String, String> tags, final DataPoint dataPoint, final int ttl) {
        final DataPointEvent event = verifyPost(eventBus);
        assertThat(event.getMetricName(), equalTo(metricName));
        assertThat(event.getTags(), equalTo(tags));
        assertThat(event.getDataPoint(), equalTo(dataPoint));
        assertThat(event.getTtl(), equalTo(ttl));
    }

    public static void verifyEvent(final Publisher<DataPointEvent> eventBus, final String metricName,
                                   final ImmutableSortedMap<String, String> tags, final DataPoint dataPoint) {
        final DataPointEvent event = verifyPost(eventBus);
        assertThat(event.getMetricName(), equalTo(metricName));
        assertThat(event.getTags(), equalTo(tags));
        assertThat(event.getDataPoint(), equalTo(dataPoint));
    }

    public static void verifyEvent(final Publisher<DataPointEvent> eventBus,
                                   final String metricName,
                                   final DataPoint dataPoint,
                                   final int ttl) {
        final DataPointEvent event = verifyPost(eventBus);
        assertThat(event.getMetricName(), equalTo(metricName));
        assertThat(event.getDataPoint(), equalTo(dataPoint));
        assertThat(event.getTtl(), equalTo(ttl));
    }

    public static void verifyEvent(final Publisher<DataPointEvent> eventBus, final String metricName,
                                   final DataPoint dataPoint) {
        final DataPointEvent event = verifyPost(eventBus);
        assertThat(event.getMetricName(), equalTo(metricName));
        assertThat(event.getDataPoint(), equalTo(dataPoint));
    }
}
