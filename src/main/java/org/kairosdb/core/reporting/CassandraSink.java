package org.kairosdb.core.reporting;

import com.arpnetworking.metrics.Event;
import com.arpnetworking.metrics.Quantity;
import com.arpnetworking.metrics.Sink;
import com.google.common.collect.Maps;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.util.SimpleStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Implementation of metrics sink for periodically writing data
 * to Cassandra.
 */
public class CassandraSink implements Sink, KairosMetricReporter {

    final ConcurrentMap<Key, SimpleStats> statistics = Maps.newConcurrentMap();

    private CassandraSink(final Builder builder) {
    }

    @Override
    public void record(final Event event) {
        addDataPoints(event.getCounterSamples(), event.getAnnotations());
        addDataPoints(event.getGaugeSamples(), event.getAnnotations());
        addDataPoints(event.getTimerSamples(), event.getAnnotations());
    }

    private void addDataPoints(
            final Map<String, List<Quantity>> metricSamples,
            final Map<String, String> annotations) {
        for (final Map.Entry<String, List<Quantity>> entry : metricSamples.entrySet()) {
            final String metric = entry.getKey();
            final List<Quantity> samples = entry.getValue();

            final Key key = new Key(metric, annotations);
            final SimpleStats stats = statistics.computeIfAbsent(key, k -> new SimpleStats());
            stats.addAllValues(samples.stream().map(q -> q.getValue().longValue()).collect(Collectors.toList()));
        }
    }

    @Override
    public List<DataPointSet> getMetrics(final long now) {
        final Map<Key, SimpleStats> previousStats;
        synchronized (this) {
            previousStats = new HashMap<>(statistics);
            statistics.clear();
        }
        final List<DataPointSet> dataPointSets = new ArrayList<>();
        for (final Map.Entry<Key, SimpleStats> entry : previousStats.entrySet()) {
            final Key key = entry.getKey();
            final SimpleStats.Data stats = entry.getValue().getAndClear();
            dataPointSets.add(newDataPointSet(key.metric, "min", now, stats.min, key.annotations));
            dataPointSets.add(newDataPointSet(key.metric, "max", now, stats.max, key.annotations));
            dataPointSets.add(newDataPointSet(key.metric, "avg", now, stats.avg, key.annotations));
            dataPointSets.add(newDataPointSet(key.metric, "count", now, stats.count, key.annotations));
            dataPointSets.add(newDataPointSet(key.metric, "sum", now, stats.sum, key.annotations));
        }
        return dataPointSets;
    }

    private DataPointSet newDataPointSet(
            final String metric,
            final String statistic,
            final long now,
            final long value,
            final Map<String, String> annotations) {
        return new DataPointSet(
                metric + "." + statistic,
                annotations,
                Collections.singletonList(new LongDataPoint(now, value)));
    }

    private DataPointSet newDataPointSet(
            final String metric,
            final String statistic,
            final long now,
            final double value,
            final Map<String, String> annotations) {
        return new DataPointSet(
                metric + "." + statistic,
                annotations,
                Collections.singletonList(new DoubleDataPoint(now, value)));
    }

    private static final class Key {
        final String metric;
        final Map<String, String> annotations;

        public Key(final String metric, final Map<String, String> annotations) {
            this.metric = metric;
            this.annotations = annotations;
        }

        @Override
        public int hashCode() {
            return Objects.hash(metric, annotations);
        }

        @Override
        public boolean equals(final Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof Key)) {
                return false;
            }
            final Key otherKey = (Key) other;
            return Objects.equals(metric, otherKey.metric)
                    && Objects.equals(annotations, otherKey.annotations);
        }
    }

    public static final class Builder {
        /**
         * Create an instance of {@link Sink}.
         *
         * @return Instance of {@link Sink}.
         */
        public Sink build() {
            return new CassandraSink(this);
        }
    }
}
