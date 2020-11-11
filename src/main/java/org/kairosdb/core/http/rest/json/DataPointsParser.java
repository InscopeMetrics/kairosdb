/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.core.http.rest.json;

import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.impl.NoOpMetricsFactory;
import com.google.common.collect.ImmutableSortedMap;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.NoTagsTagger;
import org.kairosdb.core.reporting.Tagger;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.Util;
import org.kairosdb.util.ValidationException;
import org.kairosdb.util.Validator;

import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Originally used Jackson to parse, but this approach failed for a very large JSON because
 * everything was in memory and we would run out of memory. This parser adds metrics as it walks
 * through the stream.
 */
public class DataPointsParser {
    private final Publisher<DataPointEvent> m_publisher;
    private final Reader inputStream;
    private final Gson gson;
    private final KairosDataPointFactory dataPointFactory;
    private final MetricsFactory metricsFactory;
    private final Tagger tagger;
    private int dataPointCount;
    private int ingestTime;

    public DataPointsParser(
            final Publisher<DataPointEvent> publisher,
            final Reader stream,
            final Gson gson,
            final KairosDataPointFactory dataPointFactory) {
        this(
                publisher,
                stream,
                gson,
                dataPointFactory,
                new NoOpMetricsFactory(),
                new NoTagsTagger.Builder().build());
    }

    public DataPointsParser(
            final Publisher<DataPointEvent> publisher,
            final Reader stream,
            final Gson gson,
            final KairosDataPointFactory dataPointFactory,
            final MetricsFactory metricsFactory,
            final Tagger tagger) {
        m_publisher = publisher;
        this.inputStream = checkNotNull(stream);
        this.gson = gson;
        this.dataPointFactory = dataPointFactory;
        this.metricsFactory = metricsFactory;
        this.tagger = tagger;
    }

    public int getDataPointCount() {
        return dataPointCount;
    }

    public int getIngestTime() {
        return ingestTime;
    }

    public ValidationErrors parse() throws IOException, DatastoreException {
        final long start = System.currentTimeMillis();
        final ValidationErrors validationErrors = new ValidationErrors();

        try (final JsonReader reader = new JsonReader(inputStream)) {
            int metricCount = 0;

            if (reader.peek().equals(JsonToken.BEGIN_ARRAY)) {
                try {
                    reader.beginArray();

                    while (reader.hasNext()) {
                        final NewMetric metric = parseMetric(reader);
                        validateAndAddDataPoints(metric, validationErrors, metricCount);
                        metricCount++;
                    }
                } catch (final EOFException e) {
                    validationErrors.addErrorMessage("Invalid json. No content due to end of input.");
                }

                reader.endArray();
            } else if (reader.peek().equals(JsonToken.BEGIN_OBJECT)) {
                final NewMetric metric = parseMetric(reader);
                validateAndAddDataPoints(metric, validationErrors, 0);
            } else
                validationErrors.addErrorMessage("Invalid start of json.");

        } catch (final EOFException e) {
            validationErrors.addErrorMessage("Invalid json. No content due to end of input.");
        }

        ingestTime = (int) (System.currentTimeMillis() - start);

        return validationErrors;
    }

    private NewMetric parseMetric(final JsonReader reader) {
        final NewMetric metric;
        try {
            metric = gson.fromJson(reader, NewMetric.class);
        } catch (final IllegalArgumentException e) {
            // Happens when parsing data points where one of the pair is missing (timestamp or value)
            throw new JsonSyntaxException("Invalid JSON");
        }
        return metric;
    }

    private String findType(final JsonElement value) throws ValidationException {
        if (!value.isJsonPrimitive()) {
            throw new ValidationException("value is an invalid type");
        }

        final JsonPrimitive primitiveValue = (JsonPrimitive) value;
        if (primitiveValue.isNumber() || (primitiveValue.isString() && Util.isNumber(value.getAsString()))) {
            final String v = value.getAsString();

            if (!v.contains(".")) {
                return "long";
            } else {
                return "double";
            }
        } else
            return "string";
    }

    private boolean validateAndAddDataPoints(
            final NewMetric metric,
            final ValidationErrors errors,
            final int count)
            throws DatastoreException, IOException {
        final ValidationErrors validationErrors = new ValidationErrors();

        final Context context = new Context(count);
        if (metric.validate()) {
            if (Validator.isNotNullOrEmpty(validationErrors, context.setAttribute("name"), metric.getName())) {
                context.setName(metric.getName());
                //Validator.isValidateCharacterSet(validationErrors, context, metric.getName());
            }

            if (metric.getTimestamp() != null)
                Validator.isNotNullOrEmpty(validationErrors, context.setAttribute("value"), metric.getValue());
            else if (metric.getValue() != null && !metric.getValue().isJsonNull())
                Validator.isNotNull(validationErrors, context.setAttribute("timestamp"), metric.getTimestamp());
            //				Validator.isGreaterThanOrEqualTo(validationErrors, context.setAttribute("timestamp"), metric.getTimestamp(), 1);


            if (Validator.isGreaterThanOrEqualTo(validationErrors, context.setAttribute("tags count"), metric.getTags().size(), 1)) {
                int tagCount = 0;
                final SubContext tagContext = new SubContext(context.setAttribute(null), "tag");

                for (final Map.Entry<String, String> entry : metric.getTags().entrySet()) {
                    tagContext.setCount(tagCount);
                    if (Validator.isNotNullOrEmpty(validationErrors, tagContext.setAttribute("name"), entry.getKey())) {
                        tagContext.setName(entry.getKey());
                        Validator.isNotNullOrEmpty(validationErrors, tagContext, entry.getKey());
                    }
                    if (Validator.isNotNullOrEmpty(validationErrors, tagContext.setAttribute("value"), entry.getValue()))
                        Validator.isNotNullOrEmpty(validationErrors, tagContext, entry.getValue());

                    tagCount++;
                }
            }
        }


        if (!validationErrors.hasErrors()) {
            final ImmutableSortedMap<String, String> tags = ImmutableSortedMap.copyOf(metric.getTags());

            if (metric.getTimestamp() != null && metric.getValue() != null) {
                String type = metric.getType();

                if (type == null) {
                    try {
                        type = findType(metric.getValue());
                    } catch (final ValidationException e) {
                        validationErrors.addErrorMessage(context + " " + e.getMessage());
                    }
                }

                if (type != null) {
                    if (dataPointFactory.isRegisteredType(type)) {
                        // New scope required here to allow tagging based on context (e.g. metric name or tags).
                        // This is normally a bad idea; however, the data is either:
                        // 1) Aggregated by KDB in memory through CassandraSink
                        // 2) Aggregated by MAD
                        // 3) Sent to some other metrics system and thus no feedback loop
                        try (Metrics metrics = metricsFactory.create()) {
                            final DataPoint dataPoint = dataPointFactory.createDataPoint(
                                    type,
                                    metric.getTimestamp(),
                                    metric.getValue());
                            m_publisher.post(new DataPointEvent(metric.getName(), tags, dataPoint, metric.getTtl()));

                            tagger.applyTagsToMetrics(metrics, metric::getName, tags::asMultimap);
                            metrics.incrementCounter("parser/samples", dataPoint.getSampleCount());
                            dataPointCount++;
                        }
                    } else {
                        validationErrors.addErrorMessage("Unregistered data point type '" + type + "'");
                    }
                }
            }

            if (metric.getDatapoints() != null && metric.getDatapoints().length > 0) {
                int contextCount = 0;
                final SubContext dataPointContext = new SubContext(context, "datapoints");
                for (final JsonElement[] dataPoint : metric.getDatapoints()) {
                    dataPointContext.setCount(contextCount);
                    if (dataPoint.length < 1) {
                        validationErrors.addErrorMessage(dataPointContext.setAttribute("timestamp") + " cannot be null or empty.");
                        continue;
                    } else if (dataPoint.length < 2) {
                        validationErrors.addErrorMessage(dataPointContext.setAttribute("value") + " cannot be null or empty.");
                        continue;
                    } else {
                        Long timestamp = null;
                        if (!dataPoint[0].isJsonNull())
                            timestamp = dataPoint[0].getAsLong();

                        if (metric.validate() && !Validator.isNotNull(validationErrors, dataPointContext.setAttribute("timestamp"), timestamp))
                            continue;

                        String type = metric.getType();
                        if (dataPoint.length > 2)
                            type = dataPoint[2].getAsString();

                        if (!Validator.isNotNullOrEmpty(validationErrors, dataPointContext.setAttribute("value"), dataPoint[1]))
                            continue;

                        if (type == null) {
                            try {
                                type = findType(dataPoint[1]);
                            } catch (final ValidationException e) {
                                validationErrors.addErrorMessage(context + " " + e.getMessage());
                                continue;
                            }
                        }

                        if (!dataPointFactory.isRegisteredType(type)) {
                            validationErrors.addErrorMessage("Unregistered data point type '" + type + "'");
                            continue;
                        }
                        // New scope required here to allow tagging based on context (e.g. metric name or tags).
                        // This is normally a bad idea; however, the data is either:
                        // 1) Aggregated by KDB in memory through CassandraSink
                        // 2) Aggregated by MAD
                        // 3) Sent to some other metrics system and thus no feedback loop
                        try (Metrics metrics = metricsFactory.create()) {
                            final DataPoint dataPointObj = dataPointFactory.createDataPoint(
                                    type,
                                    timestamp,
                                    dataPoint[1]);
                            m_publisher.post(new DataPointEvent(
                                    metric.getName(),
                                    tags,
                                    dataPointObj,
                                    metric.getTtl()));

                            tagger.applyTagsToMetrics(metrics, metric::getName, tags::asMultimap);
                            metrics.incrementCounter("parser/samples", dataPointObj.getSampleCount());
                            dataPointCount++;
                        }
                    }
                    contextCount++;
                }
            }
        }

        errors.add(validationErrors);

        return !validationErrors.hasErrors();
    }

    private static class Context {
        private final int m_count;
        private String m_name;
        private String m_attribute;

        public Context(final int count) {
            m_count = count;
        }

        private Context setName(final String name) {
            m_name = name;
            m_attribute = null;
            return (this);
        }

        private Context setAttribute(final String attribute) {
            m_attribute = attribute;
            return (this);
        }

        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("metric[").append(m_count).append("]");
            if (m_name != null)
                sb.append("(name=").append(m_name).append(")");

            if (m_attribute != null)
                sb.append(".").append(m_attribute);

            return (sb.toString());
        }
    }

    private static class SubContext {
        private final Context m_context;
        private final String m_contextName;
        private int m_count;
        private String m_name;
        private String m_attribute;

        public SubContext(final Context context, final String contextName) {
            m_context = context;
            m_contextName = contextName;
        }

        private SubContext setCount(final int count) {
            m_count = count;
            m_name = null;
            m_attribute = null;
            return (this);
        }

        private SubContext setName(final String name) {
            m_name = name;
            m_attribute = null;
            return (this);
        }

        private SubContext setAttribute(final String attribute) {
            m_attribute = attribute;
            return (this);
        }

        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(m_context).append(".").append(m_contextName).append("[");
            if (m_name != null)
                sb.append(m_name);
            else
                sb.append(m_count);
            sb.append("]");

            if (m_attribute != null)
                sb.append(".").append(m_attribute);

            return (sb.toString());
        }
    }

    @SuppressWarnings({"MismatchedReadAndWriteOfArray", "UnusedDeclaration"})
    private static class NewMetric {
        private String name;
        private final Long timestamp = null;
        private final Long time = null;
        private JsonElement value;
        private Map<String, String> tags;
        private JsonElement[][] datapoints;
        private final boolean skip_validate = false;
        private String type;
        private final int ttl = 0;

        private String getName() {
            return name;
        }

        public Long getTimestamp() {
            if (time != null)
                return time;
            else
                return timestamp;
        }

        public JsonElement getValue() {
            return value;
        }

        public Map<String, String> getTags() {
            return tags != null ? tags : Collections.emptyMap();
        }

        private JsonElement[][] getDatapoints() {
            return datapoints;
        }

        private boolean validate() {
            return !skip_validate;
        }

        public String getType() {
            return type;
        }

        public int getTtl() {
            return ttl;
        }
    }
}