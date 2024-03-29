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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.KairosRootConfig;
import org.kairosdb.core.TestDataPointFactory;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.eventbus.EventBusConfiguration;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.eventbus.Subscribe;
import org.kairosdb.events.DataPointEvent;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class DataPointsParserTest {
    private static final KairosDataPointFactory dataPointFactory = new TestDataPointFactory();
    private FilterEventBus eventBus;
    private Publisher<DataPointEvent> publisher;

    @Before
    public void setup() {
        eventBus = new FilterEventBus(new EventBusConfiguration(new KairosRootConfig()));
		publisher = eventBus.createPublisher(DataPointEvent.class);
	}

    @Test
    public void test_emptyJson_Invalid() throws DatastoreException, IOException {
        final String json = "";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json), new Gson(),
                dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getFirstError(), equalTo("Invalid json. No content due to end of input."));
    }

    @Test
    public void test_nullMetricName_Invalid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": 456, \"datapoints\": [[1,2]], \"tags\":{\"foo\":\"bar\"}}, {\"datapoints\": [[1,2]], \"tags\":{\"foo\":\"bar\"}}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getFirstError(), equalTo("metric[1].name may not be null."));
    }

    @Test
    public void test_timestampButNoValue_Invalid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"tags\": {\"foo\":\"bar\"}}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metric1).value may not be null."));
    }

    @Test
    public void test_valueButNoTimestamp_Invalid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"value\": 1234, \"tags\":{\"foo\":\"bar\"}}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metric1).timestamp may not be null."));
    }

    @Test
    public void test_timestamp_Zero_Valid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"timestamp\": 0, \"value\": 1234, \"tags\":{\"foo\":\"bar\"}}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(0));
        assertThat(parser.getDataPointCount(), equalTo(1));
    }

    @Test
    public void test_Timestamp_Negative_Valid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"timestamp\": -1, \"value\": 1234, \"tags\":{\"foo\":\"bar\"}}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(0));
        assertThat(parser.getDataPointCount(), equalTo(1));
    }

    @Test
    public void test_datapoints_empty_Invalid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metric1).datapoints[0].timestamp cannot be null or empty."));
    }

    @Test
    public void test_datapoints_empty_value_Invalid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[2,]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metric1).datapoints[0].value may not be empty."));
    }

    @Test
    public void test_datapoints_empty_timestamp_Invalid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[,2]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metric1).datapoints[0].timestamp may not be null."));
    }

    @Test
    public void test_emptyMetricName_Invalid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[1,2]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getFirstError(), equalTo("metric[0].name may not be empty."));
    }

    @Test
    public void test_metricName_validCharacters() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"bad:你好name\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[1,2]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(0));
        assertThat(parser.getDataPointCount(), equalTo(1));
    }

    @Test
    public void test_emptyTags_Invalid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metricName\", \"timestamp\": 12345, \"value\": 456, \"datapoints\": [[1,2]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metricName).tags count must be greater than or equal to 1."));
    }

    @Test
    public void test_datapoints_timestamp_zero_Valid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[0,2]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(0));
        assertThat(parser.getDataPointCount(), equalTo(1));
    }

    @Test
    public void test_datapoints_timestamp_negative_Valid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[-1,2]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(0));
        assertThat(parser.getDataPointCount(), equalTo(1));
    }

    @Test
    public void test_emptyTagName_Invalid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metricName\", \"tags\":{\"\":\"bar\"}, \"datapoints\": [[1,2]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metricName).tag[0].name may not be empty."));
        assertThat(parser.getDataPointCount(), equalTo(0));
    }

    @Test
    public void test_tagName_withColon() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metricName\", \"tags\":{\"bad:name\":\"bar\"}, \"datapoints\": [[1,2]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(0));
    }

    @Test
    public void test_emptyTagValue_Invalid() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metricName\", \"tags\":{\"foo\":\"\"}, \"datapoints\": [[1,2]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metricName).tag[foo].value may not be empty."));
    }

    @Test
    public void test_tagValue_withColon() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metricName\", \"tags\":{\"foo\":\"bad:value\"}, \"datapoints\": [[1,2]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(0));
    }

    @Test
    public void test_multipleValidationFailures() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metricName\", \"timestamp\": 456, \"value\":\"\", \"tags\":{\"name\":\"\"}}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(2));
        assertThat(validationErrors.getErrors(), hasItem("metric[0](name=metricName).tag[name].value may not be empty."));
        assertThat(validationErrors.getErrors(), hasItem("metric[0](name=metricName).value may not be empty."));
    }

    /**
     * Zero is a special case.
     */
    @Test
    public void test_value_decimal_with_zeros() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metricName\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[1, \"0.000000\"]]}]";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.size(), equalTo(0));
    }

    @Test
    public void test_validJsonWithTimestampValue() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": 4321, \"tags\":{\"foo\":\"bar\"}}]";

        final FakeDataStore fakeds = new FakeDataStore();
        eventBus.register(fakeds);
        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.hasErrors(), equalTo(false));

        final List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
        assertThat(dataPointSetList.size(), equalTo(1));

        assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
        assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
        assertThat(dataPointSetList.get(0).getTags().get("foo"), equalTo("bar"));
        assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(1));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(4321L));
    }

    @Test(expected = JsonSyntaxException.class)
    public void test_invalidJson() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": }]";

        final FakeDataStore fakeds = new FakeDataStore();
        eventBus.register(fakeds);
        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.hasErrors(), equalTo(false));

        final List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
        assertThat(dataPointSetList.size(), equalTo(1));

        assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
        assertThat(dataPointSetList.get(0).getTags().size(), equalTo(0));
        assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(1));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(4321L));
    }

    @Test
    public void test_validJsonWithTimestampValueAndDataPoints() throws DatastoreException, IOException {
        final String json = "[{\"name\": \"metric1\", \"time\": 1234, \"value\": 4321, \"datapoints\": [[456, 654]], \"tags\":{\"foo\":\"bar\"}}]";

        final FakeDataStore fakeds = new FakeDataStore();
        eventBus.register(fakeds);
        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.hasErrors(), equalTo(false));

        final List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
        assertThat(dataPointSetList.size(), equalTo(1));

        assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
        assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
        assertThat(dataPointSetList.get(0).getTags().get("foo"), equalTo("bar"));
        assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(2));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(4321L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(1).getTimestamp(), equalTo(456L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(1).getLongValue(), equalTo(654L));

        assertThat(parser.getDataPointCount(), equalTo(2));
    }

    @Test
    public void test_validJsonWithDatapoints() throws DatastoreException, IOException {
        final String json = Resources.toString(Resources.getResource("json-metric-parser-multiple-metric.json"), Charsets.UTF_8);

        final FakeDataStore fakeds = new FakeDataStore();
        eventBus.register(fakeds);
        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.hasErrors(), equalTo(false));

        final List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
        assertThat(dataPointSetList.size(), equalTo(2));

        assertThat(dataPointSetList.get(0).getName(), equalTo("archive_file_tracked"));
        assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
        assertThat(dataPointSetList.get(0).getTags().get("host"), equalTo("server1"));
        assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(3));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1349109376L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(123L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(1).getTimestamp(), equalTo(1349109377L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(1).getDoubleValue(), equalTo(13.2));
        assertThat(dataPointSetList.get(0).getDataPoints().get(2).getTimestamp(), equalTo(1349109378L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(2).getDoubleValue(), equalTo(23.1));

        assertThat(dataPointSetList.get(1).getName(), equalTo("archive_file_search"));
        assertThat(dataPointSetList.get(1).getTags().size(), equalTo(2));
        assertThat(dataPointSetList.get(1).getTags().get("host"), equalTo("server2"));
        assertThat(dataPointSetList.get(1).getTags().get("customer"), equalTo("Acme"));
        assertThat(dataPointSetList.get(1).getDataPoints().size(), equalTo(1));
        assertThat(dataPointSetList.get(1).getDataPoints().get(0).getTimestamp(), equalTo(1349109378L));
        assertThat(dataPointSetList.get(1).getDataPoints().get(0).getLongValue(), equalTo(321L));

        assertThat(parser.getDataPointCount(), equalTo(4));
    }

    @Test
    public void test_validJsonWithTypes() throws IOException, DatastoreException {
        final String json = Resources.toString(Resources.getResource("json-metric-parser-metrics-with-type.json"), Charsets.UTF_8);

        final FakeDataStore fakeds = new FakeDataStore();
        eventBus.register(fakeds);
        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.hasErrors(), equalTo(false));

        final List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
        assertThat(dataPointSetList.size(), equalTo(2));

        assertThat(dataPointSetList.get(0).getName(), equalTo("archive_file_tracked"));
        assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
        assertThat(dataPointSetList.get(0).getTags().get("host"), equalTo("server1"));
        assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(3));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1349109376L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(123L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(1).getTimestamp(), equalTo(1349109377L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(1).getDoubleValue(), equalTo(13.2));
        assertThat(dataPointSetList.get(0).getDataPoints().get(2).getTimestamp(), equalTo(1349109378L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(2).getDoubleValue(), equalTo(23.1));

        assertThat(dataPointSetList.get(1).getName(), equalTo("archive_file_search"));
        assertThat(dataPointSetList.get(1).getTags().size(), equalTo(2));
        assertThat(dataPointSetList.get(1).getTags().get("host"), equalTo("server2"));
        assertThat(dataPointSetList.get(1).getTags().get("customer"), equalTo("Acme"));
        assertThat(dataPointSetList.get(1).getDataPoints().size(), equalTo(1));
        assertThat(dataPointSetList.get(1).getDataPoints().get(0).getTimestamp(), equalTo(1349109378L));
        assertThat(dataPointSetList.get(1).getDataPoints().get(0).getLongValue(), equalTo(321L));

        assertThat(parser.getDataPointCount(), equalTo(4));
    }

    @Test
    public void test_justObjectNoArray_valid() throws DatastoreException, IOException {
        final String json = "{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": 4321, \"tags\":{\"foo\":\"bar\"}}";

        final FakeDataStore fakeds = new FakeDataStore();
        eventBus.register(fakeds);
        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.hasErrors(), equalTo(false));

        final List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
        assertThat(dataPointSetList.size(), equalTo(1));

        assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
        assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
        assertThat(dataPointSetList.get(0).getTags().get("foo"), equalTo("bar"));
        assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(1));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(4321L));
    }

    @Test
    public void test_stringContainsInteger_valid() throws DatastoreException, IOException {
        final String json = "{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": \"123\", \"tags\":{\"foo\":\"bar\"}}";

        final FakeDataStore fakeds = new FakeDataStore();
        eventBus.register(fakeds);
        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.hasErrors(), equalTo(false));

        final List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
        assertThat(dataPointSetList.size(), equalTo(1));

        assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
        assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
        assertThat(dataPointSetList.get(0).getTags().get("foo"), equalTo("bar"));
        assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(1));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(123L));
    }

    @Test
    public void test_stringContainsDouble_valid() throws DatastoreException, IOException {
        final String json = "{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": \"123.3\", \"tags\":{\"foo\":\"bar\"}}";

        final FakeDataStore fakeds = new FakeDataStore();
        eventBus.register(fakeds);
        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.hasErrors(), equalTo(false));

        final List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
        assertThat(dataPointSetList.size(), equalTo(1));

        assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
        assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
        assertThat(dataPointSetList.get(0).getTags().get("foo"), equalTo("bar"));
        assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(1));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
        assertThat(dataPointSetList.get(0).getDataPoints().get(0).getDoubleValue(), equalTo(123.3));

        assertThat(parser.getDataPointCount(), equalTo(1));
    }

    @Test
    public void test_valueType_invalid() throws DatastoreException, IOException {
        // Value is a map which is not valid
        final String json = "{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": " + new HashMap<>() + ", \"tags\":{\"foo\":\"bar\"}}";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.hasErrors(), equalTo(true));
        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getErrors().get(0), equalTo("metric[0](name=metric1) value is an invalid type"));
    }

    @Test
    public void test_valueType_dataPointArray_invalid() throws DatastoreException, IOException {
        // Value is a map which is not valid
        final String json = "{\"name\": \"metric1\", \"datapoints\": [[1349109376, " + new HashMap<>() + "]], \"tags\":{\"foo\":\"bar\"}}";

        final DataPointsParser parser = new DataPointsParser(publisher, new StringReader(json),
                new Gson(), dataPointFactory);

        final ValidationErrors validationErrors = parser.parse();

        assertThat(validationErrors.hasErrors(), equalTo(true));
        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getErrors().get(0), equalTo("metric[0](name=metric1) value is an invalid type"));
    }

    @Test
    public void test_parserSpeed() throws DatastoreException, IOException {
        final Reader skipReader = new InputStreamReader(
                new GZIPInputStream(ClassLoader.getSystemResourceAsStream("large_import_skip.gz")));

        final Reader reader = new InputStreamReader(
                new GZIPInputStream(ClassLoader.getSystemResourceAsStream("large_import.gz")));


        DataPointsParser parser = new DataPointsParser(publisher, skipReader,
                new Gson(), dataPointFactory);
        ValidationErrors validationErrors = parser.parse();
        System.out.println(parser.getDataPointCount());
        System.out.println("No ValidationProperty.java");
        System.out.println(parser.getIngestTime());

        parser = new DataPointsParser(publisher, reader, new Gson(), dataPointFactory);
        validationErrors = parser.parse();
        System.out.println("With ValidationProperty.java");
        System.out.println(parser.getIngestTime());
    }


    private static class FakeDataStore implements Datastore, ServiceKeyStore {
        List<DataPointSet> dataPointSetList = new ArrayList<>();
        private DataPointSet lastDataPointSet;

        protected FakeDataStore() throws DatastoreException {
        }

        public List<DataPointSet> getDataPointSetList() {
            return dataPointSetList;
        }

        @Override
        public void close() throws InterruptedException, DatastoreException {
        }

        @Subscribe
        public void putDataPoint(final DataPointEvent event) throws DatastoreException {
            if ((lastDataPointSet == null) || (!lastDataPointSet.getName().equals(event.getMetricName())) ||
                    (!lastDataPointSet.getTags().equals(event.getTags()))) {
                lastDataPointSet = new DataPointSet(event.getMetricName(), event.getTags(), Collections.emptyList());
                dataPointSetList.add(lastDataPointSet);
            }

            lastDataPointSet.addDataPoint(event.getDataPoint());
        }

		/*@Override
		public void putDataPoints(DataPointSet dps) throws DatastoreException
		{
			dataPointSetList.add(dps);
		}*/

        @Override
        public Iterable<String> getMetricNames(final String prefix) throws DatastoreException {
            return null;
        }

        @Override
        public Iterable<String> getTagNames() throws DatastoreException {
            return null;
        }

        @Override
        public Iterable<String> getTagValues() throws DatastoreException {
            return null;
        }

        @Override
        public void queryDatabase(final DatastoreMetricQuery query, final QueryCallback queryCallback) throws DatastoreException {

        }

        @Override
        public void deleteDataPoints(final DatastoreMetricQuery deleteQuery) throws DatastoreException {
        }

        @Override
        public TagSet queryMetricTags(final DatastoreMetricQuery query) throws DatastoreException {
            return null;
        }

        @Override
        public long queryCardinality(final DatastoreMetricQuery query) {
            return 0;
        }

        @Override
        public void setValue(final String service, final String serviceKey, final String key, final String value) throws DatastoreException {

        }

        @Override
        public ServiceKeyValue getValue(final String service, final String serviceKey, final String key) throws DatastoreException {
            return null;
        }

        @Override
        public Iterable<String> listServiceKeys(final String service)
                throws DatastoreException {
            return null;
        }

        @Override
        public Iterable<String> listKeys(final String service, final String serviceKey) throws DatastoreException {
            return null;
        }

        @Override
        public Iterable<String> listKeys(final String service, final String serviceKey, final String keyStartsWith) throws DatastoreException {
            return null;
        }

        @Override
        public void deleteKey(final String service, final String serviceKey, final String key)
                throws DatastoreException {
        }

        @Override
        public Date getServiceKeyLastModifiedTime(final String service, final String serviceKey)
                throws DatastoreException {
            return null;
        }
    }
}
