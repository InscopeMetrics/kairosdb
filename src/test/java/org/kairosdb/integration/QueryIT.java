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

package org.kairosdb.integration;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONException;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertTrue;


/*
List of tests we need to perform
1. test with a range that returns no data
2. test that returns data
3. test that will use the cache file to return data.
4. Nice to verify no open file handles after tests have ran.
5. Test aggregators (multiple)
6. Test group by
 */
@RunWith(DataProviderRunner.class)
public class QueryIT {
    private static final JsonParser parser = new JsonParser();
    private static final Gson gson = new Gson();
    private static final TypeToken<List<String>> stringListType = new TypeToken<List<String>>() {
    };

    private String m_host = "127.0.0.1";
    private final String m_port = "8080";

    public QueryIT() {
        m_host = System.getProperty("dockerHostAddress", m_host);
    }

    private static JsonElement readJsonFromStream(final String path, final String metricName) throws IOException, JSONException {
        try (final InputStream is = ClassLoader.getSystemResourceAsStream(path)) {
            if (is == null)
                return (null);

            String str = new String(ByteStreams.toByteArray(is), Charsets.UTF_8);

            // replace metric name
            str = str.replace("<metric_name>", metricName);

            return (parser.parse(str));
        }
    }

    @DataProvider
    public static Object[][] getQueryTests() throws IOException, JSONException, URISyntaxException {
        final ArrayList<Object[]> ret = new ArrayList<Object[]>();

        final List<String> resourceDirectoryNames = getTestDirectories("tests");

        for (final String resourceDirectory : resourceDirectoryNames) {
            final String metricName = "integration_test_" + UUID.randomUUID();
            final JsonElement dataPoints = readJsonFromStream("tests/" + resourceDirectory + "/datapoints.json", metricName);
            final JsonElement query = readJsonFromStream("tests/" + resourceDirectory + "/query.json", metricName);
            final JsonElement response = readJsonFromStream("tests/" + resourceDirectory + "/response.json", metricName);

            checkState(query != null, "No query found for test " + resourceDirectory);

            ret.add(new Object[]{resourceDirectory, dataPoints, query, response});
        }

        return (ret.toArray(new Object[0][]));
    }

    public static List<String> getTestDirectories(final String matchingDirectoryName) throws URISyntaxException, IOException {
        return findTestDirectories(new File("src/test/resources"), matchingDirectoryName);
    }

    @SuppressWarnings("ConstantConditions")
    private static List<String> findTestDirectories(final File directory, final String matchingDirectoryName) {
        final List<String> matchingDirectories = new ArrayList<String>();

        for (final File file : directory.listFiles()) {
            if (file.isDirectory()) {
                if (file.getParentFile().getName().equals(matchingDirectoryName)) {
                    matchingDirectories.add(file.getName());
                } else
                    matchingDirectories.addAll(findTestDirectories(file, matchingDirectoryName));
            }
        }
        return matchingDirectories;
    }

    private JsonElement postQuery(final JsonElement query) throws IOException, JSONException {
        try (final CloseableHttpClient client = HttpClients.createDefault()) {
            final HttpPost post = new HttpPost("http://" + m_host + ":" + m_port + "/api/v1/datapoints/query");
            post.setHeader("Content-Type", "application/json");

            post.setEntity(new StringEntity(query.toString()));
            try (final CloseableHttpResponse httpResponse = client.execute(post)) {
                if (httpResponse.getStatusLine().getStatusCode() != 200) {
                    httpResponse.getEntity().writeTo(System.out);
                    return (null);
                }

                final ByteArrayOutputStream output = new ByteArrayOutputStream(1024);
                httpResponse.getEntity().writeTo(output);

                return (parser.parse(output.toString("UTF-8")));
            }
        }
    }

    private int putDataPoints(final JsonElement dataPoints) throws IOException, JSONException {
        try (final CloseableHttpClient client = HttpClients.createDefault()) {
            final HttpPost post = new HttpPost("http://" + m_host + ":" + m_port + "/api/v1/datapoints");
            post.setHeader("Content-Type", "application/json");

            post.setEntity(new StringEntity(dataPoints.toString()));
            try (final CloseableHttpResponse httpResponse = client.execute(post)) {
                return httpResponse.getStatusLine().getStatusCode();
            }
        }
    }

    private int deleteDataPoints(final JsonElement query) throws IOException, JSONException {
        try (final CloseableHttpClient client = HttpClients.createDefault()) {
            final HttpPost post = new HttpPost("http://" + m_host + ":" + m_port + "/api/v1/datapoints/delete");
            post.setHeader("Content-Type", "application/json");

            post.setEntity(new StringEntity(query.toString()));
            try (final CloseableHttpResponse httpResponse = client.execute(post)) {
                return httpResponse.getStatusLine().getStatusCode();
            }
        }
    }

    @Test
    @UseDataProvider("getQueryTests")
    public void performQueryTest(final String testName, final JsonElement dataPoints, final JsonElement query, final JsonElement response)
            throws IOException, JSONException, InterruptedException {
        int retryCount = 0;
        if (dataPoints != null) {
            final int status = putDataPoints(dataPoints);
            assertThat(status, equalTo(204));
            retryCount = 3;
        }

        do {
            try {
                final JsonElement serverResponse = postQuery(query);
                assertResponse(testName, serverResponse, response);
                break;
            } catch (final AssertionError e) {
                if (retryCount == 0)
                    throw e;

                retryCount--;
                Thread.sleep(500); // Need to wait until datapoints are available in the data store
            }
        } while (true);

        if (dataPoints != null) {
            // clean up
            final int status = deleteDataPoints(query);

            assertThat(status, equalTo(204));

            retryCount = 3;
            do {
                try {
                    // Assert that data points are gone
                    final JsonElement serverResponse = postQuery(query);
                    final JsonArray queries = serverResponse.getAsJsonObject().get("queries").getAsJsonArray();
                    for (final JsonElement responseQuery : queries) {
                        final JsonArray results = responseQuery.getAsJsonObject().get("results").getAsJsonArray();
                        for (final JsonElement result : results) {
                            assertThat(result.getAsJsonObject().get("values").getAsJsonArray().size(), equalTo(0));
                        }
                    }

                    break;
                } catch (final AssertionError e) {
                    if (retryCount == 0)
                        throw e;

                    retryCount--;
                    Thread.sleep(500); // Need to wait until datapoints are available in the data store
                }
            } while (true);
        }
    }

    private void assertResponse(final String testName, final JsonElement actual, final JsonElement expected) {
        final JsonArray actualQueries = actual.getAsJsonObject().get("queries").getAsJsonArray();
        final JsonArray expectedQueries = expected.getAsJsonObject().get("queries").getAsJsonArray();

        assertThat("Number of queries is different for test: " + testName, actualQueries.size(), equalTo(expectedQueries.size()));

        for (int i = 0; i < expectedQueries.size(); i++) {
            final JsonArray actualResult = actualQueries.get(i).getAsJsonObject().get("results").getAsJsonArray();
            final JsonArray expectedResult = expectedQueries.get(i).getAsJsonObject().get("results").getAsJsonArray();

            assertThat("Number of results is different for test: " + testName, actualResult.size(), equalTo(expectedResult.size()));

            for (int j = 0; j < expectedResult.size(); j++) {
                final JsonObject actualMetric = actualResult.get(j).getAsJsonObject();
                final JsonObject expectedMetric = expectedResult.get(j).getAsJsonObject();

                assertThat("Metric name is different for test: " + testName, actualMetric.get("name"), equalTo(expectedMetric.get("name")));
                assertTags(testName, i, j, actualMetric, expectedMetric);
                assertDataPoints(testName, i, j, actualMetric, expectedMetric);
            }
        }
    }

    private void assertTags(final String testName, final int queryCount, final int resultCount, final JsonObject actual, final JsonObject expected) {
        final JsonObject actualTags = actual.getAsJsonObject("tags");
        final JsonObject expectedTags = expected.getAsJsonObject("tags");

        assertThat(String.format("Number of tags is different for test %s, query[%d], result[%d]", testName, queryCount, resultCount),
                actualTags.entrySet().size(), equalTo(expectedTags.entrySet().size()));
        for (final Map.Entry<String, JsonElement> tag : expectedTags.entrySet()) {
            final String tagName = tag.getKey();
            assertThat(String.format("Missing tag: %s for test %s, query[%d], result[%d]",
                    tagName, testName, queryCount, resultCount),
                    actualTags.has(tagName), equalTo(true));

            final List<String> actualTagsList = gson.fromJson(actualTags.get(tagName), stringListType.getType());
            final List<String> expectedTagsList = gson.fromJson(tag.getValue(), stringListType.getType());
            assertTrue(String.format("Tag value different for key: %S for test %s, query[%d], result[%d]",
                    tagName, testName, queryCount, resultCount),
                    actualTagsList.containsAll(expectedTagsList) && expectedTagsList.containsAll(actualTagsList));
        }
    }

    private void assertDataPoints(final String testName, final int queryCount, final int resultCount, final JsonObject actual, final JsonObject expected) {
        final JsonArray actualValues = actual.getAsJsonArray("values");
        final JsonArray expectedValues = expected.getAsJsonArray("values");

        assertThat(String.format("Number of datapoints is different for test %s, query[%d], result[%d]",
                testName, queryCount, resultCount),
                actualValues.size(), equalTo(expectedValues.size()));

        for (int i = 0; i < expectedValues.size(); i++) {
            assertThat(String.format("Timestamps different for data point %d for test %s, query[%d], result[%d]",
                    i, testName, queryCount, resultCount),
                    actualValues.get(i).getAsJsonArray().get(0), equalTo(expectedValues.get(i).getAsJsonArray().get(0)));


            if (isDouble(actualValues.get(i).getAsJsonArray().get(1)))
                assertThat(String.format("Values different for data point: %d for test %s, query[%d], result[%d]",
                        i, testName, queryCount, resultCount),
                        actualValues.get(i).getAsJsonArray().get(1).getAsDouble(),
                        closeTo(expectedValues.get(i).getAsJsonArray().get(1).getAsDouble(), .01));
            else
                assertThat(String.format("Values different for data point: %d for test: %s, query[%d], result[%d]",
                        i, testName, queryCount, resultCount),
                        actualValues.get(i).getAsJsonArray().get(1), equalTo(expectedValues.get(i).getAsJsonArray().get(1)));
        }
    }

    private boolean isDouble(final JsonElement value) {
        return value.toString().contains(".");
    }
}
