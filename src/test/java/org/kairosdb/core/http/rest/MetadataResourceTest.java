package org.kairosdb.core.http.rest;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.testing.JsonResponse;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class MetadataResourceTest extends ResourceBase {
    private static final String SERVICE = "service";
    private static final String UNAUTHORIZED_SERVICE = "_service";
    private static final String SERVICE_KEY1 = "service_key1";
    private static final String SERVICE_KEY2 = "service_key2";
    private static final String METADATA_URL = "http://localhost:9001/api/v1/metadata/";

    private static final int OK = 200;
    private static final int NO_CONTENT = 204;
    private static final int UNAUTHORIZED_ERROR = 401;
    private static final int INTERNAL_SERVER_ERROR = 500;

    @Before
    public void setup()
            throws DatastoreException {
        datastore.setValue(SERVICE, SERVICE_KEY1, "foo", "bar");
        datastore.setValue(SERVICE, SERVICE_KEY1, "foobar", "fi");
        datastore.setValue(SERVICE, SERVICE_KEY1, "tee", "too");
        datastore.setValue(SERVICE, SERVICE_KEY2, "foo", "bar");
        final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        final Logger logger = context.getLogger(MetadataResource.class);
        logger.setLevel(Level.OFF);
    }

    @Test(expected = NullPointerException.class)
    public void test_constructor_nullDatastore_invalid() {
        new MetadataResource(null);
    }

    @Test
    public void listKeysStartsWith()
            throws Exception {
        JsonResponse response = client.get(METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "?startsWith=foo");
        assertThat(response.getStatusCode(), equalTo(OK));
        assertThat(response.getJson(), equalTo("{\"results\":[\"foo\",\"foobar\"]}"));

        response = client.get(METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "?startsWith=fi");
        assertThat(response.getStatusCode(), equalTo(OK));
        assertThat(response.getJson(), equalTo("{\"results\":[]}"));
    }

    @Test
    public void listKeys()
            throws Exception {
        final JsonResponse response = client.get(METADATA_URL + SERVICE + "/" + SERVICE_KEY1);

        assertThat(response.getStatusCode(), equalTo(OK));
        assertThat(response.getJson(), equalTo("{\"results\":[\"foo\",\"foobar\",\"tee\"]}"));
    }

    @Test
    public void listKeys_notAuthorized()
            throws IOException {
        final JsonResponse response = client.get(METADATA_URL + UNAUTHORIZED_SERVICE + "/" + SERVICE_KEY1);

        assertThat(response.getStatusCode(), equalTo(UNAUTHORIZED_ERROR));
        assertThat(response.getJson(), equalTo(""));
    }

    @Test
    public void listKeys_withException()
            throws Exception {
        datastore.throwException(new DatastoreException("expected"));

        final JsonResponse response = client.get(METADATA_URL + SERVICE + "/" + SERVICE_KEY1);

        assertThat(response.getStatusCode(), equalTo(INTERNAL_SERVER_ERROR));
        assertThat(response.getJson(), equalTo("{\"errors\":[\"expected\"]}"));
        datastore.throwException(null);
    }

    @Test
    public void listServiceKeys()
            throws Exception {
        final JsonResponse response = client.get(METADATA_URL + SERVICE);

        assertThat(response.getStatusCode(), equalTo(OK));
        assertThat(response.getJson(), equalTo("{\"results\":[\"" + SERVICE_KEY1 + "\",\"" + SERVICE_KEY2 + "\"]}"));
    }

    @Test
    public void listServiceKeys_notAuthorized()
            throws Exception {
        final JsonResponse response = client.get(METADATA_URL + UNAUTHORIZED_SERVICE);

        assertThat(response.getStatusCode(), equalTo(UNAUTHORIZED_ERROR));
        assertThat(response.getJson(), equalTo(""));
    }

    @Test
    public void getValue()
            throws Exception {
        final JsonResponse response = client.get(METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "/foobar");

        assertThat(response.getStatusCode(), equalTo(OK));
        assertThat(response.getJson(), equalTo("fi"));
    }

    @Test
    public void getValue_notAuthorized()
            throws Exception {
        final JsonResponse response = client.get(METADATA_URL + UNAUTHORIZED_SERVICE + "/" + SERVICE_KEY1 + "/foobar");

        assertThat(response.getStatusCode(), equalTo(UNAUTHORIZED_ERROR));
        assertThat(response.getJson(), equalTo(""));
    }

    @Test
    public void getValue_withException()
            throws Exception {
        datastore.throwException(new DatastoreException("expected"));

        final JsonResponse response = client.get(METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "/foobar");

        assertThat(response.getStatusCode(), equalTo(INTERNAL_SERVER_ERROR));
        assertThat(response.getJson(), equalTo("{\"errors\":[\"expected\"]}"));
        datastore.throwException(null);
    }

    @Test
    public void getValue_empty()
            throws Exception {
        final JsonResponse response = client.get(METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "/bogus");

        assertThat(response.getStatusCode(), equalTo(OK));
        assertThat(response.getJson(), equalTo(""));
    }

    @Test
    public void setValue_withException()
            throws Exception {
        datastore.throwException(new DatastoreException("expected"));

        final JsonResponse response = client.post("value", METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "/foobar");

        assertThat(response.getStatusCode(), equalTo(INTERNAL_SERVER_ERROR));
        assertThat(response.getJson(), equalTo("{\"errors\":[\"expected\"]}"));
        datastore.throwException(null);
    }

    @Test
    public void setValue_notAuthorized()
            throws Exception {
        final JsonResponse response = client.post("value", METADATA_URL + UNAUTHORIZED_SERVICE + "/" + SERVICE_KEY1 + "/foobar");

        assertThat(response.getStatusCode(), equalTo(UNAUTHORIZED_ERROR));
        assertThat(response.getJson(), equalTo(""));
    }

    @SuppressWarnings("UnusedAssignment")
    @Test
    public void deleteKey()
            throws Exception {
        JsonResponse response = client.post("newValue", METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "/newKey");
        response = client.get(METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "/newKey");
        assertThat(response.getJson(), equalTo("newValue"));

        response = client.delete(METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "/newKey");
        assertThat(response.getStatusCode(), equalTo(NO_CONTENT));

        response = client.get(METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "/newKey");
        assertThat(response.getJson(), equalTo(""));
    }

    @Test
    public void deleteKey_notAuthorized()
            throws Exception {
        final JsonResponse response = client.get(METADATA_URL + UNAUTHORIZED_SERVICE + "/" + SERVICE_KEY1 + "/newKey");

        assertThat(response.getStatusCode(), equalTo(UNAUTHORIZED_ERROR));
        assertThat(response.getJson(), equalTo(""));
    }

    @SuppressWarnings("UnusedAssignment")
    @Test
    public void deleteKey_withException()
            throws Exception {
        JsonResponse response = client.post("newValue", METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "/newKey");
        datastore.throwException(new DatastoreException("expected"));

        response = client.delete(METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "/newKey");

        assertThat(response.getStatusCode(), equalTo(INTERNAL_SERVER_ERROR));
        assertThat(response.getJson(), equalTo("{\"errors\":[\"expected\"]}"));

        // clean up
        datastore.throwException(null);
        response = client.delete(METADATA_URL + SERVICE + "/" + SERVICE_KEY1 + "/newKey");

    }
}