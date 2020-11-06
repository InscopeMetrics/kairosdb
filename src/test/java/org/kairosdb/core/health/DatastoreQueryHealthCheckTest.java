package org.kairosdb.core.health;

import com.google.common.collect.Sets;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;

import static com.codahale.metrics.health.HealthCheck.Result;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatastoreQueryHealthCheckTest {

    private KairosDatastore datastore;
    private DatastoreQuery query;

    @Before
    public void setup() throws DatastoreException {
        datastore = mock(KairosDatastore.class);
        query = mock(DatastoreQuery.class);
        when(datastore.createQuery(any(QueryMetric.class))).thenReturn(query);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullDatastoreInvalid() {
        new DatastoreQueryHealthCheck(null, "foo/bar", null);
    }

    @Test(expected = RuntimeException.class)
    public void testConstructorTagsInvalid() {
        new DatastoreQueryHealthCheck(null, "foo/bar", "foo,1,bar");
    }

    @Test
    public void testCheckHealthyNoTagsNull() throws Exception {
        final DatastoreQueryHealthCheck healthCheck = new DatastoreQueryHealthCheck(
                datastore,
                "foo/bar",
                null);
        final Result result = healthCheck.check();
        final ArgumentCaptor<QueryMetric> captor = ArgumentCaptor.forClass(QueryMetric.class);
        verify(datastore).createQuery(captor.capture());
        assertTrue(captor.getValue().getTags().isEmpty());
        assertTrue(result.isHealthy());
    }

    @Test
    public void testCheckHealthyNoTagsEmpty() throws Exception {
        final DatastoreQueryHealthCheck healthCheck = new DatastoreQueryHealthCheck(
                datastore,
                "foo/bar",
                "");
        final Result result = healthCheck.check();
        final ArgumentCaptor<QueryMetric> captor = ArgumentCaptor.forClass(QueryMetric.class);
        verify(datastore).createQuery(captor.capture());
        assertTrue(captor.getValue().getTags().isEmpty());
        assertTrue(result.isHealthy());
    }

    @Test
    public void testCheckHealthyWithTags() throws Exception {
        final DatastoreQueryHealthCheck healthCheck = new DatastoreQueryHealthCheck(
                datastore,
                "foo/bar",
                "abc,123,def,456,def,789");
        final Result result = healthCheck.check();
        final ArgumentCaptor<QueryMetric> captor = ArgumentCaptor.forClass(QueryMetric.class);
        when(datastore.createQuery(any(QueryMetric.class))).thenReturn(query);
        verify(datastore).createQuery(captor.capture());
        assertEquals(3, captor.getValue().getTags().size());
        assertEquals(Collections.singleton("123"), captor.getValue().getTags().get("abc"));
        assertEquals(Sets.newHashSet(Arrays.asList("456", "789")), captor.getValue().getTags().get("def"));
        assertTrue(result.isHealthy());
    }

    @Test
    public void testCheckUnHealthy() throws Exception {
        final DatastoreQueryHealthCheck healthCheck = new DatastoreQueryHealthCheck(
                datastore,
                "foo/bar",
                null);

        final Exception exception = new DatastoreException("Error message");
        when(query.execute()).thenThrow(exception);

        final Result result = healthCheck.check();

        assertFalse(result.isHealthy());
        assertThat(result.getError(), CoreMatchers.equalTo(exception));
        assertThat(result.getMessage(), equalTo(exception.getMessage()));
    }

    @Test
    public void testGetName() {
        final DatastoreQueryHealthCheck healthCheck = new DatastoreQueryHealthCheck(
                datastore,
                "foo/bar",
                null);
        assertThat(healthCheck.getName(), equalTo(DatastoreQueryHealthCheck.NAME));
    }
}