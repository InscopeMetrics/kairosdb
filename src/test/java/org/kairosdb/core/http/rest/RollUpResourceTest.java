package org.kairosdb.core.http.rest;

import ch.qos.logback.classic.Level;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.gson.GsonBuilder;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.KairosFeatureProcessor;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.groupby.TestGroupByFactory;
import org.kairosdb.core.http.rest.json.ErrorResponse;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.http.rest.json.RollupResponse;
import org.kairosdb.core.http.rest.json.TestQueryPluginFactory;
import org.kairosdb.rollup.RollUpException;
import org.kairosdb.rollup.RollUpTasksStore;
import org.kairosdb.rollup.RollupTask;
import org.kairosdb.rollup.RollupTaskStatusStore;
import org.kairosdb.util.LoggingUtils;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.OK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RollUpResourceTest {
    private static final String BEAN_VALIDATION_ERROR = "bean validation error";
    private static final String CONTEXT = "context";
    private static final String INTERNAL_EXCEPTION_MESSAGE = "Internal Exception";

    private RollUpResource resource;
    private RollUpTasksStore mockStore;
    private RollupTaskStatusStore mockStatusStore;
    private QueryParser mockQueryParser;
    private QueryParser queryParser;

    @Before
    public void setup() throws KairosDBException {
        mockStore = mock(RollUpTasksStore.class);
        mockQueryParser = mock(QueryParser.class);
        mockStatusStore = mock(RollupTaskStatusStore.class);
        queryParser = new QueryParser(new KairosFeatureProcessor(new TestAggregatorFactory(), new TestGroupByFactory()),
                new TestQueryPluginFactory());
        resource = new RollUpResource(mockQueryParser, mockStore, mockStatusStore);
    }

    @Test(expected = NullPointerException.class)
    public void testCreate_nullJsonInvalid() {
        resource.create(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreate_emptyJsonInvalid() {
        resource.create("");
    }

    @Test
    public void testCreate_parseError() throws IOException, QueryException {
        when(mockQueryParser.parseRollupTask(anyString())).thenThrow(createBeanException());

        final Response response = resource.create("thejson");

        final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
        assertThat(response.getStatus(), equalTo(BAD_REQUEST.getStatusCode()));
        assertThat(errorResponse.getErrors().size(), equalTo(1));
        assertThat(errorResponse.getErrors().get(0), equalTo(getBeanValidationMessage()));
    }

    @Test
    public void testCreate_internalError() throws IOException, QueryException {
        final Level previousLogLevel = LoggingUtils.setLogLevel(Level.OFF);
        try {
            when(mockQueryParser.parseRollupTask(anyString())).thenThrow(createQueryException());

            final Response response = resource.create("thejson");

            final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
            assertThat(response.getStatus(), equalTo(INTERNAL_SERVER_ERROR.getStatusCode()));
            assertThat(errorResponse.getErrors().size(), equalTo(1));
            assertThat(errorResponse.getErrors().get(0), equalTo(INTERNAL_EXCEPTION_MESSAGE));
        } finally {
            LoggingUtils.setLogLevel(previousLogLevel);
        }
    }

    @Test
    public void testCreate() throws IOException, QueryException {
        resource = new RollUpResource(queryParser, mockStore, mockStatusStore);
        final String json = Resources.toString(Resources.getResource("rolluptask1.json"), Charsets.UTF_8);
        final RollupTask task = queryParser.parseRollupTask(json);

        final Response response = resource.create(json);

        assertThat(response.getStatus(), equalTo(OK.getStatusCode()));
        assertRollupResponse((String) response.getEntity(), task);
    }

    @Test
    public void testList() throws IOException, QueryException, RollUpException {
        resource = new RollUpResource(queryParser, mockStore, mockStatusStore);
        final List<RollupTask> tasks = mockTasks(Resources.toString(Resources.getResource("rolluptasks.json"), Charsets.UTF_8));

        final Response response = resource.list();

        final List<RollupTask> responseTasks = queryParser.parseRollupTasks((String) response.getEntity());
        assertThat(response.getStatus(), equalTo(OK.getStatusCode()));
        assertThat(responseTasks, containsInAnyOrder(tasks.toArray()));
    }

    @Test
    public void testList_internalError() throws IOException, QueryException, RollUpException {
        final Level previousLogLevel = LoggingUtils.setLogLevel(Level.OFF);
        try {
            when(mockStore.read()).thenThrow(createRollupException());

            final Response response = resource.list();

            final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
            assertThat(response.getStatus(), equalTo(INTERNAL_SERVER_ERROR.getStatusCode()));
            assertThat(errorResponse.getErrors().size(), equalTo(1));
            assertThat(errorResponse.getErrors().get(0), equalTo(INTERNAL_EXCEPTION_MESSAGE));
        } finally {
            LoggingUtils.setLogLevel(previousLogLevel);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testGet_nullIdInvalid() {
        resource.get(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGet_emptyIdInvalid() {
        resource.get("");
    }

    @Test
    public void testGet() throws IOException, QueryException, RollUpException {
        resource = new RollUpResource(queryParser, mockStore, mockStatusStore);
        final String json = Resources.toString(Resources.getResource("rolluptasks.json"), Charsets.UTF_8);
        final List<RollupTask> tasks = mockTasks(json);

        final Response response = resource.get(tasks.get(1).getId());

        final RollupTask responseTask = queryParser.parseRollupTask((String) response.getEntity());
        assertThat(response.getStatus(), equalTo(OK.getStatusCode()));
        assertThat(responseTask, equalTo(tasks.get(1)));
    }

    @Test
    public void testGet_taskDoesNotExist() throws IOException, QueryException, RollUpException {
        resource = new RollUpResource(queryParser, mockStore, mockStatusStore);
        final String json = Resources.toString(Resources.getResource("rolluptasks.json"), Charsets.UTF_8);
        mockTasks(json);

        final Response response = resource.get("bogus");

        final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
        assertThat(response.getStatus(), equalTo(NOT_FOUND.getStatusCode()));
        assertThat(errorResponse.getErrors().get(0), equalTo("Resource not found for id bogus"));
    }

    @Test
    public void testGet_internalError() throws IOException, QueryException, RollUpException {
        final Level previousLogLevel = LoggingUtils.setLogLevel(Level.OFF);
        try {
            when(mockStore.read(anyString())).thenThrow(createRollupException());

            final Response response = resource.get("1");

            final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
            assertThat(response.getStatus(), equalTo(INTERNAL_SERVER_ERROR.getStatusCode()));
            assertThat(errorResponse.getErrors().size(), equalTo(1));
            assertThat(errorResponse.getErrors().get(0), equalTo(INTERNAL_EXCEPTION_MESSAGE));
        } finally {
            LoggingUtils.setLogLevel(previousLogLevel);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testDelete_nullIdInvalid() {
        resource.delete(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDelete_emptyIdInvalid() {
        resource.delete("");
    }

    @Test
    public void testDelete() throws IOException, QueryException, RollUpException {
        final String json = Resources.toString(Resources.getResource("rolluptasks.json"), Charsets.UTF_8);
        final List<RollupTask> tasks = mockTasks(json);
        resource = new RollUpResource(queryParser, mockStore, mockStatusStore);

        final Response response = resource.delete(tasks.get(0).getId());

        assertThat(response.getStatus(), equalTo(NO_CONTENT.getStatusCode()));
        assertNull(response.getEntity());
    }

    @Test
    public void testDelete_internalError() throws IOException, QueryException, RollUpException {
        final Level previousLogLevel = LoggingUtils.setLogLevel(Level.OFF);
        try {
            final String json = Resources.toString(Resources.getResource("rolluptasks.json"), Charsets.UTF_8);
            final List<RollupTask> tasks = mockTasks(json);
            doThrow(createRollupException()).when(mockStore).remove(anyString());

            final Response response = resource.delete(tasks.get(0).getId());

            final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
            assertThat(response.getStatus(), equalTo(INTERNAL_SERVER_ERROR.getStatusCode()));
            assertThat(errorResponse.getErrors().size(), equalTo(1));
            assertThat(errorResponse.getErrors().get(0), equalTo(INTERNAL_EXCEPTION_MESSAGE));
        } finally {
            LoggingUtils.setLogLevel(previousLogLevel);
        }
    }

    @Test
    public void testDelete_resourceNotExists() throws IOException, QueryException, RollUpException {
        when(mockStore.read()).thenReturn(ImmutableMap.of());

        final Response response = resource.delete("1");

        final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
        assertThat(response.getStatus(), equalTo(NOT_FOUND.getStatusCode()));
        assertThat(errorResponse.getErrors().size(), equalTo(1));
        assertThat(errorResponse.getErrors().get(0), equalTo("Resource not found for id 1"));
    }

    @Test(expected = NullPointerException.class)
    public void testUpdate_nullIdInvalid() {
        resource.update(null, "json");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdate_emptyIdInvalid() {
        resource.update("", "json");
    }

    @Test(expected = NullPointerException.class)
    public void testUpdate_nullJsonInvalid() {
        resource.update("id", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdate_emptyJsonInvalid() {
        resource.update("id", "");
    }

    @Test
    public void testUpdate() throws IOException, QueryException, RollUpException {
        resource = new RollUpResource(queryParser, mockStore, mockStatusStore);
        final String json = Resources.toString(Resources.getResource("rolluptasksExisting.json"), Charsets.UTF_8);
        final List<RollupTask> tasks = mockTasks(json);

        // Replace task 1 with task 2
        final Response response = resource.update(tasks.get(0).getId(), tasks.get(1).getJson());

        @SuppressWarnings("unchecked") final Class<ArrayList<RollupTask>> listClass = (Class<ArrayList<RollupTask>>) (Class) ArrayList.class;
        final ArgumentCaptor<ArrayList<RollupTask>> captor = ArgumentCaptor.forClass(listClass);

        verify(mockStore, times(1)).write(captor.capture());
        final List<RollupTask> modifiedTasks = captor.getValue();
        assertThat(response.getStatus(), equalTo(OK.getStatusCode()));
        assertThat(modifiedTasks.size(), equalTo(1));

        final RollupTask modifiedTask = modifiedTasks.get(0);
        assertThat(modifiedTask.getId(), equalTo(tasks.get(0).getId()));
        assertThat(modifiedTask.getName(), equalTo(tasks.get(1).getName()));
        assertThat(response.getStatus(), equalTo(200));
		assertRollupResponse((String)response.getEntity(), modifiedTasks.get(0));

		// since the id is stored in the json, verify that the id has not changed
		assertThat(new GsonBuilder().create().fromJson(modifiedTask.getJson(), RollupTask.class).getId(), equalTo(tasks.get(0).getId()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpdate_internalError() throws IOException, QueryException, RollUpException {
        final Level previousLogLevel = LoggingUtils.setLogLevel(Level.OFF);
        try {
            resource = new RollUpResource(queryParser, mockStore, mockStatusStore);
            final String json = Resources.toString(Resources.getResource("rolluptasks.json"), Charsets.UTF_8);
            final List<RollupTask> tasks = mockTasks(json);
            //noinspection unchecked
            doThrow(createRollupException()).when(mockStore).write(any());

            final Response response = resource.update(tasks.get(0).getId(), tasks.get(0).getJson());

            final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
            assertThat(response.getStatus(), equalTo(INTERNAL_SERVER_ERROR.getStatusCode()));
            assertThat(errorResponse.getErrors().size(), equalTo(1));
            assertThat(errorResponse.getErrors().get(0), equalTo(INTERNAL_EXCEPTION_MESSAGE));
        } finally {
            LoggingUtils.setLogLevel(previousLogLevel);
        }
    }

    @Test
    public void testUpdate_resourceNotExists() throws IOException, QueryException, RollUpException {
        when(mockStore.read()).thenReturn(ImmutableMap.of());

        final Response response = resource.update("1", "json");

        final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
        assertThat(response.getStatus(), equalTo(NOT_FOUND.getStatusCode()));
        assertThat(errorResponse.getErrors().size(), equalTo(1));
        assertThat(errorResponse.getErrors().get(0), equalTo("Resource not found for id 1"));
    }


    private String getBeanValidationMessage() {
        return CONTEXT + " " + BEAN_VALIDATION_ERROR;
    }

    private BeanValidationException createBeanException() throws BeanValidationException {
        return new BeanValidationException(new QueryParser.SimpleConstraintViolation(CONTEXT, BEAN_VALIDATION_ERROR), "");
    }

    private Exception createQueryException() {
        return new QueryException(INTERNAL_EXCEPTION_MESSAGE);
    }

    private Exception createRollupException() {
        return new RollUpException(INTERNAL_EXCEPTION_MESSAGE);
    }

    private void assertRollupResponse(final String expected, final RollupTask actual) {
        final RollupResponse rollupResponse = new GsonBuilder().create().fromJson(expected, RollupResponse.class);
        assertThat(rollupResponse.getId(), not(isEmptyOrNullString()));
        assertThat(rollupResponse.getName(), equalTo(actual.getName()));
        assertThat(rollupResponse.getAttributes().get("url"), equalTo(RollUpResource.RESOURCE_URL + rollupResponse.getId()));
    }

    private List<RollupTask> mockTasks(final String json)
            throws BeanValidationException, QueryException, RollUpException {
        final List<RollupTask> tasks = queryParser.parseRollupTasks(json);
        final Map<String, RollupTask> taskMap = new HashMap<>();
        for (final RollupTask task : tasks) {
            taskMap.put(task.getId(), task);
        }
        when(mockStore.read()).thenReturn(taskMap);

        for (final RollupTask task : tasks) {
            when(mockStore.read(task.getId())).thenReturn(task);
        }
        return tasks;
    }

}
