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

package org.kairosdb.core.http.rest;

import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.MalformedJsonException;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.json.JSONWriter;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.QueryPlugin;
import org.kairosdb.core.datastore.QueryPostProcessingPlugin;
import org.kairosdb.core.exception.InvalidServerTypeException;
import org.kairosdb.core.formatter.DataFormatter;
import org.kairosdb.core.formatter.FormatterException;
import org.kairosdb.core.formatter.JsonFormatter;
import org.kairosdb.core.formatter.JsonResponse;
import org.kairosdb.core.http.rest.json.DataPointsParser;
import org.kairosdb.core.http.rest.json.ErrorResponse;
import org.kairosdb.core.http.rest.json.JsonResponseBuilder;
import org.kairosdb.core.http.rest.json.Query;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.http.rest.json.ValidationErrors;
import org.kairosdb.core.reporting.NoTagsTagger;
import org.kairosdb.core.reporting.Tagger;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.MemoryMonitorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.Response.ResponseBuilder;

@Path("/api/v1")
public class MetricsResource {
    public static final Logger logger = LoggerFactory.getLogger(MetricsResource.class);
    public static final String QUERY_TIME = "http/query_time";
    public static final String REQUEST_TIME = "http/request_time";
    public static final String INGEST_COUNT = "http/ingest_count";
    public static final String INGEST_TIME = "http/ingest_time";

    public static final String QUERY_URL = "/datapoints/query";

    private final KairosDatastore datastore;
    private final Publisher<DataPointEvent> m_publisher;
    private final Map<String, DataFormatter> formatters = new HashMap<>();
    private final QueryParser queryParser;
    private final MetricsFactory metricsFactory;

    //Used for parsing incoming metrics
    private final Gson gson;

    //These two are used to track rate of ingestion
    private final AtomicInteger m_ingestedDataPoints = new AtomicInteger();
    private final AtomicInteger m_ingestTime = new AtomicInteger();

    private final KairosDataPointFactory m_kairosDataPointFactory;

    @Inject(optional = true)
    private final QueryPreProcessorContainer m_queryPreProcessor = new QueryPreProcessorContainer() {
        @Override
        public Query preProcess(final Query query) {
            return query;
        }
    };

    @Inject(optional = true)
    @Named("kairosdb.log.queries.enable")
    private boolean m_logQueries = false;

    @Inject(optional = true)
    @Named("kairosdb.log.queries.greater_than")
    private int m_logQueriesLongerThan = 60;

    @Inject(optional = true)
    @Named("rest.query")
    private Tagger restQueryTagger = new NoTagsTagger.Builder().build();

    //Used for setting which API methods are enabled
    private EnumSet<ServerType> m_serverType = EnumSet.of(ServerType.INGEST, ServerType.QUERY, ServerType.DELETE);

    @Inject
    public MetricsResource(
            final KairosDatastore datastore,
            final QueryParser queryParser,
            final KairosDataPointFactory dataPointFactory,
            final FilterEventBus eventBus,
            final MetricsFactory metricsFactory,
            final PeriodicMetrics periodicMetrics) {
        this.datastore = checkNotNull(datastore);
        this.queryParser = checkNotNull(queryParser);
        this.metricsFactory = checkNotNull(metricsFactory);
        m_kairosDataPointFactory = dataPointFactory;
        m_publisher = checkNotNull(eventBus).createPublisher(DataPointEvent.class);
        formatters.put("json", new JsonFormatter());

        final GsonBuilder builder = new GsonBuilder();
        gson = builder.disableHtmlEscaping().create();

        periodicMetrics.registerPolledMetric(m -> {
            final int time = m_ingestTime.getAndSet(0);
            final int count = m_ingestedDataPoints.getAndSet(0);
            if (count != 0) {
                m.recordGauge(INGEST_COUNT, count);
                m.recordGauge(INGEST_TIME, time);
            }
        });
    }

    public static ResponseBuilder setHeaders(final ResponseBuilder responseBuilder) {
        responseBuilder.header("Access-Control-Allow-Origin", "*");
        responseBuilder.header("Pragma", "no-cache");
        responseBuilder.header("Cache-Control", "no-cache");
        responseBuilder.header("Expires", 0);

        return (responseBuilder);
    }

    @VisibleForTesting
    static void checkServerTypeStatic(final EnumSet<ServerType> serverType, final ServerType methodServerType, final String methodName, final String requestType) throws InvalidServerTypeException {
        logger.debug("checkServerType() - KairosDB ServerType set to " + serverType.toString());

        if (!serverType.contains(methodServerType)) {
            final String logtext = "Disabled request type: " + methodServerType.name() + ", " + requestType + " request via URI \"" + methodName + "\"";
            logger.info(logtext);

            final String exceptionMessage = "{\"errors\": [\"Forbidden: " + methodServerType.toString() + " API methods are disabled on this KairosDB instance.\"]}";

            throw new InvalidServerTypeException(exceptionMessage);
        }
    }

    public static ResponseBuilder getCorsPreflightResponseBuilder(final String requestHeaders,
                                                                  final String requestMethod) {
        final ResponseBuilder responseBuilder = Response.status(Response.Status.OK);
        responseBuilder.header("Access-Control-Allow-Origin", "*");
        responseBuilder.header("Access-Control-Allow-Headers", requestHeaders);
        responseBuilder.header("Access-Control-Max-Age", "86400"); // Cache for one day
        if (requestMethod != null) {
            responseBuilder.header("Access-Control-Allow_Method", requestMethod);
        }

        return responseBuilder;
    }

    @Inject(optional = true)
    @VisibleForTesting
    void setServerType(@Named("kairosdb.server.type") final String serverType) {
        if (serverType.equals("ALL")) return;
        final String serverTypeString = serverType.replaceAll("\\s+", "");
        final String[] serverTypeList = serverTypeString.split(",");

        m_serverType = EnumSet.noneOf(ServerType.class);

        for (final String stString : serverTypeList) {
            m_serverType.add(ServerType.valueOf(stString));
        }

        logger.info("KairosDB server type set to: " + m_serverType.toString());
    }

    private void checkServerType(final ServerType methodServerType, final String methodName, final String requestType) throws InvalidServerTypeException {
        checkServerTypeStatic(m_serverType, methodServerType, methodName, requestType);
    }

    @OPTIONS
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/version")
    public Response corsPreflightVersion(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
                                         @HeaderParam("Access-Control-Request-Method") final String requestMethod) {
        final ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
        return (responseBuilder.build());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/version")
    public Response getVersion() {
        final Package thisPackage = getClass().getPackage();
        final String versionString = thisPackage.getImplementationTitle() + " " + thisPackage.getImplementationVersion();
        final ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity("{\"version\": \"" + versionString + "\"}\n");
        setHeaders(responseBuilder);
        return responseBuilder.build();
    }

    @OPTIONS
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/metricnames")
    public Response corsPreflightMetricNames(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
                                             @HeaderParam("Access-Control-Request-Method") final String requestMethod) throws InvalidServerTypeException {
        checkServerType(ServerType.QUERY, "/metricnames", "OPTIONS");
        final ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
        return (responseBuilder.build());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/metricnames")
    public Response getMetricNames(@QueryParam("prefix") final String prefix) throws InvalidServerTypeException {
        checkServerType(ServerType.QUERY, "/metricnames", "GET");

        return executeNameQuery(NameType.METRIC_NAMES, prefix);
    }

    @OPTIONS
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/datapoints")
    public Response corsPreflightDataPoints(@HeaderParam("Access-Control-Request-Headers") String requestHeaders,
                                            @HeaderParam("Access-Control-Request-Method") String requestMethod) throws InvalidServerTypeException {
        checkServerType(ServerType.INGEST, "/datapoints", "OPTIONS");
        ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
        return (responseBuilder.build());
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Consumes("application/gzip")
    @Path("/datapoints")
    public Response addGzip(final InputStream gzip) throws InvalidServerTypeException {
        checkServerType(ServerType.INGEST, "gzip /datapoints", "POST");
        final GZIPInputStream gzipInputStream;
        try {
            gzipInputStream = new GZIPInputStream(gzip);
        } catch (final IOException e) {
            final JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            return builder.addError(e.getMessage()).build();
        }
        return (add(gzipInputStream));
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/datapoints")
    public Response add(final InputStream json) throws InvalidServerTypeException {
        checkServerType(ServerType.INGEST, "JSON /datapoints", "POST");
        try {
            final DataPointsParser parser = new DataPointsParser(m_publisher, new InputStreamReader(json, StandardCharsets.UTF_8),
                    gson, m_kairosDataPointFactory);
            final ValidationErrors validationErrors = parser.parse();

            m_ingestedDataPoints.addAndGet(parser.getDataPointCount());
            m_ingestTime.addAndGet(parser.getIngestTime());

            if (!validationErrors.hasErrors()) {
                return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
            } else {
                final JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
                for (final String errorMessage : validationErrors.getErrors()) {
                    builder.addError(errorMessage);
                }
                return builder.build();
            }
        } catch (final JsonIOException | MalformedJsonException | JsonSyntaxException e) {
            final JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            return builder.addError(e.getMessage()).build();
        } catch (final Exception e) {
            logger.error("Failed to add metric.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();

        } catch (final OutOfMemoryError e) {
            logger.error("Out of memory error.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        }
    }

    @OPTIONS
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/datapoints/query/tags")
    public Response corsPreflightQueryTags(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
                                           @HeaderParam("Access-Control-Request-Method") final String requestMethod) throws InvalidServerTypeException {
        checkServerType(ServerType.QUERY, "/datapoints/query/tags", "OPTIONS");
        final ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
        return (responseBuilder.build());
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/datapoints/query/tags")
    public Response getMeta(final String json) throws InvalidServerTypeException {
        checkServerType(ServerType.QUERY, "/datapoints/query/tags", "POST");
        checkNotNull(json);
        logger.debug(json);

        try {
            final File respFile = File.createTempFile("kairos", ".json", new File(datastore.getCacheDir()));
            final BufferedWriter writer = new BufferedWriter(new FileWriter(respFile));

            final JsonResponse jsonResponse = new JsonResponse(writer);

            jsonResponse.begin();

            final List<QueryMetric> queries = queryParser.parseQueryMetric(json).getQueryMetrics();

            for (final QueryMetric query : queries) {
                final List<DataPointGroup> result = datastore.queryTags(query);

                try {
                    jsonResponse.formatQuery(result, false, -1);
                } finally {
                    for (final DataPointGroup dataPointGroup : result) {
                        dataPointGroup.close();
                    }
                }
            }

            jsonResponse.end();
            writer.flush();
            writer.close();

            final ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(
                    new FileStreamingOutput(respFile));

            setHeaders(responseBuilder);
            return responseBuilder.build();
        } catch (final JsonSyntaxException | QueryException e) {
            final JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            return builder.addError(e.getMessage()).build();
        } catch (final BeanValidationException e) {
            final JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            return builder.addErrors(e.getErrorMessages()).build();
        } catch (final MemoryMonitorException e) {
            logger.error("Query failed.", e);
            System.gc();
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        } catch (final Exception e) {
            logger.error("Query failed.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();

        } catch (final OutOfMemoryError e) {
            logger.error("Out of memory error.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/datapoints/query/cardinality")
    public Response getCardinality(final String json) throws InvalidServerTypeException {
        checkServerType(ServerType.QUERY, "/datapoints/query/tags", "POST");
        checkNotNull(json);
        logger.debug(json);

        try {
            final StringWriter writer = new StringWriter();
            final JSONWriter jWriter = new JSONWriter(writer);
            jWriter.object().key("queryCardinalities").array();

            final List<QueryMetric> queries = queryParser.parseQueryMetric(json).getQueryMetrics();

            for (final QueryMetric query : queries) {
                final long result = datastore.queryCardinality(query);
                jWriter.value(result);
            }

            jWriter.endArray().endObject();
            writer.flush();
            writer.close();

            final ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(writer.toString());

            setHeaders(responseBuilder);
            return responseBuilder.build();
        } catch (final JsonSyntaxException | QueryException e) {
            final JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            return builder.addError(e.getMessage()).build();
        } catch (final BeanValidationException e) {
            final JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            return builder.addErrors(e.getErrorMessages()).build();
        } catch (final MemoryMonitorException e) {
            logger.error("Query failed.", e);
            System.gc();
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        } catch (final Exception e) {
            logger.error("Query failed.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();

        } catch (final OutOfMemoryError e) {
            logger.error("Out of memory error.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        }
    }

    /**
     * Information for this endpoint was taken from https://developer.mozilla.org/en-US/docs/HTTP/Access_control_CORS.
     * <p>
     * <p>Response to a cors preflight request to access data.
     */
    @OPTIONS
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path(QUERY_URL)
    public Response corsPreflightQuery(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
                                       @HeaderParam("Access-Control-Request-Method") final String requestMethod) throws InvalidServerTypeException {
        checkServerType(ServerType.QUERY, QUERY_URL, "OPTIONS");
        final ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
        return (responseBuilder.build());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path(QUERY_URL)
    public Response getQuery(@QueryParam("query") final String json, @Context final HttpServletRequest request) throws Exception {
        checkServerType(ServerType.QUERY, QUERY_URL, "GET");
        return runQuery(json, request.getRemoteAddr());
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path(QUERY_URL)
    public Response postQuery(final String json, @Context final HttpServletRequest request) throws Exception {
        checkServerType(ServerType.QUERY, QUERY_URL, "POST");
        return runQuery(json, request.getRemoteAddr());
    }

    public Response runQuery(final String json, final String remoteAddr) throws Exception {
        logger.debug(json);
        boolean queryFailed = false;

        final long startRunQuery = System.currentTimeMillis();

        try {
            if (json == null)
                throw new BeanValidationException(new QueryParser.SimpleConstraintViolation("query json", "must not be null or empty"), "");

            File respFile = File.createTempFile("kairos", ".json", new File(datastore.getCacheDir()));
            final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(respFile), StandardCharsets.UTF_8));

            final JsonResponse jsonResponse = new JsonResponse(writer);

            jsonResponse.begin();

            Query mainQuery = queryParser.parseQueryMetric(json);
            mainQuery = m_queryPreProcessor.preProcess(mainQuery);

            final List<QueryMetric> queries = mainQuery.getQueryMetrics();

            for (final QueryMetric query : queries) {
                ThreadReporter.initialize(metricsFactory);
                restQueryTagger.applyTagsToThreadReporter(query::getName, query::getTags);

                final DatastoreQuery dq = datastore.createQuery(query);
                final long startQuery = System.currentTimeMillis();

                try {
                    final List<DataPointGroup> results = dq.execute();
                    jsonResponse.formatQuery(results, query.isExcludeTags(), dq.getSampleSize());

                    ThreadReporter.addDataPoint(QUERY_TIME, System.currentTimeMillis() - startQuery);
                } finally {
                    dq.close();
                    ThreadReporter.close();
                }
            }

            jsonResponse.end();
            writer.flush();
            writer.close();


            //System.out.println("About to process plugins");
            final List<QueryPlugin> plugins = mainQuery.getPlugins();
            for (final QueryPlugin plugin : plugins) {
                if (plugin instanceof QueryPostProcessingPlugin) {
                    respFile = ((QueryPostProcessingPlugin) plugin).processQueryResults(respFile);
                }
            }

            final ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(
                    new FileStreamingOutput(respFile));

            setHeaders(responseBuilder);
            return responseBuilder.build();
        } catch (final JsonSyntaxException | QueryException e) {
            queryFailed = true;
            final JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            return builder.addError(e.getMessage()).build();
        } catch (final BeanValidationException e) {
            queryFailed = true;
            final JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            return builder.addErrors(e.getErrorMessages()).build();
        } catch (final MemoryMonitorException e) {
            queryFailed = true;
            logger.error("Query failed.", e);
            Thread.sleep(1000);
            System.gc();
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        } catch (final IOException e) {
            queryFailed = true;
            logger.error("Failed to open temp folder " + datastore.getCacheDir(), e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        } catch (final Exception e) {
            queryFailed = true;
            logger.error("Query failed.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        } catch (final OutOfMemoryError e) {
            queryFailed = true;
            logger.error("Out of memory error.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();

        } finally {
            final long queryTime = System.currentTimeMillis() - startRunQuery;

            try (final Metrics metrics = metricsFactory.create()) {
                metrics.addAnnotation("status", queryFailed ? "failed" : "success");
                metrics.setGauge(REQUEST_TIME, queryTime);
            }

            // Write slow queries to log file if configured to do so
            if (m_logQueries && ((queryTime / 1000) >= m_logQueriesLongerThan)) {
                logger.warn(
                        String.format(
                                "Slow query: %s, remote: %s, query time ms: %d",
                                json,
                                remoteAddr,
                                queryTime));
            }
        }
    }

    @OPTIONS
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/datapoints/delete")
    public Response corsPreflightDelete(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
                                        @HeaderParam("Access-Control-Request-Method") final String requestMethod) throws InvalidServerTypeException {
        checkServerType(ServerType.DELETE, "/datapoints/delete", "OPTIONS");
        final ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
        return (responseBuilder.build());
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/datapoints/delete")
    public Response delete(final String json) throws Exception {
        checkServerType(ServerType.DELETE, "/datapoints/delete", "POST");
        checkNotNull(json);
        logger.debug(json);

        try {
            final List<QueryMetric> queries = queryParser.parseQueryMetric(json).getQueryMetrics();

            for (final QueryMetric query : queries) {
                datastore.delete(query);
            }

            return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
        } catch (final JsonSyntaxException | QueryException e) {
            final JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            return builder.addError(e.getMessage()).build();
        } catch (final BeanValidationException e) {
            final JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            return builder.addErrors(e.getErrorMessages()).build();
        } catch (final MemoryMonitorException e) {
            logger.error("Query failed.", e);
            System.gc();
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        } catch (final Exception e) {
            logger.error("Delete failed.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();

        } catch (final OutOfMemoryError e) {
            logger.error("Out of memory error.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        }
    }

    @OPTIONS
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/metric/{metricName}")
    public Response corsPreflightMetricDelete(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
                                              @HeaderParam("Access-Control-Request-Method") final String requestMethod) throws InvalidServerTypeException {
        checkServerType(ServerType.DELETE, "/metric/{metricName}", "OPTIONS");
        final ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
        return (responseBuilder.build());
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/metric/{metricName}")
    public Response metricDelete(@PathParam("metricName") final String metricName) throws Exception {
        checkServerType(ServerType.DELETE, "/metric/{metricName}", "DELETE");
        try {
            final QueryMetric query = new QueryMetric(Long.MIN_VALUE, Long.MAX_VALUE, 0, metricName);
            datastore.delete(query);


            return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
        } catch (final Exception e) {
            logger.error("Delete failed.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        }
    }

    private Response executeNameQuery(final NameType type) {
        return executeNameQuery(type, null);
    }

    private Response executeNameQuery(final NameType type, final String prefix) {
        try {
            Iterable<String> values = null;
            switch (type) {
                case METRIC_NAMES:
                    values = datastore.getMetricNames(prefix);
                    break;
                case TAG_KEYS:
                    values = datastore.getTagNames();
                    break;
                case TAG_VALUES:
                    values = datastore.getTagValues();
                    break;
            }

            final DataFormatter formatter = formatters.get("json");

            final ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(
                    new ValuesStreamingOutput(formatter, values));
            setHeaders(responseBuilder);
            return responseBuilder.build();
        } catch (final Exception e) {
            logger.error("Failed to get " + type, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(
                    new ErrorResponse(e.getMessage())).build();
        }
    }

    enum ServerType {
        INGEST,
        QUERY,
        DELETE
    }

    enum NameType {
        METRIC_NAMES,
        TAG_KEYS,
        TAG_VALUES
    }

    public static class ValuesStreamingOutput implements StreamingOutput {
        private final DataFormatter m_formatter;
        private final Iterable<String> m_values;

        public ValuesStreamingOutput(final DataFormatter formatter, final Iterable<String> values) {
            m_formatter = formatter;
            m_values = values;
        }

        @SuppressWarnings("ResultOfMethodCallIgnored")
        public void write(final OutputStream output) throws IOException, WebApplicationException {
            final Writer writer = new OutputStreamWriter(output, StandardCharsets.UTF_8);

            try {
                m_formatter.format(writer, m_values);
            } catch (final FormatterException e) {
                logger.error("Description of what failed:", e);
            }

            writer.flush();
        }
    }

    public static class FileStreamingOutput implements StreamingOutput {
        private final File m_responseFile;

        public FileStreamingOutput(final File responseFile) {
            m_responseFile = responseFile;
        }

        @SuppressWarnings("ResultOfMethodCallIgnored")
        @Override
        public void write(final OutputStream output) throws IOException, WebApplicationException {
            try (final InputStream reader = new FileInputStream(m_responseFile)) {
                final byte[] buffer = new byte[1024];
                int size;

                while ((size = reader.read(buffer)) != -1) {
                    output.write(buffer, 0, size);
                }

                output.flush();
            } finally {
                m_responseFile.delete();
            }
        }
    }
}
