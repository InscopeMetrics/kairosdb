package org.kairosdb.core.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.http.rest.MetricsResource;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.core.http.rest.MetricsResource.setHeaders;

/**
 * Provides REST APIs for health checks
 */
@Path("/api/v1/health")
public class HealthCheckResource {
    private final HealthCheckService m_healthCheckService;

    @Inject
    @Named("kairosdb.health.healthyResponseCode")
    private int m_healthyResponse = Response.Status.NO_CONTENT.getStatusCode();

    @Inject
    public HealthCheckResource(final HealthCheckService healthCheckService) {
        m_healthCheckService = checkNotNull(healthCheckService);
    }

    @OPTIONS
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("check")
    public Response corsPreflightCheck(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
                                       @HeaderParam("Access-Control-Request-Method") final String requestMethod) {
        final Response.ResponseBuilder responseBuilder = MetricsResource.getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
        return (responseBuilder.build());
    }

    /**
     * Health check
     *
     * @return 204 if healthy otherwise 500
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("check")
    public Response check() {
        for (final HealthStatus healthCheck : m_healthCheckService.getChecks()) {
            final HealthCheck.Result result = healthCheck.execute();
            if (!result.isHealthy()) {
                return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR)).build();
            }
        }

        return setHeaders(Response.status(m_healthyResponse)).build();
    }


    @OPTIONS
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("status")
    public Response corsPreflightStatus(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
                                        @HeaderParam("Access-Control-Request-Method") final String requestMethod) {
        final Response.ResponseBuilder responseBuilder = MetricsResource.getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
        return (responseBuilder.build());
    }


    /**
     * Returns the status of each health check.
     *
     * @return 200
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("status")
    public Response status() {
        final List<String> messages = new ArrayList<String>();
        for (final HealthStatus healthCheck : m_healthCheckService.getChecks()) {
            final HealthCheck.Result result = healthCheck.execute();
            if (result.isHealthy()) {
                messages.add(healthCheck.getName() + ": OK");
            } else {
                messages.add(healthCheck.getName() + ": FAIL");
            }
        }

        final GenericEntity<List<String>> entity = new GenericEntity<List<String>>(messages) {
        };
        return setHeaders(Response.ok(entity)).build();
    }

}
