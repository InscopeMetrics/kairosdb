package org.kairosdb.core.http.rest;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.formatter.JsonFormatter;
import org.kairosdb.core.http.rest.MetricsResource.ValuesStreamingOutput;
import org.kairosdb.core.http.rest.json.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.core.http.rest.MetricsResource.setHeaders;

@Path("/api/v1/metadata")
public class MetadataResource {
    private static final Logger logger = LoggerFactory.getLogger(MetadataResource.class);

    private final ServiceKeyStore m_keyStore;
    private final JsonFormatter jsonFormatter = new JsonFormatter();

    @SuppressWarnings("ConstantConditions")
    @Inject
    public MetadataResource(final ServiceKeyStore keyStore) {
        this.m_keyStore = checkNotNull(keyStore, "m_keyStore cannot be null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/{service}")
    public Response listServiceKeys(@PathParam("service") final String service) {
        try {
            checkLocalService(service);
            final Iterable<String> keys = m_keyStore.listServiceKeys(service);
            final ResponseBuilder responseBuilder = Response.status(Status.OK).entity(
                    new ValuesStreamingOutput(jsonFormatter, keys));
            setHeaders(responseBuilder);
            return responseBuilder.build();
        } catch (final NotAuthorizedException e) {
            logger.error("Attempt to access a local service.");
            return setHeaders(Response.status(Status.UNAUTHORIZED)).build();
        } catch (final Exception e) {
            logger.error("Failed to get keys.", e);
            return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/{service}/{serviceKey}")
    public Response listKeys(@PathParam("service") final String service,
                             @PathParam("serviceKey") final String serviceKey, @QueryParam("startsWith") final String startsWidth) {
        try {
            checkLocalService(service);
            final Iterable<String> keys;
            keys = Strings.isNullOrEmpty(startsWidth) ?
                    m_keyStore.listKeys(service, serviceKey) :
                    m_keyStore.listKeys(service, serviceKey, startsWidth);

            final ResponseBuilder responseBuilder = Response.status(Status.OK).entity(
                    new ValuesStreamingOutput(jsonFormatter, keys));
            setHeaders(responseBuilder);
            return responseBuilder.build();
        } catch (final NotAuthorizedException e) {
            logger.error("Attempt to access a local service.");
            return setHeaders(Response.status(Status.UNAUTHORIZED)).build();
        } catch (final Exception e) {
            logger.error("Failed to get keys.", e);
            return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/{service}/{serviceKey}/{key}")
    public Response getValue(@PathParam("service") final String service, @PathParam("serviceKey") final
    String serviceKey, @PathParam("key") final String key) {
        try {
            checkLocalService(service);
            final String value = m_keyStore.getValue(service, serviceKey, key).getValue();
            final ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(value);
            setHeaders(responseBuilder);
            return responseBuilder.build();
        } catch (final NotAuthorizedException e) {
            logger.error("Attempt to access a local service.");
            return setHeaders(Response.status(Status.UNAUTHORIZED)).build();
        } catch (final Exception e) {
            logger.error("Failed to retrieve value.", e);
            return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/{service}/{serviceKey}/{key}")
    public Response setValue(@PathParam("service") final String service, @PathParam("serviceKey") final String serviceKey,
                             @PathParam("key") final String key, final String value) {
        try {
            checkLocalService(service);
            m_keyStore.setValue(service, serviceKey, key, value);
            return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
        } catch (final NotAuthorizedException e) {
            logger.error("Attempt to access a local service.");
            return setHeaders(Response.status(Status.UNAUTHORIZED)).build();
        } catch (final Exception e) {
            logger.error("Failed to add value.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        }
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/{service}/{serviceKey}/{key}")
    public Response deleteKey(@PathParam("service") final String service, @PathParam("serviceKey") final String serviceKey,
                              @PathParam("key") final String key) {
        try {
            checkLocalService(service);
            m_keyStore.deleteKey(service, serviceKey, key);
            return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
        } catch (final NotAuthorizedException e) {
            logger.error("Attempt to access a local service.");
            return setHeaders(Response.status(Status.UNAUTHORIZED)).build();
        } catch (final Exception e) {
            logger.error("Failed to delete key.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        }
    }

    private void checkLocalService(final String service)
            throws NotAuthorizedException {
        if (service.startsWith("_")) {
            throw new NotAuthorizedException("Attempt to access an unauthorized service");
        }
    }

    private class NotAuthorizedException extends Exception {
        private static final long serialVersionUID = -9178572502364424189L;

        NotAuthorizedException(final String message) {
            super(message);
        }
    }
}
