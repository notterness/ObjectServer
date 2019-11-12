package com.oracle.pic.casper.webserver.api.control;

import com.oracle.pic.casper.common.config.v2.BoatConfiguration;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.ControlAPIGetObjectBackend;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.api.v2.ServerInfoHandler;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A control plane handler to verify an object.
 *
 * This API call verifies both ciphertext and plaintext integrity by retrieving and decrypting the entire object.
 * The plaintext bytes are MD5 hashed and the result compared to the MD5 in the database.
 *
 * This method does not support multi-part objects, which unfortunately do not have reproducible MD5s stored.  Requests
 * for multipart objects fail with 501 Not Implemented.
 *
 * Requests for objects that lack an MD5 also fail with 501 Not Implemented.
 *
 * In no case should this method return the actual or expected MD5s for customer data!
 *
 * Authorization for this method, like {@link ServerInfoHandler}, relies on BOAT:
 *
 * https://confluence.oci.oraclecorp.com/display/AM/BMC+Operator+Access+Tenancy+%28BOAT%29+Design
 *
 * A policy must authorize OCI to access this API, like so:
 *
 *   ADMIT GROUP casper OF TENANCY boat TO { CHECK_ALL_OBJECTS } ON COMPARTMENT sparta-resources
 *         WHERE ALL { request.operation='CheckObject' }
 *
 * Tanden manages these policies.  Do not edit them by hand!
 */
public class CheckObjectHandler extends CompletableHandler {

    private static final String API_QUERY_PARAM = "api";
    private final AsyncAuthenticator authenticator;
    private final ControlAPIGetObjectBackend controlAPIGetObjectBackend;

    public CheckObjectHandler(
            AsyncAuthenticator authenticator,
            BoatConfiguration boatConfiguration,
            GetObjectBackend getObjectBackend
    ) {
        this.authenticator = authenticator;
        this.controlAPIGetObjectBackend = new ControlAPIGetObjectBackend(getObjectBackend, boatConfiguration);
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.CHECK_OBJECT);
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        try {
            return aaaAndMaybeCheckObject(context, request, response);
        } catch (Throwable t) {
            throw HttpException.rewrite(request, t);
        }
    }

    private CompletableFuture<Void> aaaAndMaybeCheckObject(
            RoutingContext context,
            HttpServerRequest request,
            HttpServerResponse response
    ) {
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);
        final String ifMatch = request.getHeader(HttpHeaders.IF_MATCH);
        final Api api = Optional.ofNullable(request.getParam(API_QUERY_PARAM))
            .map(s -> Api.fromVersion(s))
            .orElse(Api.V2);

        return authenticator.authenticate(context)
                .thenCompose(authInfo ->
                    controlAPIGetObjectBackend.checkObject(
                        context,
                        wsRequestContext,
                        authInfo,
                        namespace,
                        bucketName,
                        objectName,
                        api,
                        ifMatch
                    )
                )
                .thenApply(ignored -> {
                    response.setStatusCode(HttpResponseStatus.OK).end();
                    return null;
                });
    }
}
