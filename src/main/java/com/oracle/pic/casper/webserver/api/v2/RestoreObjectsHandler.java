package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.AlreadyRestoredObjectException;
import com.oracle.pic.casper.common.exceptions.RestoreInProgressException;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.RestoreObjectsDetails;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;

/**
 * Vert.x HTTP handler for restore objects.
 */
public class RestoreObjectsHandler extends SyncBodyHandler {

    private static final long MAX_RESTORE_CONTENT_BYTES = 10 * 1024;
    private final Authenticator authenticator;
    private final Backend backend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    public RestoreObjectsHandler(Authenticator authenticator,
                                 Backend backend,
                                 ObjectMapper mapper,
                                 CountingHandler.MaximumContentLength contentLength,
                                 EmbargoV3 embargoV3) {
        super(contentLength);
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {
        HttpContentHelpers.negotiateApplicationJsonContent(context.request(), MAX_RESTORE_CONTENT_BYTES);
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_RESTORE_OBJECTS_BUNDLE);
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.RESTORE_OBJECT);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespaceName = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final RestoreObjectsDetails restoreObjectsDetails =
                HttpContentHelpers.readRestoreObjectDetails(request.path(), mapper, bytes);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.RESTORE_OBJECT)
            .setNamespace(namespaceName)
            .setBucket(bucketName)
            .setObject(restoreObjectsDetails.getObjectName())
            .build();
        embargoV3.enter(embargoV3Operation);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(
                    context, Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));
            backend.restoreObject(context, authInfo, namespaceName, bucketName, restoreObjectsDetails);
            response.setStatusCode(HttpResponseStatus.ACCEPTED)
                    .end();
        } catch (AlreadyRestoredObjectException ex) {
            /* returns 200 on multiple restore request */
            response.setStatusCode(HttpResponseStatus.OK).end();
        } catch (RestoreInProgressException ex) {
            // return 202 if the restore has already started
            response.setStatusCode(HttpResponseStatus.ACCEPTED).end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
