package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.RenameRequest;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;

/**
 * Vert.x HTTP handler for renaming objects.
 */
public class RenameObjectHandler extends SyncBodyHandler {

    private final Authenticator authenticator;
    private final Backend backend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    public RenameObjectHandler(Authenticator authenticator,
                               Backend backend, ObjectMapper mapper,
                               CountingHandler.MaximumContentLength maximumContentLength,
                               EmbargoV3 embargoV3) {
        super(maximumContentLength);
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
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_RENAME_OBJECT_BUNDLE);
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.RENAME_OBJECT);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.RENAMEOBJECT_REQUEST_COUNT);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        try {
            final RenameRequest renameRequest = HttpContentHelpers.readRenameContent(request, mapper, bytes);

            final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
                    .setApi(EmbargoV3Operation.Api.V2)
                    .setOperation(CasperOperation.RENAME_OBJECT)
                    .setNamespace(namespace)
                    .setBucket(bucketName)
                    .setObject(renameRequest.getSourceName())
                    .build();
            embargoV3.enter(embargoV3Operation);

            final AuthenticationInfo authInfo = authenticator.authenticate(
                    context,
                    Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));
            final ObjectMetadata objMeta =
                    backend.renameObject(context, authInfo, namespace, bucketName, renameRequest);

            final HttpHeaderHelpers.Header etag = HttpHeaderHelpers.etagHeader(objMeta.getETag());
            final HttpHeaderHelpers.Header lastModified =
                HttpHeaderHelpers.lastModifiedHeader(DateUtil.httpFormattedDate(objMeta.getModificationTime()));
            response
                .putHeader(etag.getName(), etag.getValue())
                .putHeader(lastModified.getName(), lastModified.getValue())
                .setStatusCode(HttpResponseStatus.OK)
                .end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
