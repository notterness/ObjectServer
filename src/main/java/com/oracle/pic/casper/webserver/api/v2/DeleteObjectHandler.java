package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Date;

/**
 * Vert.x HTTP handler for deleting objects.
 * This is one of the few handlers where we don't record REQUEST for billing/metering
 */
public class DeleteObjectHandler extends SyncHandler {
    private final Authenticator authenticator;
    private final Backend backend;
    private final EmbargoV3 embargoV3;

    public DeleteObjectHandler(Authenticator authenticator, Backend backend, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.DELETE_OBJECT);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_DELETE_OBJECT_BUNDLE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.DELETE_OBJECT)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.DELETEOBJECT_REQUEST_COUNT);

        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.NO);
        final String etag = HttpMatchHelpers.getIfMatchHeader(request);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(context);
            final Date deleteDate = backend.deleteV2Object(context, authInfo, namespace, bucketName, objectName, etag)
                    .orElseThrow(() -> new HttpException(V2ErrorCode.OBJECT_NOT_FOUND,
                            "The object '" + objectName + "' does not exist in bucket '" + bucketName +
                                "' with namespace '" + namespace + "'", request.path()));
            response.putHeader(HttpHeaders.LAST_MODIFIED, DateUtil.httpFormattedDate(deleteDate))
                    .setStatusCode(HttpResponseStatus.NO_CONTENT)
                    .end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
