package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;


import java.util.concurrent.CompletableFuture;

/**
 * Vert.x HTTP handler for fetching object metadata.
 */
public class HeadObjectHandler extends CompletableHandler {
    private final V2ReadObjectHelper readObjectHelper;
    private final EmbargoV3 embargoV3;

    public HeadObjectHandler(V2ReadObjectHelper readObjectHelper, EmbargoV3 embargoV3) {
        this.readObjectHelper = readObjectHelper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.HEAD_OBJECT);
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.HEADOBJECT_REQUEST_COUNT);

        final HttpServerRequest request = context.request();
        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.HEAD_OBJECT)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        return readObjectHelper
            .beginHandleCompletably(
                    context, WebServerMetrics.V2_HEAD_OBJECT_BUNDLE, GetObjectBackend.ReadOperation.HEAD)
            .thenAccept(optionalReadStorageObjectExchange ->
                optionalReadStorageObjectExchange
                    .ifPresent(readStorageObjectExchange -> context.response().end())
            );
    }
}
