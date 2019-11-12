package com.oracle.pic.casper.webserver.api.s3;

import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.util.ThrowableUtil;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidObjectNameException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

/**
 * Vert.x HTTP handler for fetching storage object metadata.
 */
public class HeadObjectHandler extends CompletableHandler {
    private final S3ReadObjectHelper readObjectHelper;
    private final EmbargoV3 embargoV3;

    public HeadObjectHandler(S3ReadObjectHelper readObjectHelper, EmbargoV3 embargoV3) {
        this.readObjectHelper = readObjectHelper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.HEAD_OBJECT);
        WSRequestContext.get(context).setWebServerMonitoringMetric(WebServerMonitoringMetric.HEADOBJECT_REQUEST_COUNT);
        final String namespace = S3HttpHelpers.getNamespace(context.request(), WSRequestContext.get(context));
        final String bucketName = S3HttpHelpers.getBucketName(context.request(), WSRequestContext.get(context));
        final String objectName = S3HttpHelpers.getObjectName(context.request(), WSRequestContext.get(context));

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.HEAD_OBJECT)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoOperation);

        return readObjectHelper
            .beginHandleCompletably(
                    context, WebServerMetrics.S3_HEAD_OBJECT_BUNDLE, GetObjectBackend.ReadOperation.HEAD)
            .thenAccept(readStorageObjectExchange -> context.response().end())
            .exceptionally(ex -> {
                Throwable t = ThrowableUtil.getUnderlyingThrowable(ex);
                if (t instanceof InvalidObjectNameException) {
                    throw new S3HttpException(S3ErrorCode.NOT_FOUND,
                            ((InvalidObjectNameException) t).errorMessage(), context.request().path(),
                            ex);
                }
                throw S3HttpException.rewrite(context, ex);
            });
    }
}
