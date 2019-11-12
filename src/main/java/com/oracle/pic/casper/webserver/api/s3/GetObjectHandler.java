package com.oracle.pic.casper.webserver.api.s3;

import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.util.ThrowableUtil;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.stream.AbortableReadStream;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidObjectNameException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

/**
 * Vert.x HTTP handler for fetching storage objects.
 */
public class GetObjectHandler extends CompletableHandler {
    private final TrafficController controller;
    private final S3ReadObjectHelper readObjectHelper;
    private final EmbargoV3 embargoV3;

    public GetObjectHandler(TrafficController controller, S3ReadObjectHelper readObjectHelper, EmbargoV3 embargoV3) {
        this.controller = controller;
        this.readObjectHelper = readObjectHelper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.GET_OBJECT);
        WSRequestContext.get(context).setWebServerMonitoringMetric(WebServerMonitoringMetric.GETOBJECT_REQUEST_COUNT);

        final String namespace = S3HttpHelpers.getNamespace(context.request(), WSRequestContext.get(context));
        final String bucketName = S3HttpHelpers.getBucketName(context.request(), WSRequestContext.get(context));
        final String objectName = S3HttpHelpers.getObjectName(context.request(), WSRequestContext.get(context));

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.GET_OBJECT)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoOperation);

        final String rid = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        final long start = System.nanoTime();
        return readObjectHelper
            .beginHandleCompletably(context, WebServerMetrics.S3_GET_OBJECT_BUNDLE, GetObjectBackend.ReadOperation.GET)
            .thenComposeAsync(excAndRange -> {
                final WSStorageObject so = excAndRange.getFirst();
                final ByteRange range = excAndRange.getSecond();
                final long length = range == null ? so.getTotalSizeInBytes() : range.getLength();
                final AbortableReadStream<Buffer> stream = readObjectHelper.getObjectStream(
                        context, so, range, WebServerMetrics.S3_GET_OBJECT_BUNDLE);

                // Acquire can throw an exception(with err code of 503 or 429) if the request couldnt be accepted
                controller.acquire(so.getKey().getBucket().getNamespace(),
                        TrafficRecorder.RequestType.GetObject,
                        length);

                TrafficRecorder.getVertxRecorder().requestStarted(
                        so.getKey().getBucket().getNamespace(),
                        rid, TrafficRecorder.RequestType.GetObject, length, start);

                return HttpContentHelpers.pumpResponseContent(context, context.response(), stream)
                        .whenComplete((n, t) -> TrafficRecorder.getVertxRecorder().requestEnded(rid));
            }, VertxExecutor.eventLoop())
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
