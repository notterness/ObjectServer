package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

/**
 * Vert.x HTTP handler for fetching storage objects.
 */
public class GetObjectHandler extends CompletableHandler {
    private final TrafficController controller;
    private final V2ReadObjectHelper readObjectHelper;
    private final EmbargoV3 embargoV3;

    public GetObjectHandler(TrafficController controller, V2ReadObjectHelper readObjectHelper, EmbargoV3 embargoV3) {
        this.controller = controller;
        this.readObjectHelper = readObjectHelper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.GET_OBJECT);
        final String rid = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        final long start = System.nanoTime();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.GETOBJECT_REQUEST_COUNT);

        final HttpServerRequest request = context.request();
        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.GET_OBJECT)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        return readObjectHelper
            .beginHandleCompletably(context, WebServerMetrics.V2_GET_OBJECT_BUNDLE, GetObjectBackend.ReadOperation.GET)
            .thenComposeAsync(optExcAndRange -> {
                // If the optional is empty there was an if-none-match that matched, so just return
                final Pair<WSStorageObject, ByteRange> p = optExcAndRange.orElse(null);
                if (p == null) {
                    return CompletableFuture.completedFuture(null);
                }

                final WSStorageObject so = p.getFirst();
                final ByteRange range = p.getSecond();
                final long length = range == null ? so.getTotalSizeInBytes() : range.getLength();

                // This method can throw an exception, so we delay starting the recorder until this method returns (so
                // we do not need to deal with a try/catch to stop the recorder).
                final AbortableBlobReadStream stream = readObjectHelper.getObjectStream(
                        context, so, range, WebServerMetrics.V2_GET_OBJECT_BUNDLE);

                // Check that we have enough bandwidth to send this request
                // Acquire can throw an exception(with err code of 503 or 429) if the request couldnt be accepted
                controller.acquire(so.getKey().getBucket().getNamespace(),
                        TrafficRecorder.RequestType.GetObject,
                        length);

                // Start the recorder using the previously captured start time, since we did not know the content-length
                // of this request until now.
                TrafficRecorder.getVertxRecorder().requestStarted(
                        so.getKey().getBucket().getNamespace(),
                        rid, TrafficRecorder.RequestType.GetObject, length, start);

                // Start pumping bytes and add a handler to end the recorder when all the bytes have been sent (or there
                // was an error).
                return HttpContentHelpers.pumpResponseContent(context, context.response(), stream)
                        .whenComplete((n, t) -> TrafficRecorder.getVertxRecorder().requestEnded(rid));
            }, VertxExecutor.eventLoop());
    }
}
