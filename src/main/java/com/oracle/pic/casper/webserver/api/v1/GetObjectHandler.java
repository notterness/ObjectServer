package com.oracle.pic.casper.webserver.api.v1;

import com.oracle.pic.casper.common.exceptions.TooBusyException;
import com.oracle.pic.casper.common.exceptions.TooManyRequestsException;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.objectmeta.ObjectKey;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.traffic.TenantThrottleException;
import com.oracle.pic.casper.webserver.traffic.TrafficControlException;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.ext.web.RoutingContext;

/**
 * Returns the metadata and data for an object in the object store. If the
 * object does not exist, this method returns a 404 error.
 */
public class GetObjectHandler extends AbstractRouteHandler {

    private final TrafficController controller;
    private final V1ReadObjectHelper readObjectHelper;
    private final EmbargoV3 embargoV3;

    public GetObjectHandler(TrafficController controller,
                            V1ReadObjectHelper readObjectHelper,
                            EmbargoV3 embargoV3) {
        super();
        this.controller = controller;
        this.readObjectHelper = readObjectHelper;
        this.embargoV3 = embargoV3;
    }

    @Override
    protected void subclassHandle(RoutingContext context) {
        final ObjectKey objectKey = RequestHelpers.computeObjectKey(context.request());
        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V1)
            .setNamespace(objectKey.getBucket().getNamespace())
            .setBucket(objectKey.getBucket().getName())
            .setObject(objectKey.getName())
            .setOperation(CasperOperation.GET_OBJECT)
            .build();
        embargoV3.enter(embargoV3Operation);

        final String rid = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        final long start = System.nanoTime();
        readObjectHelper
                .beginHandleCompletably(
                        context, WebServerMetrics.V1_GET_OBJECT_BUNDLE, GetObjectBackend.ReadOperation.GET)
                .thenAcceptAsync(optExcAndRange -> {
                    final Pair<WSStorageObject, ByteRange> p = optExcAndRange.orElse(null);
                    if (p == null) {
                        return;
                    }

                    final WSStorageObject so = p.getFirst();
                    final ByteRange range = p.getSecond();
                    final long length = range == null ? so.getTotalSizeInBytes() : range.getLength();
                    final AbortableBlobReadStream stream = readObjectHelper.getObjectStream(
                            context, so, range, WebServerMetrics.V1_GET_OBJECT_BUNDLE);

                    // Acquire can throw an exception(with err code of 503 or 429) if the request couldnt be accepted
                    try {
                        controller.acquire(null, TrafficRecorder.RequestType.GetObject, length);
                    } catch (TrafficControlException tce) {
                        throw new TooBusyException("the service is currently unavailable");
                    } catch (TenantThrottleException tte) {
                        throw new TooManyRequestsException("Too many requests!");
                    }

                    TrafficRecorder.getVertxRecorder().requestStarted(
                            so.getKey().getBucket().getNamespace(),
                            rid, TrafficRecorder.RequestType.GetObject, length, start);
                    HttpContentHelpers.pumpResponseContent(context, context.response(), stream)
                            .whenComplete((n, t) -> TrafficRecorder.getVertxRecorder().requestEnded(rid));
                }, VertxExecutor.eventLoop())
                .exceptionally(throwable -> GetObjectHandler.fail(context, throwable));
    }
}
