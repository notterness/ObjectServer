package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BackendConversions;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

@SuppressWarnings("checkstyle:linelength") // Can't split the url in the block comment
/**
 * <p>
 * Reads an object, including metadata.</p>
 * <p>
 * Swift implementation notes:
 * See {@link SwiftReadObjectHelper} and
 * <a href="http://developer.openstack.org/api-ref/object-storage/index.html?expanded=show-object-metadata-detail&expanded=get-object-content-and-metadata-detail#get-object-content-and-metadata">
 *  Openstack - Get object content and metadata
 * </a></p>
 */
public class GetObjectHandler extends CompletableHandler {
    private final TrafficController controller;
    private final SwiftReadObjectHelper readObjectHelper;
    private final SwiftResponseWriter swiftResponseWriter;
    private final DecidingKeyManagementService kms;
    private final EmbargoV3 embargoV3;

    public GetObjectHandler(
        TrafficController controller,
        SwiftReadObjectHelper readObjectHelper,
        SwiftResponseWriter swiftResponseWriter,
        DecidingKeyManagementService kms,
        EmbargoV3 embargoV3) {
        this.controller = controller;
        this.readObjectHelper = readObjectHelper;
        this.swiftResponseWriter = swiftResponseWriter;
        this.kms = kms;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName("swift", getClass(), context, CasperOperation.GET_OBJECT);

        // The re-entry pass is required as the request is authorized multiple times during the GET object flow
        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.GETOBJECT_REQUEST_COUNT);

        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final String namespace = SwiftHttpPathHelpers.getNamespace(context.request(), wsRequestContext);
        final String containerName = SwiftHttpPathHelpers.getContainerName(context.request(), wsRequestContext);
        final String objectName = SwiftHttpPathHelpers.getObjectName(context.request(), wsRequestContext);
        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.Swift)
            .setOperation(CasperOperation.GET_OBJECT)
            .setNamespace(namespace)
            .setBucket(containerName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoOperation);

        final String rid = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        final long start = System.nanoTime();
        return readObjectHelper
            .beginHandleCompletably(
                    context, WebServerMetrics.SWIFT_GET_OBJECT_BUNDLE, GetObjectBackend.ReadOperation.GET)
            .thenComposeAsync(res -> {
                final WSStorageObject so = res.getStorageObject();
                final ByteRange byteRange = HttpHeaderHelpers.tryParseByteRange(context.request(), res.getSize());
                final long length = byteRange == null ? so.getTotalSizeInBytes() : byteRange.getLength();
                final AbortableBlobReadStream stream = readObjectHelper.getStream(
                        context, res, byteRange, WebServerMetrics.SWIFT_GET_OBJECT_BUNDLE);

                // Acquire can throw an exception(with err code of 503 or 429) if the request couldnt be accepted
                controller.acquire(so.getKey().getBucket().getNamespace(),
                        TrafficRecorder.RequestType.GetObject,
                        length);

                TrafficRecorder.getVertxRecorder().requestStarted(
                        so.getKey().getBucket().getNamespace(),
                        rid, TrafficRecorder.RequestType.GetObject, length, start);

                final ObjectMetadata objMeta = BackendConversions.wsStorageObjectSummaryToObjectMetadata(so, kms);
                return swiftResponseWriter.writeGetObjectResponse(
                        context, objMeta, byteRange, stream, res.getSize(), res.resourceNotModified())
                        .whenComplete((n, t) -> TrafficRecorder.getVertxRecorder().requestEnded(rid));
            }, VertxExecutor.eventLoop());
    }
}
