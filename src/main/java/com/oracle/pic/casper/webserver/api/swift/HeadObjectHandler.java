package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BackendConversions;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

@SuppressWarnings("checkstyle:linelength") // Can't split the url in the block comment
/**
 * <p>
 * Reads metadata on an object.</p>
 *
 * Swift implementation notes:
 *
 * I haven't been able to find any differences between this method and the Swift GET object method, besides the obvious
 * one, so see {@link SwiftReadObjectHelper} for implementation notes.  For Content-Lengths, swift behaves exactly as we
 * do:
 * <ul>
 *  <li>on 304 responses, the Content-Length is set to zero</li>
 *  <li>on 200 responses, the Content-Length is set to the size of the object</li>
 * </ul>
 * Also see:
 * <a href="http://developer.openstack.org/api-ref/object-storage/index.html?expanded=show-object-metadata-detail#show-object-metadata">
 *     Openstack - Show object metadata
 * </a>
 */
public class HeadObjectHandler extends CompletableHandler {
    private final SwiftReadObjectHelper readObjectHelper;
    private final SwiftResponseWriter swiftResponseWriter;
    private final DecidingKeyManagementService kms;
    private final EmbargoV3 embargoV3;

    public HeadObjectHandler(SwiftReadObjectHelper readObjectHelper,
                             SwiftResponseWriter swiftResponseWriter,
                             DecidingKeyManagementService kms,
                             EmbargoV3 embargoV3) {
        this.readObjectHelper = readObjectHelper;
        this.swiftResponseWriter = swiftResponseWriter;
        this.kms = kms;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName("swift", getClass(), context, CasperOperation.HEAD_OBJECT);

        // The re-entry pass is required as the request is authorized multiple times during the HEAD object flow
        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final String namespace = SwiftHttpPathHelpers.getNamespace(context.request(), wsRequestContext);
        final String containerName = SwiftHttpPathHelpers.getContainerName(context.request(), wsRequestContext);
        final String objectName = SwiftHttpPathHelpers.getObjectName(context.request(), wsRequestContext);
        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.Swift)
            .setOperation(CasperOperation.HEAD_OBJECT)
            .setNamespace(namespace)
            .setBucket(containerName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoOperation);

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.HEADOBJECT_REQUEST_COUNT);

        return readObjectHelper
            .beginHandleCompletably(
                    context, WebServerMetrics.SWIFT_HEAD_OBJECT_BUNDLE, GetObjectBackend.ReadOperation.HEAD)
            .thenAccept(intermediateResponse -> {
                final ObjectMetadata objectMetadata = BackendConversions.wsStorageObjectSummaryToObjectMetadata(
                        intermediateResponse.getStorageObject(), kms);
                final long objectSizeInBytes = intermediateResponse.getSize();
                final boolean notModified = intermediateResponse.resourceNotModified();
                swiftResponseWriter.writeHeadObjectResponse(context, objectMetadata, objectSizeInBytes, notModified);
            });
    }
}
