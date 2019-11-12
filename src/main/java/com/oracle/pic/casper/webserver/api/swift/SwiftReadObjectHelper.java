package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.ChecksumType;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.Checksum;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.IntermediateReadObjectResponse;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetricsBundle;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.CompletableFuture;

/**
 * <p>
 * Helper to read an object's metadata with Swift's semantics; the metadata can be used for a HEAD response or to
 * getStream for a GET response.</p>
 * <p>
 * Swift implementation notes:
 * <ul>
 * <li>Swift always returns the object's ETag, Last-Modified, Content-Type, X-Timestamp, and custom user metadata
 * headers, even on byte-range, 412, and 304 responses.</li>
 * <li>Swift sends a Content-Length of zero on 304 responses, as expected.</li>
 * <li>Swift always allows wildcards in OCC headers and they always match (i.e. no-op headers like "GET If-Match: *" and
 * nonsensical headers like "GET If-None-Match: *" are allowed and treated as matching; 200 and 304 respectively), so
 * we do too.</li>
 * <li>Swift supports multiple ranges on range get requests, e.g. "Range: bytes=1-2,4-5". Our backend doesn't support
 * this, so our Swift impl doesn't either.</li>
 * </ul>
 * The HTTP spec says that sending both If-Match and If-None-Match on a request results in undefined behavior.  Swift
 * seems to allow any combination of these headers and has an interpretation for each combination, but the algorithm
 * is complicated (see code comment below), and the value of duplicating Swift's edge-case behavior here is low, so
 * for now we're just going to return 400 if you pass us both headers.</p>
 */
public class SwiftReadObjectHelper {
    /*
     * Some notes on Swift's OCC algorithm that we'll hopefully never need:
     *
     * If the If-Match header matches and the If-None-Match header matches (both), Swift returns 304.
     * If the If-Match header matches and the If-None-Match header doesn't, Swift returns 200.
     * If the If-Match header doesn't match and the If-None-Match header does, Swift returns 304.
     * If the If-Match header doesn't match and the If-None-Match header doesn't (neither), Swift returns 412.
     *
     * This applies to both HEAD and GET requests.
     * Things get complicated if you pass the same header twice.  More testing needed to figure out that algorithm.
     */
    public static final String STATIC_ACCEPT_RANGES = "bytes";
    public static final int MAX_PARTS_SUPPORT = 1000;

    private final Authenticator authenticator;
    private final GetObjectBackend backend;
    private final TenantBackend tenantBackend;
    private final DecidingKeyManagementService kms;

    public SwiftReadObjectHelper(Authenticator authenticator,
                                 GetObjectBackend backend,
                                 TenantBackend tenantBackend,
                                 DecidingKeyManagementService kms) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.tenantBackend = tenantBackend;
        this.kms = kms;
    }

    public CompletableFuture<IntermediateReadObjectResponse> beginHandleCompletably(
            RoutingContext context,
            WebServerMetricsBundle bundle,
            GetObjectBackend.ReadOperation readOperation) {
        MetricsHandler.addMetrics(context, bundle);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = SwiftHttpPathHelpers.getNamespace(request, wsRequestContext);
        final String containerName = SwiftHttpPathHelpers.getContainerName(request, wsRequestContext);
        final String objectName = SwiftHttpPathHelpers.getObjectName(request, wsRequestContext);

        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.YES_WITH_STAR);

        // Inform clients that we do now support range requests
        response.putHeader(HttpHeaders.ACCEPT_RANGES, STATIC_ACCEPT_RANGES);

        return VertxUtil.runAsync(() -> {
            final String tenantOcid = tenantBackend.getNamespace(context, scope, new NamespaceKey(Api.V2, namespace))
                    .map(MdsNamespace::getTenantOcid)
                    .orElse(null);
            return authenticator.authenticateSwiftOrCavage(context, null, namespace, tenantOcid);
        })//get storage object
            .thenCompose(authInfo ->
                backend.getV2StorageObject(context, authInfo, readOperation, namespace, containerName, objectName)
                .thenApply(so -> Pair.of(authInfo, so)))
            .thenCompose(pair -> {
                final WSStorageObject so = pair.getRight();

                // TODO: are the headers case-insensitive here?
                boolean isLargeObject = so.getMetadata(kms).containsKey(SwiftHeaders.LARGE_OBJECT_HEADER);

                if (isLargeObject) {
                    //if it is dynamic large object then return all objects in container with specific prefix
                    String partsPath = so.getMetadata(kms).get(SwiftHeaders.LARGE_OBJECT_HEADER);
                    int delimitIndex = partsPath.indexOf('/');
                    if (delimitIndex == -1) {
                        throw new BadRequestException("Manifest header must be in <container>/<prefix> format, was " +
                            partsPath);
                    }
                    String partsContainerName = partsPath.substring(0, delimitIndex);
                    String partsObjectPrefix = partsPath.substring(delimitIndex + 1);
                    if (partsObjectPrefix.equals("")) {
                        partsObjectPrefix = null;
                    }
                    final AuthenticationInfo authInfo = pair.getLeft();
                    return backend.getStorageObjectsFromPrefix(context, authInfo, readOperation, namespace,
                        partsContainerName, partsObjectPrefix, SwiftReadObjectHelper.MAX_PARTS_SUPPORT)
                        .thenApply(exchanges -> new IntermediateReadObjectResponse(so, exchanges,
                            false));
                } else {
                    //otherwise check if not modified, return storage object
                    final Checksum checksum = so.getChecksumType() == ChecksumType.MD5 ?
                            Checksum.fromBase64(so.getMd5()) :
                            Checksum.fromMultipartBase64(so.getMd5(), so.getPartCount());
                    final boolean notModified = HttpMatchHelpers.checkConditionalHeaders(request, checksum.getHex());
                    IntermediateReadObjectResponse readObjectResponse =
                        new IntermediateReadObjectResponse(so, null, notModified);
                    ServiceLogsHelper.logServiceEntry(context);
                    return CompletableFuture.completedFuture(readObjectResponse);
                }
            })
            .handle((val, ex) -> SwiftHttpExceptionHelpers.handle(context, val, ex));
    }

    /**
     * Pass through to the backend to get the object's content
     */
    public AbortableBlobReadStream getStream(RoutingContext context,
                                             IntermediateReadObjectResponse readObjectResponse,
                                             ByteRange byteRange,
                                             WebServerMetricsBundle bundle) {
        if (readObjectResponse.resourceNotModified()) {
            return null;
        } else if (readObjectResponse.isLargeObject()) {
            return backend.getCombinedObjectStream(
                    WSRequestContext.get(context),
                    readObjectResponse.getParts(),
                    readObjectResponse.getSize(),
                    byteRange,
                    bundle);
        } else {
            return backend.getObjectStream(context, readObjectResponse.getStorageObject(), byteRange, bundle);
        }
    }
}
