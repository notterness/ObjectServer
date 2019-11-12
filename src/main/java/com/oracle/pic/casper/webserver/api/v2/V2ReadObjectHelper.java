package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.backend.BackendConversions;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetricsBundle;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * <p>
 * Helper to implement Vert.x HTTP handlers that fetch object metadata (HEAD) or fetch object metadata with the object
 * content (GET).</p>
 */
public class V2ReadObjectHelper {
    private final AsyncAuthenticator authenticator;
    private final GetObjectBackend backend;
    private final DecidingKeyManagementService kms;

    public V2ReadObjectHelper(AsyncAuthenticator authenticator,
                              GetObjectBackend backend,
                              DecidingKeyManagementService kms) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.kms = kms;
    }

    /**
     * Begin handling HEAD and GET object requests following the general pattern to handle HTTP requests:
     * <ul>
     * <li>Parse and validate the request</li>
     * <li>Authenticate</li>
     * <li>Call backend to fetch object metadata</li>
     * <li>Begin crafting the response to be completed by the caller</li>
     * <li>Handle any exception thrown by converting it to a HTTP response</li>
     * </ul>
     *
     * @param context the Vert.x request context
     * @param bundle specific to the HTTP method and API
     * @param readOperation the operation being performed
     * @return A present {@link Optional} indicates success and the caller should end the response. An empty
     * {@link Optional} optional indicates the resource was not modified and the response has already been ended.
     */
    public CompletableFuture<Optional<Pair<WSStorageObject, ByteRange>>> beginHandleCompletably(
            RoutingContext context,
            WebServerMetricsBundle bundle,
            GetObjectBackend.ReadOperation readOperation) {

        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);

        return beginHandleCompletably(context, bundle, readOperation, namespace, bucketName, objectName);
    }

    public CompletableFuture<Optional<Pair<WSStorageObject, ByteRange>>> beginHandleCompletably(
            RoutingContext context,
            WebServerMetricsBundle bundle,
            GetObjectBackend.ReadOperation readOperation,
            String namespace,
            String bucketName,
            String objectName) {
        MetricsHandler.addMetrics(context, bundle);

        // Parse the HTTP request
        // TODO extract functionality common with S3 into static method(s).
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
            HttpMatchHelpers.IfNoneMatchAllowed.YES);

        // at this point we do not know actual content length of object
        // try to parse range offsets from header and throw exception if they are invalid
        HttpHeaderHelpers.validateRangeHeader(request);

        // used reference b/c some compilers get confused when handle() is appended directly to the chain
        CompletableFuture<Optional<Pair<WSStorageObject, ByteRange>>> cf =

            // Authenticate
            authenticator.authenticate(context)

                // Call the backend to get object metadata
                .thenCompose(authInfo -> backend.getV2StorageObject(
                        context, authInfo, readOperation, namespace, bucketName, objectName))

                // Begin crafting the HTTP response for the object metadata
                .thenApply(so -> {
                    final ObjectMetadata objMeta = BackendConversions.wsStorageObjectSummaryToObjectMetadata(so, kms);
                    if (HttpMatchHelpers.checkConditionalHeaders(request, objMeta.getETag())) {
                        // If-None-Match ETag matches.  Return 304 Not Modified
                        response.putHeader(HttpHeaders.ETAG, objMeta.getETag())
                            .setStatusCode(HttpResponseStatus.NOT_MODIFIED)
                            .end();
                        return Optional.empty();
                    }
                    final ByteRange byteRange = HttpHeaderHelpers.tryParseByteRange(request, objMeta.getSizeInBytes());
                    HttpContentHelpers.writeReadObjectResponseHeadersAndStatus(context, objMeta, byteRange);
                    response.putHeader(HttpHeaders.LAST_MODIFIED,
                        DateUtil.httpFormattedDate(objMeta.getModificationTime()));
                    response.putHeader(HttpHeaders.ETAG, objMeta.getETag());
                    HttpHeaderHelpers.addUserMetadataToHttpHeaders(response, objMeta);
                    ServiceLogsHelper.logServiceEntry(context);
                    return Optional.of(Pair.pair(so, byteRange));
                });

        // Handle errors
        return cf.handle((val, ex) -> HttpException.handle(request, val, ex));
    }

    public AbortableBlobReadStream getObjectStream(RoutingContext context,
                                                   WSStorageObject so,
                                                   ByteRange byteRange,
                                                   WebServerMetricsBundle bundle) {
        return backend.getObjectStream(context, so, byteRange, bundle);
    }
}
