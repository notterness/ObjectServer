package com.oracle.pic.casper.webserver.api.v1;

import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.objectmeta.ObjectKey;
import com.oracle.pic.casper.webserver.api.backend.BackendConversions;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.api.v2.V2ReadObjectHelper;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetricsBundle;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * This is similar to {@link V2ReadObjectHelper} and is its own class because we have to remove authentication.
 * I rather make the code copy then much with V2 version because V1 is on deprecation path.
 */
@Deprecated
public class V1ReadObjectHelper {

    private final GetObjectBackend backend;
    private final DecidingKeyManagementService kms;

    public V1ReadObjectHelper(GetObjectBackend backend, DecidingKeyManagementService kms) {
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
     * @param bundle specific to the HTTP method and API
     * @return A present {@link Optional} indicates success and the caller should end the response. An empty
     * {@link Optional} optional indicates the resource was not modified and the response has already been ended.
     */
    public CompletableFuture<Optional<Pair<WSStorageObject, ByteRange>>> beginHandleCompletably(
            RoutingContext context,
            WebServerMetricsBundle bundle,
            GetObjectBackend.ReadOperation readOperation) {
        MetricsHandler.addMetrics(context, bundle);

        // Parse the HTTP request
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        final ObjectKey objectKey = RequestHelpers.computeObjectKey(request);
        final String namespace = objectKey.getBucket().getNamespace();
        final String bucketName = objectKey.getBucket().getName();
        final String objectName = objectKey.getName();

        WSRequestContext.get(context).setNamespaceName(namespace);
        WSRequestContext.get(context).setBucketName(bucketName);
        WSRequestContext.get(context).setObjectName(objectName);

        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.YES);

        // at this point we do not know actual content length of object
        // try to parse range offsets from header and throw exception if they are invalid
        HttpHeaderHelpers.validateRangeHeader(request);

        // used reference b/c some compilers get confused when handle() is appended directly to the chain
        CompletableFuture<Optional<Pair<WSStorageObject, ByteRange>>> cf =
                backend.getV1StorageObject(context, readOperation, namespace, bucketName, objectName)
                        // Begin crafting the HTTP response for the object metadata
                        .thenApply(so -> {
                            final ObjectMetadata objMeta = BackendConversions.wsStorageObjectSummaryToObjectMetadata(
                                    so, kms);
                            if (HttpMatchHelpers.checkConditionalHeaders(request, so.getETag())) {
                                // If-None-Match ETag matches.  Return 304 Not Modified
                                response.putHeader(HttpHeaders.ETAG, so.getETag())
                                        .setStatusCode(HttpResponseStatus.NOT_MODIFIED)
                                        .end();
                                return Optional.empty();
                            }
                            final ByteRange byteRange = HttpHeaderHelpers.tryParseByteRange(
                                    request, so.getTotalSizeInBytes());
                            HttpContentHelpers.writeReadObjectResponseHeadersAndStatus(
                                    context, objMeta, byteRange);
                            response.putHeader(HttpHeaders.LAST_MODIFIED,
                                    DateUtil.httpFormattedDate(so.getModificationTime()));
                            response.putHeader(HttpHeaders.ETAG, so.getETag());
                            HttpHeaderHelpers.addUserMetadataToHttpHeaders(response, objMeta,
                                    HttpHeaderHelpers.UserMetadataPrefix.V1);
                            return Optional.of(Pair.pair(so, byteRange));
                        });

        // Handle errors
        return cf.handle((val, ex) -> HttpException.handle(request, val, ex));
    }

    public AbortableBlobReadStream getObjectStream(RoutingContext context,
                                                   WSStorageObject so,
                                                   @Nullable ByteRange byteRange,
                                                   WebServerMetricsBundle bundle) {
        return backend.getObjectStream(context, so, byteRange, bundle);
    }

}
