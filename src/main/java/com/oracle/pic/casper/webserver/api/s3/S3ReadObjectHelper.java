package com.oracle.pic.casper.webserver.api.s3;

import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.exceptions.NotFoundException;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.common.vertx.HttpServerUtil;
import com.oracle.pic.casper.common.vertx.stream.AbortableReadStream;
import com.oracle.pic.casper.webserver.api.auth.S3AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.backend.BackendConversions;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers.UserMetadataPrefix;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetricsBundle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.NotImplementedException;

import java.util.concurrent.CompletableFuture;

import static com.oracle.pic.casper.common.util.DateUtil.httpFormattedDate;
import static io.vertx.core.http.HttpHeaders.LAST_MODIFIED;

/**
 * Vert.x HTTP handler for fetching storage objects.
 */
public class S3ReadObjectHelper {
    private final S3AsyncAuthenticator authenticator;
    private final GetObjectBackend backend;
    private final DecidingKeyManagementService kms;

    public S3ReadObjectHelper(S3AsyncAuthenticator authenticator,
                              GetObjectBackend backend,
                              DecidingKeyManagementService kms) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.kms = kms;
    }

    public CompletableFuture<Pair<WSStorageObject, ByteRange>> beginHandleCompletably(
            RoutingContext context,
            WebServerMetricsBundle bundle,
            GetObjectBackend.ReadOperation readOperation) {
        MetricsHandler.addMetrics(context, bundle);
        final HttpServerRequest request = context.request();
        HttpServerResponse response = context.response();

        final String firstParam = HttpServerUtil.getRoutingParam(request);
        if (firstParam != null) {
            if (S3Api.UNSUPPORTED_CONFIGURATION_PARAMS.contains(firstParam) &&
                    readOperation == GetObjectBackend.ReadOperation.GET) {
                throw new NotImplementedException(String.format(
                        "S3 Get Object %s operation is not supported.", firstParam));
            }
            throw new NotFoundException("Not Found");
        }
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);
        final String objectName = S3HttpHelpers.getObjectName(request, wsRequestContext);
        HttpHeaderHelpers.validateRangeHeader(request);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);

        return authenticator.authenticate(context, contentSha256)
            .thenCompose(authInfo ->
                backend.getV2StorageObject(context, authInfo, readOperation, namespace, bucketName, objectName))
            .thenApply(so -> {
                final ObjectMetadata objMeta = BackendConversions.wsStorageObjectSummaryToObjectMetadata(so, kms);
                S3HttpHelpers.checkConditionalHeaders(request, objMeta);
                final ByteRange byteRange = HttpHeaderHelpers.tryParseByteRange(
                        context.request(), objMeta.getSizeInBytes());

                /*
                'x-amz-storage-class' Header: Provides storage class information of the object only if not 'STANDARD',
                else remove the header
                */
                final String storageClass = S3StorageClass.valueFrom(objMeta).toString();
                if (storageClass.equalsIgnoreCase(S3StorageClass.STANDARD.toString())) {
                    response.headers().remove(S3Api.STORAGE_CLASS_HEADER);
                } else {
                    response.putHeader(S3Api.STORAGE_CLASS_HEADER, storageClass);
                }

                /*
                'x-amz-restore' Header: Provides restoration status of the object, including expiry if applicable
                    "Restored: : returned the value ongoing-request="false" and expiry-date set to the time where this
                                 restored object will expire, and will need to be restored again in order to be accessed
                    "Restoring": return the value ongoing-request="true", and no expiry-date set
                */
                if (so.getArchivalState() == ArchivalState.Restored) {
                    final String expiryDate = DateUtil.httpFormattedDate(so.getArchivedTime().get());

                    response.putHeader(S3Api.RESTORE_HEADER,
                            String.format("ongoing-request=\"%s\" expiry-date=\"%s\"", "false", expiryDate));
                } else if (so.getArchivalState() == ArchivalState.Restoring) {
                    response.putHeader(S3Api.RESTORE_HEADER, "ongoing-request=\"true\"");
                }
                ServiceLogsHelper.logServiceEntry(context);
                HttpContentHelpers.writeReadObjectResponseHeadersAndStatus(context, objMeta, byteRange);
                response.putHeader(LAST_MODIFIED, httpFormattedDate(objMeta.getModificationTime()));
                objMeta.getChecksum().addHeaderToResponseS3(response);
                HttpHeaderHelpers.addUserMetadataToHttpHeaders(response, objMeta, UserMetadataPrefix.S3);
                return Pair.pair(so, byteRange);
            });
    }

    public AbortableReadStream<Buffer> getObjectStream(RoutingContext context,
                                                       WSStorageObject so,
                                                       ByteRange byteRange,
                                                       WebServerMetricsBundle bundle) {
        return backend.getObjectStream(context, so, byteRange, bundle);
    }
}
