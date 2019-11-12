package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.ETagType;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.S3AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.backend.BackendConversions;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.MetadataUpdater;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.s3.model.CopyObjectDetails;
import com.oracle.pic.casper.webserver.api.s3.model.CopyObjectResult;
import com.oracle.pic.casper.webserver.util.CommonUtils;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class CopyObjectHandler extends CompletableHandler {

    private static final long TIMER_PERIOD_IN_MS = 15000;
    private static final long MAX_S3_COPY_OBJECT_SIZE = 5368709120L;

    private final S3AsyncAuthenticator authenticator;
    private final PutObjectBackend putObjectBackend;
    private final GetObjectBackend getObjectBackend;
    private final XmlMapper mapper;
    private final DecidingKeyManagementService kms;
    private final EmbargoV3 embargoV3;

    public CopyObjectHandler(
        S3AsyncAuthenticator authenticator,
        PutObjectBackend putObjectBackend,
        GetObjectBackend getObjectBackend,
        XmlMapper mapper,
        DecidingKeyManagementService kms,
        EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.putObjectBackend = putObjectBackend;
        this.getObjectBackend = getObjectBackend;
        this.mapper = mapper;
        this.kms = kms;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_COPY_OBJECT_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.PUT_OBJECT);

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.COPYOBJECT_REQUEST_COUNT);

        final HttpServerRequest request = context.request();
        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String destinationObject = S3HttpHelpers.getObjectName(request, wsRequestContext);
        final String destinationBucket = S3HttpHelpers.getBucketName(request, wsRequestContext);
        final String sourceBucket = S3HttpHelpers.getSourceBucketName(request);
        final String sourceObject = S3HttpHelpers.getSourceObjectName(request);

        Preconditions.checkArgument(!sourceBucket.equals(destinationBucket) || !sourceObject.equals(destinationObject),
                "Bad routing of UpdateMetadata request.");

        return authenticator.authenticate(context, contentSha256)
                .thenCompose(authInfo ->
                        // Read the storage object from the backend
                        getObjectBackend.getV2StorageObject(context, authInfo,
                                GetObjectBackend.ReadOperation.GET, namespace, sourceBucket, sourceObject)
                                .thenApply(so -> Pair.pair(authInfo, so)))
                .thenCompose(authAndSo -> {
                    final ObjectMetadata sourceObjectMetadata =
                            BackendConversions.wsStorageObjectSummaryToObjectMetadata(authAndSo.getSecond(), kms);

                    final String storageClass = S3StorageClass.valueFrom(sourceObjectMetadata).toString();
                    if (storageClass.equalsIgnoreCase(S3StorageClass.GLACIER.toString())) {
                        throw new S3HttpException(S3ErrorCode.INVALID_OBJECT_STATE, "Object is of storage class " +
                                "GLACIER. Unable to perform copy operations on GLACIER objects. You must restore the " +
                                "object to be able to the perform operation.",
                                // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                                request.path().replace("/%2F", "//"));
                    }

                    final String storageClassHeader = S3HttpHelpers.unquoteIfNeeded(
                            request.getHeader(S3Api.STORAGE_CLASS_HEADER));
                    if (storageClassHeader != null
                            && !storageClassHeader.equalsIgnoreCase(S3StorageClass.STANDARD.toString())) {
                        throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT, storageClassHeader + " is not allowed.",
                                // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                                request.path().replace("/%2F", "//"));
                    }

                    final String ifMatch = S3HttpHelpers.unquoteIfNeeded(
                            request.getHeader(S3Api.COPY_SOURCE_IF_MATCH));
                    final String ifNoneMatch = S3HttpHelpers.unquoteIfNeeded(
                            request.getHeader(S3Api.COPY_SOURCE_IF_NONE_MATCH));
                    final Date ifModifiedSince = DateUtil.s3HttpHeaderStringToDate(S3HttpHelpers.unquoteIfNeeded(
                            request.getHeader(S3Api.COPY_SOURCE_IF_MODIFIED_SINCE)));
                    final Date ifUnmodifiedSince = DateUtil.s3HttpHeaderStringToDate(S3HttpHelpers.unquoteIfNeeded(
                            request.getHeader(S3Api.COPY_SOURCE_IF_UNMODIFIED_SINCE)));

                    final String metadataDirective = S3HttpHelpers.unquoteIfNeeded(
                            request.getHeader(S3Api.METADATA_DIRECTIVE_HEADER));
                    final Map<String, String> newUserMetadata = HttpHeaderHelpers.getUserMetadataHeaders(
                            request, HttpHeaderHelpers.UserMetadataPrefix.S3);
                    if (metadataDirective != null && ((metadataDirective.equals("REPLACE") && newUserMetadata.isEmpty())
                            || (!metadataDirective.equals("COPY") && !metadataDirective.equals("REPLACE")))) {
                        throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT,
                                "x-amz-metadata-directive should be equal to COPY or be equal to REPLACE with " +
                                        "new user metadata specified",
                                // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                                request.path().replace("/%2F", "//"));
                    }
                    CopyObjectDetails copyObjectDetails = new CopyObjectDetails(
                            sourceBucket,
                            sourceObject,
                            destinationBucket,
                            destinationObject,
                            ifMatch,
                            ifNoneMatch,
                            ifUnmodifiedSince != null ? ifUnmodifiedSince.toInstant() : null,
                            ifModifiedSince != null ? ifModifiedSince.toInstant() : null,
                            newUserMetadata);

                    S3HttpHelpers.checkConditionalHeadersForCopy(context.request(), copyObjectDetails,
                            sourceObjectMetadata);

                    if (sourceObjectMetadata.getSizeInBytes() > MAX_S3_COPY_OBJECT_SIZE) {
                        throw new S3HttpException(S3ErrorCode.INVALID_REQUEST,
                                "The specified copy source is larger than the maximum allowable size for a copy " +
                                        "source: " + MAX_S3_COPY_OBJECT_SIZE,
                                // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                                request.path().replace("/%2F", "//"));
                    }

                    // Read the object stream from getObjectBackend
                    AbortableBlobReadStream stream = getObjectBackend.getObjectStream(
                            context,
                            authAndSo.getSecond(),
                            null,
                            WebServerMetrics.S3_COPY_OBJECT_BUNDLE);

                    // Prepare the user metadata for the destination object
                    Map<String, String> destinationUserMetadata = sourceObjectMetadata.getMetadata();
                    if (metadataDirective != null && metadataDirective.equals("REPLACE")) {
                        final Map<String, String> oldUserMetadata = authAndSo.getSecond().getMetadata(kms)
                                .entrySet().stream()
                                .filter(entry ->
                                        HttpHeaderHelpers.PRESERVED_CONTENT_HEADERS_LOOKUP.containsKey(entry.getKey()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                        destinationUserMetadata = MetadataUpdater.updateMetadata(false,
                                oldUserMetadata, newUserMetadata);
                    }

                    ObjectMetadata copyObjectMetadata = new ObjectMetadata(sourceObjectMetadata.getNamespace(),
                            destinationBucket,
                            destinationObject,
                            sourceObjectMetadata.getSizeInBytes(),
                            sourceObjectMetadata.getChecksum(),
                            destinationUserMetadata,
                            sourceObjectMetadata.getCreationTime(),
                            sourceObjectMetadata.getModificationTime(),
                            null,
                            sourceObjectMetadata.getArchivedTime().orElse(null),
                            sourceObjectMetadata.getRestoredTime().orElse(null),
                            sourceObjectMetadata.getArchivalState()
                    );

                    final EmbargoV3Operation readOperation = EmbargoV3Operation.builder()
                        .setApi(EmbargoV3Operation.Api.S3)
                        .setOperation(CasperOperation.GET_OBJECT)
                        .setNamespace(namespace)
                        .setBucket(sourceBucket)
                        .setObject(sourceObject)
                        .build();
                    embargoV3.enter(readOperation);
                    final EmbargoV3Operation writeOperation = EmbargoV3Operation.builder()
                        .setApi(EmbargoV3Operation.Api.S3)
                        .setOperation(CasperOperation.PUT_OBJECT)
                        .setNamespace(namespace)
                        .setBucket(destinationBucket)
                        .setObject(destinationObject)
                        .build();
                    embargoV3.enter(writeOperation);

                    // Write the object into destination
                    return putObjectBackend.createOrOverwriteObject(context,
                            authAndSo.getFirst(),
                            copyObjectMetadata,
                            curObjMeta -> {
                                S3HttpHelpers.startChunkedXmlResponse(context.response(), true);
                                CommonUtils.startKeepConnectionAliveTimer(context, "\r\n", TIMER_PERIOD_IN_MS);
                            },
                            stream,
                            Api.V2,
                            null /* expectedMD5 */,
                            null /* expectedSHA256 */,
                            null,
                            null,
                            null,
                            null,
                            null,
                            ETagType.MD5);
                })
                .handle((destinationObjectMetadata, thrown) -> {

                    try {
                        if (destinationObjectMetadata != null) {
                            CopyObjectResult result = new CopyObjectResult(
                                    destinationObjectMetadata.getChecksum() == null ?
                                            null : destinationObjectMetadata.getChecksum().getQuotedHex(),
                                    destinationObjectMetadata.getModificationTime());
                            S3HttpHelpers.endChunkedXmlResponse(context.response(), result, mapper);
                        } else {
                            S3HttpHelpers.endChunkedXmlErrorResponse(context, thrown, mapper);
                            throw S3HttpException.rewrite(context, thrown);
                        }
                    } catch (JsonProcessingException e) {
                        S3HttpHelpers.endChunkedXmlErrorResponse(context, e, mapper);
                        throw S3HttpException.rewrite(context, e);
                    }
                    return null;
                });
    }
}
