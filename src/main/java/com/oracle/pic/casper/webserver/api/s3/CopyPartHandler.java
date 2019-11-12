package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.S3AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.backend.BackendConversions;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.common.Checksum;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.CreatePartRequest;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.s3.model.CopyPartResult;
import com.oracle.pic.casper.webserver.util.CommonUtils;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class CopyPartHandler extends CompletableHandler {

    private final S3AsyncAuthenticator authenticator;
    private final PutObjectBackend putObjectBackend;
    private final WebServerConfiguration webServerConfiguration;
    private final GetObjectBackend getObjectBackend;
    private final XmlMapper mapper;
    private final DecidingKeyManagementService kms;
    private final EmbargoV3 embargoV3;
    private static final long TIMER_PERIOD_IN_MS = 15000;

    public CopyPartHandler(
            S3AsyncAuthenticator authenticator,
            PutObjectBackend putObjectBackend,
            WebServerConfiguration webServerConfiguration,
            GetObjectBackend getObjectBackend,
            XmlMapper mapper,
            DecidingKeyManagementService kms,
            EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.putObjectBackend = putObjectBackend;
        this.webServerConfiguration = webServerConfiguration;
        this.getObjectBackend = getObjectBackend;
        this.mapper = mapper;
        this.kms = kms;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_COPY_PART_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.COPY_PART);

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));
        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807

        final HttpServerRequest request = context.request();

        //fail fast on invalid range(just validate the format)
        S3HttpHelpers.validateRangeHeader(request);

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String destBucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);
        final String destObjectName = S3HttpHelpers.getObjectName(request, wsRequestContext);
        final String sourceBucketName = S3HttpHelpers.getSourceBucketName(request);
        final String sourceObjectName = S3HttpHelpers.getSourceObjectName(request);

        wsRequestContext.getCommonRequestContext().getMetricScope()
                .annotate("sourceBucketName", sourceBucketName)
                .annotate("sourceObjectName", sourceObjectName);
        final String uploadId = S3HttpHelpers.getUploadID(request);
        final int uploadPartNum = S3HttpHelpers.getPartNumberForPutPart(request);

        EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.COPY_PART)
            .setNamespace(namespace)
            .setBucket(sourceBucketName)
            .setObject(sourceObjectName)
            .build();
        embargoV3.enter(embargoOperation);

        embargoOperation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.S3)
                .setOperation(CasperOperation.UPLOAD_PART)
                .setNamespace(namespace)
                .setBucket(destBucketName)
                .setObject(destObjectName)
                .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);

        //range validation:
        //checking format of Range is done by validateRangeHeader(), and done again by getObjectStream()
        //checking legal range of Range is done by webServerConfiguration
        return authenticator.authenticate(context, contentSha256)
                .thenCompose(authInfo ->
                        getObjectBackend.getV2StorageObject(context, authInfo,
                                GetObjectBackend.ReadOperation.GET, namespace, sourceBucketName, sourceObjectName)
                                .thenApply(so -> Pair.pair(authInfo, so)))
                .thenApplyAsync(authAndSo -> {
                    final ObjectMetadata objMeta =
                            BackendConversions.wsStorageObjectSummaryToObjectMetadata(authAndSo.getSecond(), kms);
                    S3HttpHelpers.checkConditionalHeadersForCopyPart(request, objMeta);
                    final ByteRange byteRange = S3HttpHelpers.tryParseByteRange(
                            request, objMeta.getSizeInBytes());
                    HttpContentHelpers.validateByteRange(byteRange, webServerConfiguration, objMeta);
                    AbortableBlobReadStream stream = getObjectBackend.getObjectStream(
                            context,
                            authAndSo.getSecond(),
                            byteRange,
                            WebServerMetrics.S3_COPY_PART_BUNDLE);
                    CreatePartRequest req = new CreatePartRequest(
                            namespace,
                            destBucketName,
                            destObjectName,
                            uploadId,
                            uploadPartNum,
                            byteRange == null ? objMeta.getSizeInBytes() : byteRange.getLength(),
                            stream,
                            Optional.empty(),
                            Optional.empty());
                    return Pair.pair(authAndSo.getFirst(), req);
                }, VertxExecutor.eventLoop())
                .thenCompose(authAndReq -> {
                    return putObjectBackend.putPart(context, authAndReq.getFirst(), authAndReq.getSecond(),
                                () -> {
                                    S3HttpHelpers.startChunkedXmlResponse(context.response(), true);
                                    CommonUtils.startKeepConnectionAliveTimer(context, "\r\n", TIMER_PERIOD_IN_MS);
                                });
                })
                .handle((partMetadata, thrown) -> {
                    try {
                        if (partMetadata != null) {
                            CopyPartResult copyPartResult = new CopyPartResult(partMetadata.getLastModified(),
                                    Checksum.fromBase64(partMetadata.getMd5()).getQuotedHex());
                            S3HttpHelpers.endChunkedXmlResponse(context.response(), copyPartResult, mapper);
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
