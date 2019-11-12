package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.bmc.objectstorage.model.CopyPartDetails;
import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BackendConversions;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.common.AsyncBodyHandler;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.WSExceptionTranslator;
import com.oracle.pic.casper.webserver.api.model.CopyPartResult;
import com.oracle.pic.casper.webserver.api.model.CreatePartRequest;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.CommonUtils;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class CopyPartHandler extends AsyncBodyHandler {

    private final AsyncAuthenticator asyncAuthenticator;
    private final PutObjectBackend putObjectBackend;
    private final WebServerConfiguration webServerConfiguration;
    private final GetObjectBackend getObjectBackend;
    private final ObjectMapper mapper;
    private final DecidingKeyManagementService kms;
    private final WSExceptionTranslator wsExceptionTranslator;
    private final EmbargoV3 embargoV3;

    public CopyPartHandler(
            AsyncAuthenticator asyncAuthenticator,
            PutObjectBackend putObjectBackend,
            WebServerConfiguration webServerConfiguration,
            GetObjectBackend getObjectBackend,
            ObjectMapper mapper,
            DecidingKeyManagementService kms,
            WSExceptionTranslator wsExceptionTranslator,
            CountingHandler.MaximumContentLength maximumContentLength,
            EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.asyncAuthenticator = asyncAuthenticator;
        this.putObjectBackend = putObjectBackend;
        this.webServerConfiguration = webServerConfiguration;
        this.getObjectBackend = getObjectBackend;
        this.mapper = mapper;
        this.kms = kms;
        this.wsExceptionTranslator = wsExceptionTranslator;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context, Buffer buffer) {
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_COPY_PART_BUNDLE);
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.COPY_PART);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));
        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807

        final String destNamespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String destBucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String destObjectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);
        final CopyPartDetails copyPartDetails = HttpContentHelpers.readCopyPartDetails(request.path(),
                mapper, buffer.getBytes());

        final String sourceNamespace = copyPartDetails.getSourceNamespace();
        final String sourceBucketName = copyPartDetails.getSourceBucket();
        final String sourceObjectName = copyPartDetails.getSourceObject();

        EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.COPY_PART)
            .setNamespace(sourceNamespace)
            .setBucket(sourceBucketName)
            .setObject(sourceObjectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        embargoV3Operation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.V2)
                .setOperation(CasperOperation.UPLOAD_PART)
                .setNamespace(destNamespace)
                .setBucket(destBucketName)
                .setObject(destObjectName)
                .build();
        embargoV3.enter(embargoV3Operation);

        MetricScope metricScope = WSRequestContext.getCommonRequestContext(context).getMetricScope();
        metricScope
                .annotate("sourceNamespace", sourceNamespace)
                .annotate("sourceBucketName", sourceBucketName)
                .annotate("sourceObjectName", sourceObjectName)
                .annotate("destNamespace", destNamespace)
                .annotate("destBucketName", destBucketName)
                .annotate("destObjectName", destObjectName);

        if (StringUtils.isNotBlank(copyPartDetails.getRange())) {
            metricScope.annotate("range", copyPartDetails.getRange());
        }

        final String uploadId = HttpPathQueryHelpers.getUploadId(request);
        final int uploadPartNum = HttpPathQueryHelpers.getUploadPartNumParam(request);

        return asyncAuthenticator.authenticate(context,
                Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(buffer.getBytes()).asBytes()))
                .thenComposeAsync(authInfo -> getObjectBackend.getV2StorageObject(context, authInfo,
                        GetObjectBackend.ReadOperation.GET, copyPartDetails.getSourceNamespace(),
                        copyPartDetails.getSourceBucket(), copyPartDetails.getSourceObject())
                                .thenApply(so -> {
                                    final ObjectMetadata objMeta =
                                            BackendConversions.wsStorageObjectSummaryToObjectMetadata(so, this.kms);

                                    HttpMatchHelpers.checkConditionalHeaders(request, objMeta.getETag());

                                    final ByteRange byteRange = HttpContentHelpers.tryParseByteRange(
                                            copyPartDetails.getRange(), objMeta.getSizeInBytes());

                                    HttpContentHelpers.validateByteRange(byteRange, webServerConfiguration, objMeta);

                                    AbortableBlobReadStream stream = getObjectBackend.getObjectStream(
                                            context,
                                            so,
                                            byteRange,
                                            WebServerMetrics.V2_COPY_PART_BUNDLE);

                                    CommonUtils.startKeepConnectionAliveTimer(context, () -> {
                                        if (!response.headWritten()) {
                                            // set to chunked only if timer is invoked at least once so that errors
                                            // until then can be properly passed on.
                                            response.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON);
                                            response.setStatusCode(HttpResponseStatus.OK);
                                            context.response().setChunked(true);
                                        }
                                    });

                                    CreatePartRequest req = new CreatePartRequest(
                                            destNamespace,
                                            destBucketName,
                                            destObjectName,
                                            uploadId,
                                            uploadPartNum,
                                            byteRange == null ? objMeta.getSizeInBytes() : byteRange.getLength(),
                                            stream,
                                            Optional.empty(),
                                            Optional.empty());
                                    return Pair.pair(authInfo, req);
                                }), VertxExecutor.eventLoop())
                .thenComposeAsync(authInfoAndRequest ->
                        putObjectBackend.putPart(context,
                                authInfoAndRequest.getFirst(),
                                authInfoAndRequest.getSecond()), VertxExecutor.eventLoop())
                .handleAsync((partMetadata, thrown) -> {
                    if (thrown != null) {
                        if (response.headWritten()) {
                            // if fails after the timer for keep alive is called then send error message only.
                            writeException(response, HttpException.rewrite(request, thrown));
                        }
                        throw HttpException.rewrite(request, thrown);
                    }

                    CopyPartResult copyPartResult = new CopyPartResult(partMetadata.getEtag());
                    if (response.headWritten()) {
                        try {
                            response.write(mapper.writeValueAsString(copyPartResult));
                            response.end();
                        } catch (IOException e) {
                            writeException(response, HttpException.rewrite(request, e));
                            throw HttpException.rewrite(request, e);
                        }
                    } else {
                        HttpContentHelpers.writeJsonResponse(request, response, copyPartResult, mapper);
                    }
                    return null;
                }, VertxExecutor.eventLoop());
    }

    private void writeException(HttpServerResponse response, RuntimeException rewrite) {
        // after setting response as chunked there is no way to send another status code.
        // send the error message.
        response.write(wsExceptionTranslator.getResponseErrorMessage(rewrite));
        // failure handler expects the response to be ended in the individual service if the headers are
        // already written.
        response.end();
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {
        HttpMatchHelpers.validateConditionalHeaders(context.request(),
                HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.YES);
    }
}
