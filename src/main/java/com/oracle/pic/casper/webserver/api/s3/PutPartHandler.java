package com.oracle.pic.casper.webserver.api.s3;

import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.S3AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.common.Checksum;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.CreatePartRequest;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static javax.measure.unit.NonSI.BYTE;

public class PutPartHandler extends CompletableHandler {

    private final TrafficController controller;
    private final S3AsyncAuthenticator authenticator;
    private final PutObjectBackend putObjectBackend;
    private final WebServerConfiguration webServerConfiguration;
    private final EmbargoV3 embargoV3;

    public PutPartHandler(TrafficController controller,
                          S3AsyncAuthenticator authenticator,
                          PutObjectBackend putObjectBackend,
                          WebServerConfiguration webServerConfiguration,
                          EmbargoV3 embargoV3) {
        this.controller = controller;
        this.authenticator = authenticator;
        this.putObjectBackend = putObjectBackend;
        this.webServerConfiguration = webServerConfiguration;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_PUT_UPLOAD_PART_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.UPLOAD_PART);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        request.pause();

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);
        final String objectName = S3HttpHelpers.getObjectName(request, wsRequestContext);

        final String uploadId = S3HttpHelpers.getUploadID(request);
        final int uploadPartNum = S3HttpHelpers.getPartNumberForPutPart(request);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.UPLOAD_PART)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);

        HttpContentHelpers.negotiateStorageObjectContent(request, 1,
            webServerConfiguration.getMaxUploadPartSize().longValue(BYTE));

        final long contentLength = Long.parseLong(request.getHeader(HttpHeaders.CONTENT_LENGTH));
        final Optional<String> contentMd5 = ChecksumHelper.getContentMD5Header(request);
        final AbortableBlobReadStream abortableBlobReadStream = HttpContentHelpers.readStream(wsRequestContext, request,
            WebServerMetrics.S3_PUT_UPLOAD_PART_BUNDLE, contentMd5, contentLength);

        // Acquire can throw an exception(with err code of 503 or 429) if the request couldnt be accepted
        controller.acquire(namespace, TrafficRecorder.RequestType.PutObject, contentLength);

        return authenticator.authenticate(context, contentSha256)
            .thenComposeAsync(authInfo -> {
                final CreatePartRequest createPartRequest = new CreatePartRequest(
                        namespace,
                        bucketName,
                        objectName,
                        uploadId,
                        uploadPartNum,
                        contentLength,
                        abortableBlobReadStream,
                        Optional.empty(),
                        Optional.empty());
                return putObjectBackend.putPart(context, authInfo, createPartRequest);
            }, VertxExecutor.eventLoop())
            .thenAcceptAsync(partMetadata -> {
                Checksum.fromBase64(partMetadata.getMd5()).addHeaderToResponseS3(response);
                response.end();
            }, VertxExecutor.eventLoop())
            .exceptionally(thrown -> {
                throw S3HttpException.rewrite(context, thrown);
            });
    }
}
