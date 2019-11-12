package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.CreatePartRequest;
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

public class V2PutPartHelper {

    private final TrafficController controller;
    private final AsyncAuthenticator authenticator;
    private final PutObjectBackend backend;
    private final WebServerConfiguration webServerConfiguration;

    public V2PutPartHelper(AsyncAuthenticator authenticator,
                           PutObjectBackend backend,
                           WebServerConfiguration webServerConfiguration,
                           TrafficController controller) {
        this.controller = controller;
        this.authenticator = authenticator;
        this.backend = backend;
        this.webServerConfiguration = webServerConfiguration;
    }

    public CompletableFuture<Void> handleCompletably(RoutingContext context,
                                                     String namespace,
                                                     String bucketName,
                                                     String objectName,
                                                     String uploadId,
                                                     int uploadPartNum) {
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_POST_UPLOAD_PART_BUNDLE);
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        // Vert.x reads the request in the background by default, but we want to stream it through a state machine, so
        // we tell Vert.x to pause the reading. The request is resumed in the PutStateMachine.
        request.pause();

        HttpContentHelpers.negotiateStorageObjectContent(request, 1,
                webServerConfiguration.getMaxUploadPartSize().longValue(BYTE));
        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.STAR_ONLY);

        final long contentLength = Long.parseLong(request.getHeader(HttpHeaders.CONTENT_LENGTH));
        final Optional<String> contentMd5 = ChecksumHelper.getContentMD5Header(request);
        final Optional<String> ifMatchEtag = Optional.ofNullable(request.getHeader(HttpHeaders.IF_MATCH));
        final Optional<String> ifNoneMatchEtag = Optional.ofNullable(request.getHeader(HttpHeaders.IF_NONE_MATCH));
        final AbortableBlobReadStream abortableBlobReadStream = HttpContentHelpers.readStream(
                wsRequestContext, request, WebServerMetrics.V2_POST_UPLOAD_PART_BUNDLE, contentMd5, contentLength);

        // Acquire can throw an exception(with err code of 503 or 429) if the request couldn't be accepted
        controller.acquire(namespace, TrafficRecorder.RequestType.PutObject, contentLength);

        return authenticator.authenticatePutObject(context)
                .thenComposeAsync(authInfo -> {
                    final CreatePartRequest createPartReq = new CreatePartRequest(
                            namespace,
                            bucketName,
                            objectName,
                            uploadId,
                            uploadPartNum,
                            contentLength,
                            abortableBlobReadStream,
                            ifMatchEtag,
                            ifNoneMatchEtag);
                    return backend.putPart(context, authInfo, createPartReq);
                }, VertxExecutor.eventLoop())
                .thenAcceptAsync(partMeta -> {
                    response.putHeader(HttpHeaders.ETAG, partMeta.getEtag());
                    response.putHeader(HttpHeaderHelpers.OPC_CONTENT_MD5_HEADER, partMeta.getMd5());
                    response.end();
                }, VertxExecutor.eventLoop());
    }
}
