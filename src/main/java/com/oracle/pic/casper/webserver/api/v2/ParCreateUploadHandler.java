package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.ParAuthenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.CreateUploadRequest;
import com.oracle.pic.casper.webserver.api.model.ParUploadMetadata;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.eclipse.jetty.util.URIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ParCreateUploadHandler extends CompletableHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ParCreateUploadHandler.class);

    private final ParAuthenticator authenticator;
    private final Backend backend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    public ParCreateUploadHandler(ParAuthenticator authenticator, Backend backend, ObjectMapper mapper,
                                  EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.CREATE_MULTIPART_UPLOAD);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_CREATE_UPLOAD_BUNDLE);

        final HttpServerRequest request = context.request();
        HttpMatchHelpers.validateConditionalHeaders(
                request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR, HttpMatchHelpers.IfNoneMatchAllowed.STAR_ONLY);
        final String path = request.path();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.CREATE_MULTIPART_UPLOAD)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        final String ifMatch = request.getHeader(HttpHeaders.IF_MATCH);
        final String ifNoneMatch = request.getHeader(HttpHeaders.IF_NONE_MATCH);
        final Map<String, String> metadata = HttpHeaderHelpers.getUserMetadataHeaders(request);

        return authenticator.authenticate(context)
                .thenApplyAsync(authInfo -> {
                    CreateUploadRequest createUpload = new CreateUploadRequest(namespace, bucketName, objectName,
                            metadata, ifMatch, ifNoneMatch);
                    return backend.beginMultipartUpload(context, authInfo, createUpload);
                }, VertxExecutor.workerThread())
                .handle((val, ex) -> HttpException.handle(request, val, ex))
                .thenAccept(uploadMetadata -> {
                    LOG.debug("created multipart upload {}", uploadMetadata.getUploadId());
                    // request path is /p/nonce/n/namespace/b/bucket/o/myObject
                    // want to construct /p/nonce/n/namespace/b/bucket/u/myObject/id/uploadId/
                    // object name myObject needs to be encoded so that http clients can safely parse it
                    final String accessUri = path.substring(0, path.indexOf("/o/")) + "/u/" +
                            URIUtil.encodePath(objectName) + "/id/" + uploadMetadata.getUploadId() + "/";
                    final ParUploadMetadata parUploadMetadata = new ParUploadMetadata(
                            uploadMetadata.getNamespace(),
                            uploadMetadata.getBucketName(),
                            uploadMetadata.getObjectName(),
                            uploadMetadata.getUploadId(),
                            uploadMetadata.getTimeCreated(),
                            accessUri);
                    HttpContentHelpers.writeJsonResponse(request, context.response(), parUploadMetadata, mapper);
                });
    }
}
