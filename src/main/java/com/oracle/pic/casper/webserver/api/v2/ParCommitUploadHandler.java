package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.ParAuthenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.UploadIdentifier;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

public class ParCommitUploadHandler extends CompletableHandler {

    private final ParAuthenticator authenticator;
    private final Backend backend;
    private final EmbargoV3 embargoV3;

    public ParCommitUploadHandler(ParAuthenticator authenticator, Backend backend, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.COMMIT_MULTIPART_UPLOAD);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_COMMIT_UPLOAD_BUNDLE);

        final HttpServerRequest request = context.request();
        HttpMatchHelpers.validateConditionalHeaders(request,
                HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR, HttpMatchHelpers.IfNoneMatchAllowed.STAR_ONLY);
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);
        final String uploadId = HttpPathQueryHelpers.getUploadId(request);
        final UploadIdentifier uploadIdentifier = new UploadIdentifier(namespace, bucketName, objectName, uploadId);
        final String ifMatch = request.getHeader(HttpHeaders.IF_MATCH);
        final String ifNoneMatch = request.getHeader(HttpHeaders.IF_NONE_MATCH);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.COMMIT_MULTIPART_UPLOAD)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        return authenticator.authenticate(context)
                .thenApplyAsync(authInfo -> backend.finishMultipartUploadViaPars(context, authInfo, uploadIdentifier,
                        ifMatch, ifNoneMatch), VertxExecutor.workerThread())
                .handle((val, ex) -> HttpException.handle(request, val, ex))
                .thenAccept(objMeta -> {
                    response.putHeader(HttpHeaders.ETAG, objMeta.getETag())
                            .putHeader(HttpHeaders.LAST_MODIFIED,
                                    DateUtil.httpFormattedDate(objMeta.getModificationTime()));
                    objMeta.getChecksum().addHeaderToResponseV2Put(response);
                    response.end();
                });
    }
}
