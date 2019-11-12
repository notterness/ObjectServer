package com.oracle.pic.casper.webserver.api.v2;

import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;

public class PostBucketReencryptHandler extends SyncBodyHandler {
    private final Authenticator authenticator;
    private final BucketBackend bucketBackend;
    private final EmbargoV3 embargoV3;
    private static final String WORK_REQUEST_ID_HEADER = "opc-work-request-id";

    public PostBucketReencryptHandler(Authenticator authenticator, BucketBackend bucketBackend,
                                      CountingHandler.MaximumContentLength contentLengthLimiter,
                                      EmbargoV3 embargoV3) {
        super(contentLengthLimiter);
        this.authenticator = authenticator;
        this.bucketBackend = bucketBackend;
        this.embargoV3 = embargoV3;
    }

    @Override
    protected void validateHeaders(RoutingContext context) {
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_POST_BUCKET_BUNDLE);
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.REENCRYPT_BUCKET);
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucket = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.REENCRYPT_BUCKET)
            .setNamespace(namespace)
            .setBucket(bucket)
            .build();
        embargoV3.enter(embargoV3Operation);

        try {
            // Authenticator verifies body SHA for PUT, POST, PATCH methods, even ReencryptBucket operation doesn't
            // contain any body, we still need to make it extend SyncBodyHandler
            final AuthenticationInfo authInfo = authenticator.authenticate(context,
                    Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));
            final String workRequestId = bucketBackend.reencryptBucketEncryptionKey(context, namespace, bucket,
                    authInfo);
            response.putHeader(WORK_REQUEST_ID_HEADER, workRequestId).setStatusCode(HttpResponseStatus.ACCEPTED).end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
