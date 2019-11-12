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

public class PostBucketPurgeDeletedTagsHandler extends SyncBodyHandler {
    private final Authenticator authenticator;
    private final BucketBackend backend;
    private final EmbargoV3 embargoV3;

    public PostBucketPurgeDeletedTagsHandler(Authenticator authenticator, BucketBackend backend,
                                             CountingHandler.MaximumContentLength maximumContentLength,
                                             EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_POST_BUCKET_PURGE_DELETED_TAGS_BUNDLE);
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.PURGE_DELETED_TAGS);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.PURGE_DELETED_TAGS)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        try {
            // Authenticator verifies body SHA for PUT, POST, PATCH methods, even purgeDeletedTag operation doesn't
            // contain any body, we still need to make it extend SyncBodyHandler
            final AuthenticationInfo authInfo = authenticator.authenticate(context,
                    Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));
            backend.purgeDeletedDefinedTags(context, authInfo, namespace, bucketName);
            response.setStatusCode(HttpResponseStatus.OK).end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
