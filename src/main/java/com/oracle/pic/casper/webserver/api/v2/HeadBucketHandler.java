package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.model.Bucket;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * Vert.x HTTP handler to check for the existence of a bucket.
 */
public class HeadBucketHandler extends SyncHandler {
    private final Authenticator authenticator;
    private final BucketBackend backend;
    private final EmbargoV3 embargoV3;

    public HeadBucketHandler(Authenticator authenticator, BucketBackend backend, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.HEAD_BUCKET);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_HEAD_BUCKET_BUNDLE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.HEAD_BUCKET)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.YES);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(context);
            final Bucket bucket = backend.headBucket(context, authInfo, namespace, bucketName)
                    .orElseThrow(() -> new HttpException(V2ErrorCode.BUCKET_NOT_FOUND,
                        "The bucket '" + bucketName + "' does not exist in namespace '" + namespace + "'",
                        request.path()));

            if (HttpMatchHelpers.checkConditionalHeaders(request, bucket.getETag())) {
                response.putHeader(HttpHeaders.ETAG, bucket.getETag())
                        .setStatusCode(HttpResponseStatus.NOT_MODIFIED)
                        .end();
            } else {
                response.putHeader(HttpHeaders.ETAG, bucket.getETag())
                        .setStatusCode(HttpResponseStatus.OK)
                        .end();
            }
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
