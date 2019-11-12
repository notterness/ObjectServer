package com.oracle.pic.casper.webserver.api.s3;

import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

/**
 * Vert.x HTTP handler for S3 Api to check for the existence of a bucket.
 */
public class HeadBucketHandler extends SyncHandler {
    private final S3Authenticator authenticator;
    private final BucketBackend backend;
    private final EmbargoV3 embargoV3;

    public HeadBucketHandler(S3Authenticator authenticator, BucketBackend backend, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_HEAD_BUCKET_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.HEAD_BUCKET);

        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.HEAD_BUCKET)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);

        try {
            AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
            backend.headBucket(context, authInfo, namespace, bucketName)
                    .orElseThrow(() -> new NoSuchBucketException("The specified bucket does not exist."));
            context.response().putHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_XML)
                .putHeader(S3Api.BUCKET_REGION, ConfigRegion.fromSystemProperty().getFullName())
                .setStatusCode(HttpResponseStatus.OK).end();
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
