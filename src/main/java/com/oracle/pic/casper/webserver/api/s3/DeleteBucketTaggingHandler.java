package com.oracle.pic.casper.webserver.api.s3;

import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.HashMap;

/**
 * Vert.x HTTP handler for S3 Api to delete tagging from a given bucket.
 */
public class DeleteBucketTaggingHandler extends SyncHandler {

    private final S3Authenticator authenticator;
    private final BucketBackend bucketBackend;
    private final EmbargoV3 embargoV3;

    public DeleteBucketTaggingHandler(S3Authenticator authenticator, BucketBackend bucketBackend, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.bucketBackend = bucketBackend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handle(RoutingContext context) {
        super.handle(context);
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_DELETE_BUCKET_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.UPDATE_BUCKET);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.UPDATE_BUCKET)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);

        try {
            AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
            // send empty hashMap to updateBucketTags to actually delete, NOTE don't pass null, which means do nothing.
            bucketBackend.updateBucketTags(context, authInfo, namespace, bucketName, new HashMap<>(), new HashMap<>());
            response.setStatusCode(HttpResponseStatus.OK).end();
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
