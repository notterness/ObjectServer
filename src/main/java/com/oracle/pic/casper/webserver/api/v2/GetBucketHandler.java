package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.Bucket;
import com.oracle.pic.casper.webserver.api.model.BucketProperties;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Set;

/**
 * Vert.x HTTP handler for fetching buckets.
 */
public class GetBucketHandler extends SyncHandler {
    private final Authenticator authenticator;
    private final ObjectMapper mapper;
    private final BucketBackend backend;
    private final EmbargoV3 embargoV3;

    public GetBucketHandler(Authenticator authenticator, ObjectMapper mapper, BucketBackend backend,
                            EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.mapper = mapper;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.GET_BUCKET);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_GET_BUCKET_BUNDLE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.YES);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.GET_BUCKET)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        final Set<BucketProperties> bucketProperties =
                HttpPathQueryHelpers.getRequestedBucketParametersForList(request, BucketProperties.APPROXIMATE_COUNT,
                        BucketProperties.APPROXIMATE_SIZE);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(context);
            final Bucket bucket = backend.getBucket(context, authInfo, namespace, bucketName, bucketProperties)
                    .orElseThrow(() -> new HttpException(V2ErrorCode.BUCKET_NOT_FOUND,
                            "The bucket '" + bucketName + "' does not exist in namespace '" + namespace + "'",
                            request.path()));

            if (HttpMatchHelpers.checkConditionalHeaders(request, bucket.getETag())) {
                response.putHeader(HttpHeaders.ETAG, bucket.getETag())
                        .setStatusCode(HttpResponseStatus.NOT_MODIFIED)
                        .end();
            } else {
                FilterProvider filter = null;
                // GetBucket can return the approximate size and object count of a bucket.
                // The problem is that these values can change without the bucket etag changing!
                // (Creating/Modifying/Deleting objects doesn't change the bucket's etag).
                // Set the cache control header to tell clients not to cache the value returned
                // by GetBucket. This change should result in the OCI Console showing bucket sizes correctly.
                HttpContentHelpers.writeJsonResponse(
                        request, response, bucket, mapper, filter, HttpHeaderHelpers.etagHeader(bucket.getETag()),
                        HttpHeaderHelpers.cacheControlHeader("no-store"));
            }
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
