package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.common.util.ObjectLifecycleHelper;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectLifecyclePolicy;
import com.oracle.pic.casper.webserver.api.model.PutObjectLifecyclePolicyDetails;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchLifecyclePolicyException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Vert.x HTTP handler for fetching lifecycle objects.
 *
 * This handler is relatively simple compared to the {@link PutLifecyclePolicyHandler}.  It:
 * 1. Authenticates the user
 * 2. Looks up the user's target bucket
 * 3. Looks up the object that stores the lifecycle policy for that bucket
 * 4. Reads that object, buffering it locally
 * 5. Deserializes, and then immediately reserializes the policy back out in a 200 response
 *
 * This handler does not use the V2ReadObjectHelper to begin handling the request, because it does a bit too much to
 * fit here (e.g. returning 304 on conditional ETag match, adding response header).
 */
public class GetLifecyclePolicyHandler extends CompletableHandler {

    private final Authenticator authenticator;
    private final BucketBackend bucketBackend;
    private final GetObjectBackend backend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    public GetLifecyclePolicyHandler(Authenticator authenticator,
                                     BucketBackend bucketBackend,
                                     GetObjectBackend backend,
                                     ObjectMapper mapper,
                                     EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.bucketBackend = bucketBackend;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.GET_BUCKET);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_GET_LIFECYCLE_BUNDLE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.GET_BUCKET)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();

        return VertxUtil.runAsync(rootScope, () -> {

            // Authenticate HTTP request first.
            final AuthenticationInfo authInfo = authenticator.authenticate(context);

            // Then find out whether target bucket exist or not.
            return bucketBackend.getBucket(context, authInfo, namespace, bucketName,
                    Collections.emptySet())
                    .orElseThrow(() -> new HttpException(V2ErrorCode.BUCKET_NOT_FOUND,
                        "The bucket '" + bucketName + "' does not exist in namespace '" + namespace + "'",
                        request.path()));

        }).thenComposeAsync(bucket -> {

            final String lifecycleBucketName =
                    ObjectLifecycleHelper.toLifecycleBucketName(bucket.getNamespaceName());

            // Get lifecycle object.
            if (Strings.isNullOrEmpty(bucket.getObjectLifecyclePolicyEtag()) ||
                    bucket.getObjectLifecyclePolicyEtag().equals(ObjectLifecycleHelper.CLEAR_LIFECYCLE_POLICY)) {
                throw new NoSuchLifecyclePolicyException(
                        "The bucket '" + bucket.getBucketName() + "' does not define a lifecycle policy.");
            }

            return backend.getLifecycleStorageObject(context,
                    namespace, lifecycleBucketName, bucket.getObjectLifecyclePolicyEtag());

        }, VertxExecutor.eventLoop()
        ).thenComposeAsync(so -> {

            // Get ReadStream of lifecycle object.
            final AbortableBlobReadStream abortableBlobReadStream = backend.getObjectStream(
                    context, so, null, WebServerMetrics.V2_GET_LIFECYCLE_BUNDLE);

            return HttpContentHelpers.bufferStreamData(abortableBlobReadStream)
                    .thenApply(buffer -> Pair.pair(buffer.getBytes(), so));

        }, VertxExecutor.eventLoop()
        ).thenAcceptAsync(bytesAndMetadata -> {

            final byte[] bytes = bytesAndMetadata.getFirst();
            final WSStorageObject so = bytesAndMetadata.getSecond();
            final PutObjectLifecyclePolicyDetails putObjectLifecyclePolicyDetails;
            final ObjectLifecyclePolicy objectLifecyclePolicy;

            try {
                putObjectLifecyclePolicyDetails = mapper.readValue(bytes, PutObjectLifecyclePolicyDetails.class);
                objectLifecyclePolicy = new ObjectLifecyclePolicy(so.getCreationTime().toInstant(),
                        putObjectLifecyclePolicyDetails.getItems());
            } catch (IOException e) {
                throw new HttpException(V2ErrorCode.INTERNAL_SERVER_ERROR,
                    "Internal server error, can not get objectLifecycle policy.", request.path(), e);
            }

            response.putHeader(HttpHeaders.ETAG, so.getKey().getName());
            response.putHeader(HttpHeaders.LAST_MODIFIED,
                    DateUtil.httpFormattedDate(so.getCreationTime()));

            HttpContentHelpers.writeJsonResponse(request, response, objectLifecyclePolicy, mapper);

        }, VertxExecutor.eventLoop()
        ).handle((val, ex) -> HttpException.handle(request, val, ex));
    }
}
