package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.common.util.ObjectLifecycleHelper;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.lifecycle.LifecycleEngineClient;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableBodyHandler;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.CreateLifecycleObjectExchange;
import com.oracle.pic.casper.webserver.api.model.ObjectLifecyclePolicy;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.PutObjectLifecyclePolicyDetails;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

/**
 * Vert.x HTTP handler for creating or overwriting lifecycle policies.
 *
 * This handler manages a lot of business logic.  At a high level, it takes these actions:
 * 1. Authenticates the user
 * 2. Deserializes and validates the lifecycle policy from the user's request
 * 3. Retrieves or creates the internal bucket that holds the tenancy's lifecycle policies
 * 4. Creates a lifecycle policy object in that internal bucket
 * 5. Cancels any ongoing execution of the previous policy on the user's target bucket
 * 6. Updates the target bucket's lifecycle policy ETag
 * 7. Returns the user's lifecycle policy back to them, along with its new ETag and creation timestamp
 *
 * This handler does not currently clean up old lifecycle policy objects.  When you or overwrite (or delete) a
 * lifecycle policy, the object that stored the old policy is simply leaked.  See
 * https://jira.oci.oraclecorp.com/browse/CASPER-4668
 */
public class PutLifecyclePolicyHandler extends CompletableHandler {

    private final Authenticator authenticator;
    private final PutObjectBackend putObjectBackend;
    private final BucketBackend bucketBackend;
    private final TenantBackend tenantBackend;
    private final ObjectMapper mapper;
    private final LifecycleEngineClient lifecycleEngineClient;
    private final EmbargoV3 embargoV3;

    public PutLifecyclePolicyHandler(Authenticator authenticator,
                                     PutObjectBackend putObjectBackend,
                                     BucketBackend bucketBackend,
                                     TenantBackend tenantBackend,
                                     ObjectMapper mapper,
                                     LifecycleEngineClient lifecycleEngineClient,
                                     EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.putObjectBackend = putObjectBackend;
        this.bucketBackend = bucketBackend;
        this.tenantBackend = tenantBackend;
        this.mapper = mapper;
        this.lifecycleEngineClient = lifecycleEngineClient;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.PUT_OBJECT_LIFECYCLE_POLICY);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_PUT_LIFECYCLE_BUNDLE);

        // Re-entry is definitely needed.  We must both check the user's authorization and the LE's authorization.
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();
        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String lifecycleBucketName = ObjectLifecycleHelper.toLifecycleBucketName(namespace);
        final String lifecycleObjectName = ObjectLifecycleHelper.generateLifecycleObjectName();

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.PUT_OBJECT_LIFECYCLE_POLICY)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        HttpContentHelpers.negotiateStorageObjectContent(request, 0,
                ObjectLifecycleHelper.MAX_LIFECYCLE_OBJECT_BYTES);
        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.STAR_ONLY);

        return CompletableBodyHandler.bodyHandler(request, Buffer::getBytes)
                .thenApplyAsync(bytes -> {

                    // Authenticate an HTTP request with a body.
                    final AuthenticationInfo authInfo = authenticator.authenticate(
                            context, Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));

                    // Deserialize byte[] as ObjectLifecycleDetails.
                    final PutObjectLifecyclePolicyDetails objectLifecycleDetails = HttpContentHelpers.
                            getObjectLifecycleDetails(request, bytes, mapper);

                    // Generate lifecycle object metadata.
                    final long contentLen = HttpContentHelpers.serializeObjectLifecycleDetails(request,
                            objectLifecycleDetails, mapper).length;

                    final ObjectMetadata objectMetadata = new ObjectMetadata(
                            namespace,
                            lifecycleBucketName,
                            lifecycleObjectName,
                            contentLen,
                            null,
                            new HashMap<>(),
                            new Date(),
                            new Date(),      // creation and modification time are the same
                            lifecycleObjectName, // ETag is the name of lifecycle object
                            null,
                            null, //archivedTime and restoredTime are null for non-archival object
                            ArchivalState.NotApplicable
                    );

                    // Creating an object helps to exchange lifecycle related values.
                    final CreateLifecycleObjectExchange createLifecycleObjectExchange =
                            new CreateLifecycleObjectExchange(request, objectLifecycleDetails,
                                    objectMetadata, authInfo, mapper);

                    // Ensure the namespace exists before attempting to create an internal bucket.
                    // Only V2/S3/V1 createBucket or V2 get/update Namespace can create a namespace.
                    MdsNamespace namespaceInfo =
                            tenantBackend.getNamespace(context, scope, new NamespaceKey(Api.V2, namespace))
                        .orElseThrow(() -> new NoSuchBucketException(BucketBackend.noSuchBucketMsg(
                            bucketName,
                            namespace)));

                    bucketBackend.headOrCreateInternalBucket(
                            context,
                            createLifecycleObjectExchange.getAuthInfo(),
                            namespaceInfo.getTenantOcid(),
                            createLifecycleObjectExchange.getObjectMetadata().getNamespace(),
                            createLifecycleObjectExchange.getObjectMetadata().getBucketName(),
                            BucketBackend.InternalBucketType.LIFECYCLE.getSuffix()
                    );

                    return createLifecycleObjectExchange;
                }, VertxExecutor.workerThread())

                .thenComposeAsync(exchange -> {

                    // Create a lifecycle object, this thread would consume I/O thus run on event loop.
                    final CompletableFuture<ObjectMetadata> objectMetadataCompletableFuture =
                            putObjectBackend.createLifecyclePolicyObject(context, exchange);

                    // Here we return a pair of exchange plus a new ObjectMetadata comes from DB, which contains
                    // the latest necessary ObjectMetadata info we need.
                    return objectMetadataCompletableFuture.thenApply(objectMeta ->
                            Pair.pair(exchange, objectMeta));

                }, VertxExecutor.eventLoop())

                .thenApplyAsync(exchangeAndMetadata -> {

                    final CreateLifecycleObjectExchange lifecycleObjectExchange = exchangeAndMetadata.getFirst();
                    final ObjectMetadata objectMetadata = exchangeAndMetadata.getSecond();

                    // Cancel lifecycle policy in lifecycle engine
                    lifecycleEngineClient.cancel(namespace, bucketName);

                    // We update lifecycle policy eTag when putting/overwriting lifecycle object in
                    // object DB completed.
                    bucketBackend.overwriteBucketCurrentLifecyclePolicyEtag(
                            context,
                            lifecycleObjectExchange.getAuthInfo(),
                            namespace,
                            bucketName,
                            lifecycleObjectExchange.getPutObjectLifecyclePolicyDetails(),
                            objectMetadata.getObjectName());

                    return exchangeAndMetadata;

                }, VertxExecutor.workerThread())

                .thenAcceptAsync(exchangeAndMetadata -> {

                    final CreateLifecycleObjectExchange lifecycleObjectExchange = exchangeAndMetadata.getFirst();
                    final ObjectMetadata objectMetadata = exchangeAndMetadata.getSecond();

                    final ObjectLifecyclePolicy objectLifecyclePolicy =
                            new ObjectLifecyclePolicy(objectMetadata.getCreationTime().toInstant(),
                                    lifecycleObjectExchange.getPutObjectLifecyclePolicyDetails().getItems());

                    response.putHeader(HttpHeaders.ETAG, objectMetadata.getObjectName());
                    response.putHeader(HttpHeaders.LAST_MODIFIED,
                            DateUtil.httpFormattedDate(objectMetadata.getCreationTime()));

                    HttpContentHelpers.writeJsonResponse(request, response, objectLifecyclePolicy, mapper);

                }, VertxExecutor.eventLoop())

                .handle((val, ex) -> HttpException.handle(request, val, ex));
    }
}
