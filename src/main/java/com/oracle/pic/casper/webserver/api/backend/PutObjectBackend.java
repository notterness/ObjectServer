package com.oracle.pic.casper.webserver.api.backend;

import com.codahale.metrics.Histogram;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.common.config.v2.ArchiveConfiguration;
import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.encryption.DigestUtils;
import com.oracle.pic.casper.common.encryption.EncryptionConstants;
import com.oracle.pic.casper.common.encryption.EncryptionKey;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.encryption.store.Secrets;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.AuthorizationException;
import com.oracle.pic.casper.common.exceptions.ContentSha256MismatchException;
import com.oracle.pic.casper.common.exceptions.InvalidContentLengthException;
import com.oracle.pic.casper.common.exceptions.InvalidDigestException;
import com.oracle.pic.casper.common.exceptions.MultipartUploadNotFoundException;
import com.oracle.pic.casper.common.exceptions.StorageLimitExceededException;
import com.oracle.pic.casper.common.host.HostInfo;
import com.oracle.pic.casper.common.json.JsonSerializer;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.metrics.MetricsBundle;
import com.oracle.pic.casper.common.model.BucketStorageTier;
import com.oracle.pic.casper.common.model.ObjectStorageTier;
import com.oracle.pic.casper.common.model.Stripe;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.util.ObjectLifecycleHelper;
import com.oracle.pic.casper.common.util.ParUtil;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.common.vertx.stream.BlobReadStream;
import com.oracle.pic.casper.common.vertx.stream.ByteBackedReadStream;
import com.oracle.pic.casper.common.vertx.stream.DefaultBlobReadStream;
import com.oracle.pic.casper.common.vertx.stream.DigestReadStream;
import com.oracle.pic.casper.common.vertx.stream.MeasuringReadStream;
import com.oracle.pic.casper.mds.MdsEncryptionKey;
import com.oracle.pic.casper.mds.MdsObjectKey;
import com.oracle.pic.casper.mds.common.exceptions.MdsException;
import com.oracle.pic.casper.mds.common.grpc.TimestampUtils;
import com.oracle.pic.casper.mds.loadbalanced.MdsExecutor;
import com.oracle.pic.casper.mds.object.AbortPartRequest;
import com.oracle.pic.casper.mds.object.AbortPutRequest;
import com.oracle.pic.casper.mds.object.BeginChunkRequest;
import com.oracle.pic.casper.mds.object.BeginPartChunkRequest;
import com.oracle.pic.casper.mds.object.BeginPartRequest;
import com.oracle.pic.casper.mds.object.BeginPartResponse;
import com.oracle.pic.casper.mds.object.BeginPutRequest;
import com.oracle.pic.casper.mds.object.BeginPutResponse;
import com.oracle.pic.casper.mds.object.FinishChunkRequest;
import com.oracle.pic.casper.mds.object.FinishPartChunkRequest;
import com.oracle.pic.casper.mds.object.FinishPartRequest;
import com.oracle.pic.casper.mds.object.FinishPartResponse;
import com.oracle.pic.casper.mds.object.FinishPutRequest;
import com.oracle.pic.casper.mds.object.MdsDigest;
import com.oracle.pic.casper.mds.object.MdsPartKey;
import com.oracle.pic.casper.mds.object.ObjectServiceGrpc.ObjectServiceBlockingStub;
import com.oracle.pic.casper.mds.object.exception.MdsUploadNotFoundException;
import com.oracle.pic.casper.mds.object.exception.ObjectExceptionClassifier;
import com.oracle.pic.casper.metadata.utils.CryptoUtils;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.CasperTransactionId;
import com.oracle.pic.casper.objectmeta.CasperTransactionIdFactory;
import com.oracle.pic.casper.objectmeta.ConcurrentModificationException;
import com.oracle.pic.casper.objectmeta.ETagType;
import com.oracle.pic.casper.objectmeta.ObjectId;
import com.oracle.pic.casper.objectmeta.PartKey;
import com.oracle.pic.casper.objectmeta.StoragePartInfo;
import com.oracle.pic.casper.storageclient.SCRequestContext;
import com.oracle.pic.casper.storageclient.VolumeStorageClient;
import com.oracle.pic.casper.storageclient.models.PutResult;
import com.oracle.pic.casper.storageclient.models.VolumeStorageContext;
import com.oracle.pic.casper.volumemeta.VolumeMetadataClientCache;
import com.oracle.pic.casper.volumemeta.util.VolumeMetadataUtils;
import com.oracle.pic.casper.volumemgmt.VolumeAndVonPicker;
import com.oracle.pic.casper.volumemgmt.model.PickerContext;
import com.oracle.pic.casper.volumemgmt.model.VolumeMetaAndVon;
import com.oracle.pic.casper.webserver.api.auditing.ObjectEvent;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.AuthorizationResponse;
import com.oracle.pic.casper.webserver.api.auth.Authorizer;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend.InternalBucketType;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpPathHelpers;
import com.oracle.pic.casper.webserver.api.common.MdsClientHelper;
import com.oracle.pic.casper.webserver.api.common.MdsTransformer;
import com.oracle.pic.casper.webserver.api.common.RecycleBinHelper;
import com.oracle.pic.casper.webserver.api.eventing.CasperObjectEvent;
import com.oracle.pic.casper.webserver.api.eventing.EventAction;
import com.oracle.pic.casper.webserver.api.eventing.EventPublisher;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.CreateLifecycleObjectExchange;
import com.oracle.pic.casper.webserver.api.model.CreatePartRequest;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.PartMetadata;
import com.oracle.pic.casper.webserver.api.model.PutObjectLifecyclePolicyDetails;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;
import com.oracle.pic.casper.webserver.api.model.exceptions.ConcurrentObjectModificationException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.replication.ReplicationEnforcer;
import com.oracle.pic.casper.webserver.api.usage.QuotaEvaluator;
import com.oracle.pic.casper.webserver.api.usage.UsageCache;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.webserver.auth.limits.Limits;
import com.oracle.pic.casper.webserver.server.MdsClients;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorderReadStream;
import com.oracle.pic.casper.webserver.util.BucketOptionsUtil;
import com.oracle.pic.casper.webserver.util.CommonUtils;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetricsBundle;
import com.oracle.pic.casper.webserver.vertx.EncryptingReadStream;
import com.oracle.pic.casper.webserver.vertx.FixedCapacityReadStream;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.StatusRuntimeException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.web.RoutingContext;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static javax.measure.unit.NonSI.BYTE;

public final class PutObjectBackend {

    static final class PutObjectBackendHelper {

        private PutObjectBackendHelper() {
        }

        public static <T> T invokeMds(
                MetricsBundle aggregateBundle, MetricsBundle apiBundle, boolean retryable, Callable<T> callable) {
            return MdsClientHelper.invokeMds(aggregateBundle, apiBundle, retryable,
                    callable, PutObjectBackendHelper::toRuntimeException);
        }

        private static <T> RuntimeException toRuntimeException(StatusRuntimeException ex) {
            final MdsException mdsException = ObjectExceptionClassifier.fromStatusRuntimeException(ex);
            if (mdsException instanceof MdsUploadNotFoundException) {
                return new MultipartUploadNotFoundException("No such upload", mdsException);
            } else {
                return Backend.BackendHelper.toRuntimeException(mdsException);
            }
        }
    }
    private static final Logger LOG = LoggerFactory.getLogger(PutObjectBackend.class);

    private static final ImmutableSet<CasperPermission> ALL_PUT_OBJECT_PERMISSIONS =
            ImmutableSet.of(CasperPermission.OBJECT_OVERWRITE, CasperPermission.OBJECT_CREATE);

    private final MdsExecutor<ObjectServiceBlockingStub> objectClient;
    private final long objectDeadlineSeconds;
    private final VolumeAndVonPicker volVonPicker;
    private final VolumeStorageClient volumeStorageClient;
    private final VolumeMetadataClientCache volMetaCache;
    private final CasperTransactionIdFactory txnIdFactory;
    private final Authorizer authorizer;
    private final WebServerConfiguration webServerConf;
    private final JsonSerializer jsonSerializer;
    private final DecidingKeyManagementService kms;

    private final BucketBackend bucketBackend;
    private final ArchiveConfiguration archiveConf;
    private final EventPublisher eventPublisher;
    private final boolean metersDebugEnabled;
    private final RetryPolicy retryPolicy;
    private final Limits limits;
    private final UsageCache usageCache;
    private final ResourceControlledMetadataClient metadataClient;
    private final QuotaEvaluator quotaEvaluator;
    private final boolean quotaEvaluationEnabled;
    private final boolean singleChunkOptimizationEnabled;

    private final Histogram totalChunkTime = Metrics.REGISTRY.histogram("webserver.put.chunk.total.time");

    public PutObjectBackend(
        MdsClients mdsClients,
        VolumeAndVonPicker volVonPicker,
        VolumeStorageClient volumeStorageClient,
        VolumeMetadataClientCache volMetaCache,
        CasperTransactionIdFactory txnIdFactory,
        Authorizer authorizer,
        WebServerConfiguration webServerConf,
        boolean metersDebugEnabled,
        JsonSerializer jsonSerializer,
        DecidingKeyManagementService kms,
        BucketBackend bucketBackend,
        ArchiveConfiguration archiveConf,
        EventPublisher eventPublisher,
        Limits limits,
        UsageCache usageCache,
        ResourceControlledMetadataClient metadataClient,
        QuotaEvaluator quotaEvaluator,
        boolean quotaEvaluationEnabled,
        boolean singleChunkOptimizationEnabled) {
        this.objectClient = mdsClients.getObjectMdsExecutor();
        this.objectDeadlineSeconds = mdsClients.getObjectRequestDeadline().getSeconds();
        this.volVonPicker = volVonPicker;
        this.volumeStorageClient = volumeStorageClient;
        this.volMetaCache = volMetaCache;
        this.txnIdFactory = txnIdFactory;
        this.authorizer = authorizer;
        this.webServerConf = webServerConf;
        this.jsonSerializer = jsonSerializer;
        this.kms = kms;
        this.bucketBackend = bucketBackend;
        this.archiveConf = archiveConf;
        this.eventPublisher = eventPublisher;
        this.metersDebugEnabled = metersDebugEnabled;
        this.limits = limits;
        this.usageCache = usageCache;
        this.metadataClient = metadataClient;
        this.quotaEvaluator = quotaEvaluator;
        this.quotaEvaluationEnabled = quotaEvaluationEnabled;
        this.singleChunkOptimizationEnabled = singleChunkOptimizationEnabled;

        retryPolicy = webServerConf.getRetryCount() > 0 ?
                new RetryPolicy().retryOn(ConcurrentModificationException.class)
                .withBackoff(webServerConf.getRetryBackoffDelayMs(), webServerConf.getRetryBackoffMaxDelayMs(),
                        TimeUnit.MILLISECONDS)
                .withJitter(webServerConf.getRetryJitter())
                .withMaxDuration(webServerConf.getRetryMaxDurationMs(), TimeUnit.MILLISECONDS)
                .withMaxRetries(webServerConf.getRetryCount()) : new RetryPolicy().withMaxRetries(0);
    }

    /**
     * Create a new lifecycle object, we distinguish this from other creating object in different aspects:
     * 1. Lifecycle validation is done differently.
     * 2. The lifecycle BlobReadStream is use for streaming "objectLifecycleDetails" to the storage servers.
     * 3. The authorizer passed to writeObject() is fake.
     *
     * @param context the Vert.x routing context.
     * @param createLifecycleObjectExchange the details for creating lifecycle request.
     * @return a {@link CompletableFuture} containing a {@link ObjectMetadata} for the newly created object,
     *         or an exception.
     */
    public CompletableFuture<ObjectMetadata> createLifecyclePolicyObject(
            RoutingContext context,
            CreateLifecycleObjectExchange createLifecycleObjectExchange) {
        Preconditions.checkNotNull(context);
        Preconditions.checkNotNull(createLifecycleObjectExchange.getObjectMetadata());

        final MetricScope rootScope = WSRequestContext.getMetricScope(context);
        final ObjectMetadata objMeta = createLifecycleObjectExchange.getObjectMetadata();
        Validator.validateInternalBucket(objMeta.getBucketName(),
                ObjectLifecycleHelper.LIFECYCLE_NAME_SUFFIX);
        Validator.validateObjectName(objMeta.getObjectName());
        Validator.validateMetadata(objMeta.getMetadata(), jsonSerializer);

        final ReadStream<Buffer> stream = createLifecycleStream(createLifecycleObjectExchange);

        HttpPathHelpers.logPathParameters(
                rootScope, objMeta.getNamespace(), objMeta.getBucketName(), objMeta.getObjectName());

        return getBucketMetadata(context, rootScope, objMeta.getNamespace(), objMeta.getBucketName(), Api.V2)
                .thenComposeAsync(bucketInfo -> writeObject(
                        WSRequestContext.get(context),
                        objMeta,
                        bucketInfo,
                        stream,
                        md -> { },
                        casperPermission ->
                                new PutObjectAuthorizationResult(true, ALL_PUT_OBJECT_PERMISSIONS),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        ETagType.ETAG,
                        null), VertxExecutor.eventLoop());
    }

    /**
     * Create a new par object, we distinguish this from other creating object in different aspects:
     * 1. PAR validation is done differently.
     * 2. The PAR BlobReadStream and DigestReadStream are hardcoded empty streams.
     * 3. Authorization was performed in PreAuthenticatedRequestBackend, so the authorizer
     *    passed to writeObject() is fake.
     *
     * @param context the Vert.x routing context.
     * @param authInfo the authentication information returned by the identity service.
     * @param objMeta the object metadata for the new object.
     * @param api the API to use.
     * @return a {@link CompletableFuture} containing a {@link ObjectMetadata} for the newly created object,
     *         or an exception.
     */
    public CompletableFuture<ObjectMetadata> createParObject(RoutingContext context,
                                                             AuthenticationInfo authInfo,
                                                             ObjectMetadata objMeta,
                                                             Api api) {
        Preconditions.checkNotNull(context);
        Preconditions.checkNotNull(authInfo);
        Preconditions.checkNotNull(objMeta);
        Preconditions.checkNotNull(api);

        Validator.validateInternalBucket(objMeta.getBucketName(), ParUtil.PAR_BUCKET_NAME_SUFFIX);
        Validator.validateParObjectName(objMeta.getObjectName());
        Validator.validateMetadata(objMeta.getMetadata(), jsonSerializer);

        final MetricScope rootScope = WSRequestContext.getMetricScope(context);

        HttpPathHelpers.logPathParameters(
                rootScope, objMeta.getNamespace(), objMeta.getBucketName(), objMeta.getObjectName());

        return getBucketMetadata(context, rootScope, objMeta.getNamespace(), objMeta.getBucketName(), api)
                .thenComposeAsync(bucketInfo -> writeObject(
                        WSRequestContext.get(context),
                        objMeta,
                        bucketInfo,
                        new ByteBackedReadStream(new byte[0]),
                        md -> { },
                        casperPermission ->
                                new PutObjectAuthorizationResult(true, ALL_PUT_OBJECT_PERMISSIONS),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        ETagType.ETAG,
                        null), VertxExecutor.eventLoop());
    }

    /**
     * Create an ordinary object or overwrite an existing one, we distinguish this from other creating object
     * in different aspects:
     * 1. Ordinary object and bucket name validation
     * 2. Ordinary read stream creation consuming the expectedMD5 and expectedSHA256
     * 3. Real authorization based on whether the object is being created or overwritten.
     *
     * @param context the Vert.x routing context.
     * @param authInfo authentication info for the new object
     * @param objMeta the object metadata for the new object.
     * @param validateCB a validation callback used to check the value of the existing object (if there is one).
     * @param api the API to use.
     * @param metrics the web server metrics bundle used for this request.
     * @param expectedMD5 a base-64 MD5 that will be checked against a computed value if it is not null.
     * @param expectedSHA256Digest a SHA256 digest that will be checked against a computed value if it is not null.
     * @param ifMatchEtag The current object has to have this ETag.
     * @param ifNoneMatchEtag The current object cannot have this ETag.
     * @return a {@link CompletableFuture} containing a {@link ObjectMetadata} for the newly created object,
     *         or an exception.
     */
    public CompletableFuture<ObjectMetadata> createOrOverwriteObject(
        RoutingContext context,
        AuthenticationInfo authInfo,
        ObjectMetadata objMeta,
        Consumer<Optional<ObjectMetadata>> validateCB,
        Api api,
        WebServerMetricsBundle metrics,
        @Nullable String expectedMD5,
        @Nullable Digest expectedSHA256Digest,
        @Nullable String ifMatchEtag,
        @Nullable String ifNoneMatchEtag,
        ETagType eTagType) {

        return createOrOverwriteObject(context, authInfo, objMeta, validateCB, api, metrics, expectedMD5,
                expectedSHA256Digest, ifMatchEtag, ifNoneMatchEtag, null, null, null, eTagType);
    }

    /**
     * @param md5Override the md5 override of the new object, non-null if the request comes from a cross-region
     *                    replication worker
     * @param etagOverride the etag override of the new object, non-null if the request comes from a cross-region
     *                     replication worker
     */
    public CompletableFuture<ObjectMetadata> createOrOverwriteObject(
        RoutingContext context,
        AuthenticationInfo authInfo,
        ObjectMetadata objMeta,
        Consumer<Optional<ObjectMetadata>> validateCB,
        Api api,
        WebServerMetricsBundle metrics,
        @Nullable String expectedMD5,
        @Nullable Digest expectedSHA256Digest,
        @Nullable String ifMatchEtag,
        @Nullable String ifNoneMatchEtag,
        @Nullable String md5Override,
        @Nullable String etagOverride,
        @Nullable Integer partCountOverride,
        ETagType eTagType) {
        Preconditions.checkNotNull(context);

        final WSRequestContext wsCtx = WSRequestContext.get(context);
        final MetricScope rootScope = wsCtx.getCommonRequestContext().getMetricScope();
        final String rid = wsCtx.getCommonRequestContext().getOpcRequestId();

        final ReadStream<Buffer> stream = new MeasuringReadStream<>(
                metrics.getFirstByteLatency(),
                wsCtx.getStartTime(),
                rootScope,
                new TrafficRecorderReadStream(context.request(), rid));

        return createOrOverwriteObject(context, authInfo, objMeta, validateCB, stream, api, expectedMD5,
                expectedSHA256Digest, ifMatchEtag, ifNoneMatchEtag, md5Override, etagOverride, partCountOverride,
                eTagType);
    }

    public CompletableFuture<ObjectMetadata> createOrOverwriteObject(RoutingContext context,
                                                                     AuthenticationInfo authInfo,
                                                                     ObjectMetadata objMeta,
                                                                     Consumer<Optional<ObjectMetadata>> validateCB,
                                                                     ReadStream<Buffer> stream,
                                                                     Api api,
                                                                     @Nullable String expectedMD5,
                                                                     @Nullable Digest expectedSHA256Digest,
                                                                     @Nullable String ifMatchEtag,
                                                                     @Nullable String ifNoneMatchEtag,
                                                                     @Nullable String md5Override,
                                                                     @Nullable String etagOverride,
                                                                     @Nullable Integer partCountOverride,
                                                                     ETagType eTagType) {
        Preconditions.checkNotNull(context);
        Preconditions.checkNotNull(authInfo);
        Preconditions.checkNotNull(objMeta);
        Preconditions.checkNotNull(validateCB);
        Preconditions.checkNotNull(api);
        Preconditions.checkNotNull(eTagType);

        Validator.validateBucket(objMeta.getBucketName());
        Validator.validateObjectName(objMeta.getObjectName());
        Validator.validateMetadata(objMeta.getMetadata(), jsonSerializer);

        final WSRequestContext wsCtx = WSRequestContext.get(context);
        final MetricScope rootScope = wsCtx.getCommonRequestContext().getMetricScope();
        final String rid = wsCtx.getCommonRequestContext().getOpcRequestId();

        HttpPathHelpers.logPathParameters(
                rootScope, objMeta.getNamespace(), objMeta.getBucketName(), objMeta.getObjectName());

        final AtomicBoolean isCreate = new AtomicBoolean(true);
        TrafficRecorder.getVertxRecorder().requestStarted(
                objMeta.getNamespace(),
                rid, TrafficRecorder.RequestType.PutObject, objMeta.getSizeInBytes());
        return getBucketMetadata(context, rootScope, objMeta.getNamespace(), objMeta.getBucketName(), api)
                .thenComposeAsync(bucketInfo -> {
                    // Put objectLevelAuditMode in the context to be retrieved by AuditFilterHandler
                    if (!BucketBackend.InternalBucketType.isInternalBucket(bucketInfo.getBucketName())) {
                        WSRequestContext.get(context).setObjectLevelAuditMode(bucketInfo.getObjectLevelAuditMode());
                        WSRequestContext.get(context).setBucketOcid(bucketInfo.getOcid());
                    }
                    WSRequestContext.get(context).setBucketCreator(bucketInfo.getCreationUser());
                    WSRequestContext.get(context).setBucketLoggingStatus(BucketOptionsUtil
                                    .getBucketLoggingStatusFromOptions(bucketInfo.getOptions()));

                    return writeObject(
                            WSRequestContext.get(context),
                            objMeta,
                            bucketInfo,
                            stream,
                            oom -> {
                                oom.ifPresent(v -> isCreate.set(false));
                                validateCB.accept(oom);
                            },
                            casperPermissions -> {
                                // Authorize putting a new object or overwriting an existing object. We call this with
                                // the two permissions (create/overwrite) and authorizeAll = false. This means we will
                                // authorized if either of the permissions are allowed. We need to do this because we
                                // perform the authorization checks in two places. For create, we can check in BeginPut,
                                // but for overwrite, we need to check in FinishPut. So after calling authorize, we need
                                // to look at the AuthorizationResponse which contains the permissions that are actually
                                // allowed. Then we can use that to perform the checks in BeginPut and FinishPut.
                                Optional<AuthorizationResponse> result = authorizer.authorize(
                                        WSRequestContext.get(context),
                                        authInfo,
                                        bucketInfo.getNamespaceKey(),
                                        bucketInfo.getBucketName(),
                                        bucketInfo.getCompartment(),
                                        bucketInfo.getPublicAccessType(), CasperOperation.PUT_OBJECT,
                                        bucketInfo.getKmsKeyId().orElse(null),
                                        false,
                                        bucketInfo.isTenancyDeleted(),
                                        casperPermissions.toArray(new CasperPermission[casperPermissions.size()]));

                                Set<CasperPermission> allowedPermissions = result.isPresent() ?
                                        result.get().getCasperPermissions() : ImmutableSet.of();

                                return new PutObjectAuthorizationResult(result.isPresent(), allowedPermissions);
                            },
                            expectedMD5,
                            expectedSHA256Digest,
                            md5Override,
                            etagOverride,
                            partCountOverride,
                            ifMatchEtag,
                            ifNoneMatchEtag,
                            eTagType,
                            () -> ReplicationEnforcer.throwIfReadOnlyButNotReplicationScope(context,
                                    bucketInfo)).thenApplyAsync(om -> {
                        WSRequestContext.get(context).setEtag(om.getETag());
                        WSRequestContext.get(context).setArchivalState(om.getArchivalState());
                        WSRequestContext.get(context).setObjectEventsEnabled(bucketInfo.isObjectEventsEnabled());
                        eventPublisher.addEvent(
                            new CasperObjectEvent(
                                kms,
                                bucketInfo,
                                om.getObjectName(),
                                om.getETag(),
                                om.getArchivalState(),
                                isCreate.get() ? EventAction.CREATE : EventAction.UPDATE),
                            authInfo.getMainPrincipal().getTenantId(),
                            bucketInfo.getObjectLevelAuditMode());
                        WSRequestContext.get(context).addObjectEvent(new ObjectEvent(isCreate.get() ?
                            ObjectEvent.EventType.CREATE_OBJECT : ObjectEvent.EventType.UPDATE_OBJECT,
                            om.getObjectName(), om.getETag(), om.getArchivalState().getState()));
                        wsCtx.addNewState("eTag", om.getETag());
                        return om;
                    }, VertxExecutor.workerThread()).thenApplyAsync((om) -> {
                        ServiceLogsHelper.logServiceEntry(context);
                        return om;
                    }, VertxExecutor.workerThread());
                }, VertxExecutor.eventLoop())
                .whenCompleteAsync(
                        (b, t) -> TrafficRecorder.getVertxRecorder().requestEnded(rid),
                VertxExecutor.eventLoop());
    }

    /**
     * Helper to create the ReadStream that will be used to write an lifecycle object to storage servers.
     */
    private ReadStream<Buffer> createLifecycleStream(CreateLifecycleObjectExchange createLifecycleObjectExchange) {
        final HttpServerRequest request = createLifecycleObjectExchange.getRequest();
        final PutObjectLifecyclePolicyDetails putObjectLifecyclePolicyDetails =
                createLifecycleObjectExchange.getPutObjectLifecyclePolicyDetails();
        final ObjectMapper mapper = createLifecycleObjectExchange.getMapper();

        final byte[] bytes = HttpContentHelpers.serializeObjectLifecycleDetails(request,
                putObjectLifecyclePolicyDetails, mapper);
        return new ByteBackedReadStream(bytes);
    }

    private CompletableFuture<WSTenantBucketInfo> getBucketMetadata(
        RoutingContext context, MetricScope scope, String ns, String bucketName, Api api) {
        final MetricScope childScope = scope.child("PutObjectBackend:getBucketMetadata");
        return VertxUtil.runAsync(
            childScope,
            () -> bucketBackend.getBucketMetadataWithCache(context, childScope, ns, bucketName, api));
    }

    /**
     * Write the chunks of an object to storage servers and update the object metadata DB.
     *
     * @param context the request context, used for logging and metrics.
     * @param objMeta the metadata of the object to be written.
     * @param bucket the bucket into which the object will be written.
     * @param stream the read stream used to receive the bytes of the object.
     * @param validateCB a callback that will be called once, on the event loop thread, with the object metadata for the
     *                   current object (of the same name) if there is one. If the callback throws an exception, the
     *                   CF returned by this method will fail with that exception.
     * @param authorizer a function used to authorize the write of the object. If the authz function throws an exception
     *                   the CF returned by this method will fail with that exception.
     * @param expectedMD5 the expected MD5 of the data sent via the stream, or null if there is no expected MD5. If
     *                    there is an expected MD5, and it does not match the computed value, the CF returned by this
     *                    method will fail with an InvalidDigestException.
     * @param expectedSHA256Digest the expected SHA256 of the data sent via the stream, or null if there is no expected
     *                       SHA256. If there is an expected SHA256 and it does not match the computed value, the CF
     *                       returned by this method will fail with a ContentSha256MismatchException.
     * @param md5Override the md5 override of the new object, non-null if the request comes from a cross-region
     *                    replication worker
     * @param etagOverride the etag override of the new object, non-null if the request comes from a cross-region
     *                     replication worker
     * @param ifMatchEtag The current object has to have this ETag.
     * @param ifNoneMatchEtag The current object cannot have this ETag.
     * @return a CompletableFuture that will contain the final metadata for the new object (if it is written
     *         successfully), or an exception if there was an error. An exception does not mean the object failed, as
     *         there are cases (like timeouts) in which the CF can fail, but the object write can succeed. In addition
     *         to the exceptions noted above, the following can be thrown: ConcurrentObjectModificationException (when
     *         there are multiple writes to the same object), InvalidContentLengthException (if the stream does not
     *         contain the correct amount of data) or IllegalArgumentException (if the SHA-256 or MD5 is malformed).
     */
    private CompletableFuture<ObjectMetadata> writeObject(
        WSRequestContext context,
        ObjectMetadata objMeta,
        WSTenantBucketInfo bucket,
        ReadStream<Buffer> stream,
        Consumer<Optional<ObjectMetadata>> validateCB,
        Function<Set<CasperPermission>, PutObjectAuthorizationResult> authorizer,
        @Nullable String expectedMD5,
        @Nullable Digest expectedSHA256Digest,
        @Nullable String md5Override,
        @Nullable String etagOverride,
        @Nullable Integer partCountOverride,
        @Nullable String ifMatchEtag,
        @Nullable String ifNoneMatchEtag,
        ETagType eTagType,
        ReplicationEnforcer replicationEnforcer) {
        final CommonRequestContext commonContext = context.getCommonRequestContext();

        final MetricScope rootScope = context.getCommonRequestContext().getMetricScope();
        final MetricScope childScope = rootScope.child("Backend:writeObject");

        // Blocking creation of objects in the recycle bin
        if (RecycleBinHelper.isBinObject(bucket, objMeta.getObjectName())) {
            throw new AuthorizationException(V2ErrorCode.FORBIDDEN.getStatusCode(),
                    V2ErrorCode.FORBIDDEN.getErrorName(),
                    "Not authorized to write or modify objects in the Recycle Bin");
        }

        final DigestReadStream drs;
        if (expectedSHA256Digest != null) {
            drs = new DigestReadStream(DigestAlgorithm.SHA256, stream);
        } else {
            drs = null;
        }

        final Optional<Digest> expMD5Dig = Optional.ofNullable(expectedMD5)
                .map(c -> Digest.fromBase64Encoded(DigestAlgorithm.MD5, c));
        final BlobReadStream rs = new DefaultBlobReadStream(
                DigestAlgorithm.MD5, expMD5Dig, objMeta.getSizeInBytes(), drs == null ? stream : drs, false);

        final Duration archiveMinRetention;
        final Duration archiveNewObjPeriod;
        // The object storage tier will always be set to STANDARD but when we read the object
        // and if the time we read it is after the archive period we set for this object,
        // we will treat this object as an archived object when we read it.
        final ObjectStorageTier objectStorageTier = ObjectStorageTier.STANDARD;
        if (bucket.getStorageTier() == BucketStorageTier.Archive) {
            archiveMinRetention = archiveConf.getMinimumRetentionPeriodForArchives();
            archiveNewObjPeriod = archiveConf.getArchivePeriodForNewObject();
        } else {
            archiveMinRetention = null;
            archiveNewObjPeriod = null;
        }
        final CasperTransactionId txnId = txnIdFactory.newCasperTransactionId("writeObject");

        // These values are set in one completion stage and used from another, which means it will be written by one
        // thread and read from another. Having it be an atomic reference makes that thread-safe (as would a volatile).
        final AtomicReference<ObjectId> objIdRef = new AtomicReference<>();
        final AtomicBoolean overwritePermissionRef = new AtomicBoolean();
        final AtomicReference<EncryptionKey> chunkEncKey = new AtomicReference<>();
        final AtomicReference<VolumeMetaAndVon> volumeMetaAndVonAtomicReference = new AtomicReference<>();

        final boolean isSingleChunkObject =
                (objMeta.getSizeInBytes() <= webServerConf.getChunkSize().longValue(BYTE)) &&
                singleChunkOptimizationEnabled;
        return VertxUtil.runAsync(childScope, () -> {
            final ObjectMetadata curObjMeta;

            // Call Identity to check to see if the customer is authorized.
            PutObjectAuthorizationResult putObjectAuthorizationResult = MetricScope.timeScopeF(childScope,
                "authorizer",
                (innerScope) -> authorizer.apply(ALL_PUT_OBJECT_PERMISSIONS));

            if (!putObjectAuthorizationResult.isAuthorized()) {
                throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucket.getBucketName(),
                    bucket.getNamespaceKey().getName()));
            }

            if (replicationEnforcer != null) {
                replicationEnforcer.enforce();
            }

            final String namespace = bucket.getNamespaceKey().getName();
            final Optional<Tenant> resourceTenant =
                metadataClient.getTenantByCompartmentId(bucket.getCompartment(), namespace);
            if (resourceTenant.isPresent()) {
                context.setResourceTenantOcid(resourceTenant.get().getId());
            }

            // No need to check storage limit for internal bucket.
            // If we fail to get destination tenant ocid from identity, then fail open.
            if (resourceTenant.isPresent() && !InternalBucketType.isInternalBucket(bucket.getBucketName())) {
                evaluateStorageQuota(namespace, resourceTenant.get().getId(), bucket.getCompartment(),
                        objMeta.getSizeInBytes());
            }

            final BeginPutRequest.Builder requestBuilder = BeginPutRequest.newBuilder()
                    .setBucketToken(bucket.getMdsBucketToken().toByteString())
                    .setObjectKey(Backend.objectKey(
                            objMeta.getObjectName(),
                            bucket.getNamespaceKey().getName(),
                            bucket.getBucketName(),
                            bucket.getNamespaceKey().getApi()))
                    .setStorageTier(MdsTransformer.toObjectStorageTier(objectStorageTier))
                    .setTransactionId(txnId.toString());

            if (isSingleChunkObject) {
                final VolumeMetaAndVon volMetaAndVon = MetricScope.timeScopeF(rootScope,
                        "pickVolume",
                        innerScope -> volVonPicker.getNextVon(
                                innerScope,
                                createDefaultPickerContext(objMeta.getSizeInBytes(),
                                        context.getCommonRequestContext().getOpcRequestId())));
                volumeMetaAndVonAtomicReference.set(volMetaAndVon);

                requestBuilder.setVolumeId(Int64Value.of(volMetaAndVon.getVolumeMetadata().getVolumeId()))
                        .setVon(Int32Value.of(volMetaAndVon.getVon()))
                        .setSequenceNum(Int64Value.of(0L));
            }

            curObjMeta = MetricScope.timeScopeF(childScope, "objectMds:beginPut", innerScope -> {
                final BeginPutResponse response = PutObjectBackendHelper.invokeMds(
                        MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_BEGIN_PUT,
                        false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                c, objectDeadlineSeconds, commonContext).beginPut(requestBuilder.build())));

                objIdRef.set(new ObjectId(response.getObjectId()));
                if (!response.hasExistingObject()) {
                    return null;
                }
                return BackendConversions.mdsObjectSummaryToObjectMetadata(response.getExistingObject(), kms);
            });

            // Check the permissions the customer has. They need the OBJECT_CREATE permission if the object doesn't
            // exist yet, and the OBJECT_OVERWRITE permission if there's already an existing object.
            // The check for OBJECT_CREATE is sufficient here in BeginPut. However, the OBJECT_OVERWRITE check is not
            // sufficient here because there could be a new object created by another operation after this point, so it
            if ((curObjMeta == null &&
                 !putObjectAuthorizationResult.getCasperPermissions().contains(CasperPermission.OBJECT_CREATE)) ||
                (curObjMeta != null &&
                 !putObjectAuthorizationResult.getCasperPermissions().contains(CasperPermission.OBJECT_OVERWRITE))) {
                throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucket.getBucketName(),
                        bucket.getNamespaceKey().getName()));
            }

            // Keep track of whether or not the customer has OBJECT_OVERWRITE permission.
            // We need to use this later in finishPut.
            overwritePermissionRef.set(putObjectAuthorizationResult.getCasperPermissions()
                    .contains(CasperPermission.OBJECT_OVERWRITE));

            return curObjMeta;
        }).thenComposeAsync(curObjMeta -> {
            validateCB.accept(Optional.ofNullable(curObjMeta));

            if (isSingleChunkObject) {
                return createSingleChunk(bucket,
                        objMeta,
                        commonContext,
                        rs,
                        volumeMetaAndVonAtomicReference.get(),
                        chunkEncKey,
                        childScope);
            } else {
                return createChunk(
                        bucket,
                        null,  // previous stream
                        objMeta.getObjectName(),
                        rs,
                        txnId,
                        objIdRef.get(),
                        context.getCommonRequestContext(),
                        0,                 // chunk sequence number
                        objMeta.getSizeInBytes(),
                        0,                 // bytes written
                        bucket.getKmsKeyId().orElse(null),
                        metersDebugEnabled);
            }
        }, VertxExecutor.eventLoop())
        .thenApplyAsync(putResult -> {
            final String actualMD5 = rs.getCalculatedDigest().getBase64Encoded();
            final EncryptionKey metaEncKey = getEncryption(bucket.getKmsKeyId().orElse(null), kms);
            final String encryptedMetadata = CryptoUtils.encryptMetadata(objMeta.getMetadata(), metaEncKey);
            final FinishPutRequest.Builder builder = FinishPutRequest.newBuilder()
                    .setObjectId(objIdRef.get().getId())
                    .setBucketToken(bucket.getMdsBucketToken().toByteString())
                    .setObjectKey(Backend.objectKey(objMeta.getObjectName(), bucket.getNamespaceKey().getName(),
                            bucket.getBucketName(), bucket.getNamespaceKey().getApi()))
                    .setStorageTier(MdsTransformer.toObjectStorageTier(objectStorageTier))
                    .setTransactionId(txnId.toString())
                    .setMetadata(encryptedMetadata)
                    .setMd5(md5Override == null ? actualMD5 : md5Override)
                    .setEtag(etagOverride == null ? CommonUtils.generateETag() : etagOverride)
                    .setEncryptionKey(MdsTransformer.toMdsEncryptionKey(metaEncKey))
                    .setEtagType(MdsTransformer.toEtagType(eTagType))
                    .setObjectOverwriteAllowed(overwritePermissionRef.get());

            if (isSingleChunkObject) {
                builder.setChunkSizeInBytes(Int64Value.of(putResult.getContentLength()))
                        .setDigest(MdsTransformer.toMdsDigest(DigestUtils.nullDigest()))
                        .setChunkEncryptionKey(MdsTransformer.toMdsEncryptionKey(chunkEncKey.get()))
                        .setSequenceNum(Int64Value.of(0L));
            }

            if (archiveMinRetention != null) {
                builder.setMinRetention(TimestampUtils.toProtoDuration(archiveMinRetention));
            }
            if (archiveNewObjPeriod != null) {
                builder.setTimeToArchive(TimestampUtils.toProtoDuration(archiveNewObjPeriod));
            }

            if (ifMatchEtag != null) {
                builder.setIfMatchEtag(ifMatchEtag);
            }
            if (ifNoneMatchEtag != null) {
                builder.setIfNoneMatchEtag(ifNoneMatchEtag);
            }

            if (partCountOverride != null) {
                builder.setMultipartOverride(true);
                builder.setPartcountOverride(partCountOverride);
            }

            validateStreamAfterObjectFinished(rs, expectedSHA256Digest, drs);

            return BackendConversions.mdsObjectSummaryToObjectMetadata(
                    PutObjectBackendHelper.invokeMds(
                            MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_FINISH_PUT,
                            false, () -> objectClient.execute(
                                    c -> Backend.getClientWithOptions(c, objectDeadlineSeconds, commonContext))
                                    .finishPut(builder.build()).getSummary()),
                    kms);

        }, VertxExecutor.workerThread())
        .whenCompleteAsync((result, throwable) -> {
            childScope.getRoot().annotate("totalBytesTransferred", rs.getBytesTransferred());
            if (throwable != null) {
                // If beginPut failed, objIdRef won't be set. In that case, we don't need to abortPut.
                final ObjectId objectId = objIdRef.get();
                if (objectId == null) {
                    return;
                }
                final MetricScope innerScope = rootScope.child("abort");
                final AbortPutRequest request = AbortPutRequest.newBuilder()
                        .setBucketToken(bucket.getMdsBucketToken().toByteString())
                        .setObjectKey(Backend.objectKey(objMeta.getObjectName(), bucket.getNamespaceKey().getName(),
                                bucket.getBucketName(), bucket.getNamespaceKey().getApi()))
                        .setTransactionId(txnId.toString())
                        .setObjectId(objectId.getId())
                        .build();
                try {
                    PutObjectBackendHelper.invokeMds(MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_ABORT_PUT,
                            false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                    c, objectDeadlineSeconds, commonContext).abortPut(request)));
                } catch (Exception ex) {
                    innerScope.fail(ex);
                    LOG.warn("Failed to abort a Casper transaction with id {} on object {}", txnId,
                            objMeta.getObjectName(), ex);
                } finally {
                    innerScope.end();
                }
            }
        }, VertxExecutor.workerThread());
    }

    /**
     * Create a chunk in the database and write out the chunk data to storage servers.
     *
     * This method works for both multi-part PUT object requests and normal (non-multi-part) requests. The difference is
     * that multi-part requests have a non-null ObjectId and a null CasperTransactionId, and normal requests have the
     * opposite.
     *  @param bucket bucket information for the chunk.
     * @param prevStream The stream for the previous chunk of the same object, or null if this is the first chunk.
     * @param objectName The name for the object of which this chunk is a piece.
     * @param stream The stream from the customer connection.
     * @param txnId If this is a PUT object (non-multi-part), this is the txn ID for the write, otherwise it is null.
     * @param objId If this is a multi-part PUT object, this is the object ID for the upload, otherwise it is null.
     * @param context The common request context for this customer request.
     * @param seqNum The chunk sequence number, which starts at zero.
     * @param objContentLen The content length of the entire object (not just this chunk).
     * @param bytesSentSoFar The number of bytes sent in previous chunks.
     * @param kmsKeyId The KMS key ID if encryption is using KMS, or null if it is not.
     * @param metersDebugEnabled
     */
    private CompletableFuture<PutResult> createChunk(
            WSTenantBucketInfo bucket,
            @Nullable FixedCapacityReadStream prevStream,
            String objectName,
            BlobReadStream stream,
            @Nullable CasperTransactionId txnId,
            ObjectId objId,
            CommonRequestContext context,
            long seqNum,
            long objContentLen,
            long bytesSentSoFar,
            @Nullable String kmsKeyId,
            boolean metersDebugEnabled) {
        Preconditions.checkNotNull(objId);
        long start = System.nanoTime();
        final boolean isMultiPart = txnId == null;
        final long chunkSize = webServerConf.getChunkSize().longValue(BYTE);
        final long bytesRemaining = objContentLen - bytesSentSoFar;
        final long chunkContentLen = Math.min(bytesRemaining, chunkSize);

        //The order of the streams here are important.
        //With this order, the digest is for the encrypted chunk.
        final FixedCapacityReadStream fixedLenStream = new FixedCapacityReadStream(
                chunkSize,
                stream,
                prevStream != null && prevStream.delegateHasEnded(),
                Optional.ofNullable(prevStream));
        final EncryptionKey chunkEncKey = getEncryption(kmsKeyId, kms);
        final EncryptingReadStream encStream = new EncryptingReadStream(fixedLenStream, chunkEncKey,
                metersDebugEnabled);

        final MetricScope scope = context.getMetricScope();
        return VertxUtil.runAsync(() -> {
            final VolumeMetaAndVon volMetaAndVon = MetricScope.timeScopeF(scope, "pickVolume",
                    innerScope -> volVonPicker.getNextVon(
                            innerScope, createDefaultPickerContext(chunkContentLen, context.getOpcRequestId())));

            MetricScope.timeScopeC(scope, "objectMds:beginChunk",
                    v -> {
                        MdsObjectKey mdsObjectKey = Backend.objectKey(objectName, bucket.getNamespaceKey().getName(),
                        bucket.getBucketName(), bucket.getNamespaceKey().getApi());
                if (isMultiPart) {
                    final BeginPartChunkRequest request = BeginPartChunkRequest.newBuilder()
                            .setBucketToken(bucket.getMdsBucketToken().toByteString())
                            .setObjectKey(mdsObjectKey)
                            .setObjectId(objId.getId())
                            .setSequenceNum(seqNum)
                            .setVolumeId(volMetaAndVon.getVolumeMetadata().getVolumeId())
                            .setVon(volMetaAndVon.getVon())
                            .build();
                    // Multi-part PUT object
                    PutObjectBackendHelper.invokeMds(
                            MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_BEGIN_PART_CHUNK,
                            false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                    c, objectDeadlineSeconds, context).beginPartChunk(request)));
                } else {
                    final BeginChunkRequest request = BeginChunkRequest.newBuilder()
                            .setObjectId(objId.getId())
                            .setBucketToken(bucket.getMdsBucketToken().toByteString())
                            .setObjectKey(mdsObjectKey)
                            .setTransactionId(txnId.toString())
                            .setSequenceNum(seqNum)
                            .setVolumeId(volMetaAndVon.getVolumeMetadata().getVolumeId())
                            .setVon(volMetaAndVon.getVon())
                            .build();
                    // Normal PUT object
                    PutObjectBackendHelper.invokeMds(
                            MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_BEGIN_CHUNK,
                            false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                    c, objectDeadlineSeconds, context).beginChunk(request)));
                }
            });

            return volMetaAndVon;
        })
        .thenComposeAsync(volMetaAndVon ->
                        writeToStorageServer(volMetaAndVon, context, encStream, chunkContentLen, scope),
                VertxExecutor.eventLoop())
        .thenComposeAsync(putResult -> {
            MetricScope.timeScopeC(scope, "storageObjDb:finishChunk",
                    v -> {
                final MdsObjectKey mdsObjectKey = Backend.objectKey(objectName, bucket.getNamespaceKey().getName(),
                        bucket.getBucketName(), bucket.getNamespaceKey().getApi());
                final MdsDigest mdsDigest = MdsTransformer.toMdsDigest(DigestUtils.nullDigest());
                final MdsEncryptionKey mdsEncryptionKey = MdsTransformer.toMdsEncryptionKey(chunkEncKey);
                if (isMultiPart) {
                    final FinishPartChunkRequest request = FinishPartChunkRequest.newBuilder()
                            .setBucketToken(bucket.getMdsBucketToken().toByteString())
                            .setObjectKey(mdsObjectKey)
                            .setObjectId(objId.getId())
                            .setSequenceNum(seqNum)
                            .setSizeInBytes(chunkContentLen)
                            .setDigest(mdsDigest)
                            .setEncryptionKey(mdsEncryptionKey)
                            .build();
                    // Multi-part PUT object
                    PutObjectBackendHelper.invokeMds(
                            MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_FINISH_PART_CHUNK,
                            false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                    c, objectDeadlineSeconds, context).finishPartChunk(request)));
                } else {
                    final FinishChunkRequest request = FinishChunkRequest.newBuilder()
                            .setObjectId(objId.getId())
                            .setBucketToken(bucket.getMdsBucketToken().toByteString())
                            .setObjectKey(mdsObjectKey)
                            .setTransactionId(txnId.toString())
                            .setSequenceNum(seqNum)
                            .setSizeInBytes(putResult.getContentLength())
                            .setDigest(mdsDigest)
                            .setEncryptionKey(mdsEncryptionKey)
                            .build();
                    // Normal PUT object
                    PutObjectBackendHelper.invokeMds(
                            MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_FINISH_CHUNK,
                            false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                    c, objectDeadlineSeconds, context).finishChunk(request)));
                }
            });

            return CompletableFuture.completedFuture(putResult);
        }, VertxExecutor.workerThread())
        .thenComposeAsync(putResult -> {
            totalChunkTime.update(System.nanoTime() - start);
            final boolean objectFinished = isObjectCreationFinished(
                    fixedLenStream,
                    stream,
                    bytesSentSoFar,
                    chunkContentLen,
                    objContentLen);
            if (!objectFinished) {
                return createChunk(
                        bucket,
                        fixedLenStream,
                        objectName,
                        stream,
                        txnId,
                        objId,
                        context,
                        seqNum + 1,
                        objContentLen,
                        bytesSentSoFar + chunkContentLen,
                        kmsKeyId, metersDebugEnabled);
            }

            return CompletableFuture.completedFuture(putResult);
        }, VertxExecutor.eventLoop());
    }

    private CompletableFuture<PutResult> createSingleChunk(
            WSTenantBucketInfo bucket,
            ObjectMetadata objMeta,
            CommonRequestContext commonContext,
            BlobReadStream rs,
            VolumeMetaAndVon volumeMetaAndVon,
            AtomicReference<EncryptionKey> chunkEncKey,
            MetricScope metricScope) {
        chunkEncKey.set(getEncryption(bucket.getKmsKeyId().orElse(null), kms));

        final long chunkSize = webServerConf.getChunkSize().longValue(BYTE);
        final FixedCapacityReadStream fixedLenStream = new FixedCapacityReadStream(
                chunkSize,
                rs,
                false,
                Optional.empty());
        final EncryptingReadStream encStream = new EncryptingReadStream(fixedLenStream,
                chunkEncKey.get(),
                metersDebugEnabled);

        return writeToStorageServer(
                volumeMetaAndVon,
                commonContext,
                encStream,
                objMeta.getSizeInBytes(),
                metricScope)
                .thenComposeAsync(putResult -> {
                    rs.resume();
                    return CompletableFuture.completedFuture(putResult);
                }, VertxExecutor.eventLoop());
    }

    private CompletableFuture<PutResult> writeToStorageServer(
            VolumeMetaAndVon volMetaAndVon,
            CommonRequestContext context,
            EncryptingReadStream chunkStream,
            long contentLength,
            MetricScope scope) {
        VertxUtil.assertOnVertxEventLoop();
        final Stripe<HostInfo> hostInfoStripe =
                VolumeMetadataUtils.getStorageServerHostInfoStripe(volMetaAndVon.getVolumeMetadata(), volMetaCache);

        final MetricScope putScope = scope.child("volumeStorageClient:put");
        return volumeStorageClient.put(
                new SCRequestContext(
                        context.getOpcRequestId(),
                        context.getOpcClientRequestId().orElse(null),
                        putScope),
                new VolumeStorageContext(volMetaAndVon.getVolumeMetadata().getVolumeId(),
                        volMetaAndVon.getVon(),
                        hostInfoStripe),
                chunkStream,
                contentLength).whenComplete((result, throwable) -> {
            if (throwable != null) {
                putScope.fail(throwable);
            } else {
                putScope.annotate("chunkBytesTransferred", result.getContentLength()).end();
            }
        });
    }

    private static PickerContext createDefaultPickerContext(long chunkContentLength, String requestId) {
        return PickerContext.builder(requestId)
                .setObjectSizeHint(chunkContentLength)
                .build();
    }

    /**
     * Inspects the current state of object creation by looking at the latest chunk created as
     * well as the data available from the stream. Makes necessary adjustment to the streams if
     * object creation is finishe.
     */
    private static boolean isObjectCreationFinished(FixedCapacityReadStream fixedCapacityReadStream,
                                                    BlobReadStream objectReadStream,
                                                    long bytesTransferredSoFar,
                                                    long chunkContentLength,
                                                    long objectContentLength) {
        final boolean objectFinished;
        if ((fixedCapacityReadStream.delegateHasEnded() && !fixedCapacityReadStream.getOverflowBuffer().isPresent())) {
            objectFinished = true;
        } else if ((bytesTransferredSoFar + chunkContentLength) == objectContentLength) {
            // In the event that object content length is a multiple of chunk size, we will not know the stream has
            // ended until we read again from it. This causes us to create extraneous zero-byte chunks at the end of
            // the object. In order to avoid that, we simply check if we've written object content length bytes. And
            // if so, we complete object creation. We must also resume the underlying object read stream, which was
            // paused in the last FixedCapacityReadStream instance, to prevent subsequent calls over the same
            // connection from being ignored.
            objectFinished = true;
            objectReadStream.resume();
        } else {
            objectFinished = false;
        }
        return objectFinished;
    }

    private static void validateStreamAfterObjectFinished(BlobReadStream blobReadStream,
                                                          @Nullable Digest expectedSha256Digest,
                                                          @Nullable DigestReadStream digestReadStream) {
        if (expectedSha256Digest != null) {
            Preconditions.checkNotNull(digestReadStream);
            Digest calculatedDigest = digestReadStream.getCalculatedDigest();
            Preconditions.checkState(calculatedDigest.getAlgorithm().equals(DigestAlgorithm.SHA256));


            if (!calculatedDigest.equals(expectedSha256Digest)) {
                throw new ContentSha256MismatchException(
                        "The provided 'x-amz-content-sha256' header does not match what was computed.");
            }
        }

        String md5 = blobReadStream.getCalculatedDigest().getBase64Encoded();
        assert blobReadStream.getCalculatedDigest().getAlgorithm() == DigestAlgorithm.MD5;
        blobReadStream.getExpectedDigest().ifPresent(expectedDigest -> {
            if (!blobReadStream.getCalculatedDigest().equals(expectedDigest)) {
                throw new InvalidDigestException(
                        "The computed MD5 hash (" + md5 + ") does not match the expected MD5 hash (" +
                                expectedDigest.getBase64Encoded() + ")");
            }
        });

        if (blobReadStream.getBytesTransferred() != blobReadStream.getContentLength()) {
            long expectedLen = blobReadStream.getContentLength();
            throw new InvalidContentLengthException(
                    "The size of the body (" + blobReadStream.getBytesTransferred() +
                            " bytes) does not match the expected content length (" + expectedLen + " bytes)");
        }
    }

    @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    static EncryptionKey getEncryption(@Nullable String kmsKeyId, DecidingKeyManagementService kms) {
        // How can we tell if we are using KMS?
        //  - When encrypting data we look to see if there is a KmsKeyId associated with the bucket, if there is
        //    we are using KMS, if not we should use KmsSecretStoreImpl (which uses a master encryption key stored
        //    as a secret).
        //  - When decrypting data we look at the key version associated with the data. If the data has a key version
        //    then we encrypted the data using KmsSecretStoreImpl (secrets have versions). If not, we use KMS to
        //    decrypt the key. Note that we do NOT need the OCID of the KMS key for decryption because an encrypted
        //    KMS key is self-identifying -- it contains the ID of the key it is encrypted with.
        final boolean usingKms = !Strings.isNullOrEmpty(kmsKeyId);
        final String encryptionKeyId = usingKms ? kmsKeyId : Secrets.ENCRYPTION_MASTER_KEY;
        return kms.generateKey(encryptionKeyId,
                EncryptionConstants.AES_ALGORITHM, EncryptionConstants.AES_KEY_SIZE_BYTES);
    }

    public CompletableFuture<PartMetadata> putPart(RoutingContext context,
                                                   AuthenticationInfo authInfo,
                                                   CreatePartRequest createPartReq) {
        return putPart(context, authInfo, createPartReq, null);
    }


    public CompletableFuture<PartMetadata> putPart(RoutingContext context,
                                                   AuthenticationInfo authInfo,
                                                   CreatePartRequest createPartReq,
                                                   CommonUtils.TimerCallback timerCallback) {
        Validator.validateV2Namespace(createPartReq.getNamespace());
        Validator.validateBucket(createPartReq.getBucketName());

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();
        final String rid = commonContext.getOpcRequestId();

        HttpPathHelpers.logMultipartPathParameters(
                scope,
                createPartReq.getNamespace(),
                createPartReq.getBucketName(),
                createPartReq.getObjectName(),
                createPartReq.getUploadId(),
                String.valueOf(createPartReq.getUploadPartNum()));

        final PartKey partKey = new PartKey(createPartReq.getUploadId(), createPartReq.getUploadPartNum());

        TrafficRecorder.getVertxRecorder().requestStarted(
                createPartReq.getNamespace(),
                rid, TrafficRecorder.RequestType.PutObject, createPartReq.getSizeInBytes());
        return getBucketMetadata(context, scope, createPartReq.getNamespace(), createPartReq.getBucketName(), Api.V2)
                .thenComposeAsync(
                        bucketInfo -> writePart(context, createPartReq, bucketInfo, authInfo, partKey, timerCallback),
                VertxExecutor.eventLoop())
                .whenComplete((n, t) -> {
                    TrafficRecorder.getVertxRecorder().requestEnded(rid);
                });
    }

    private CompletableFuture<PartMetadata> writePart(RoutingContext context,
                                                      CreatePartRequest createPartReq,
                                                      WSTenantBucketInfo bucketInfo,
                                                      AuthenticationInfo authInfo,
                                                      PartKey partKey,
                                                      CommonUtils.TimerCallback timerCallback) {
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final CommonRequestContext commonContext = wsRequestContext.getCommonRequestContext();
        final MetricScope scope = commonContext.getMetricScope();

        final BlobReadStream rs = createPartReq.getBlobReadStream();
        final String ifMatchEtag = createPartReq.getIfMatchEtag().orElse(null);
        final String ifNoneMatchEtag = createPartReq.getIfNoneMatchEtag().orElse(null);

        // This value is set in one completion stage and used from another, which means it will be written by one thread
        // and read from another. Having it be an atomic reference makes that thread-safe (as would a volatile).
        final AtomicReference<ObjectId> objIdRef = new AtomicReference<>();

        return VertxUtil.runAsync(() -> {
            Backend.authorizeOperationForPermissions(scope,
                    context, authorizer, authInfo, bucketInfo,
                    CasperOperation.UPLOAD_PART,
                    CasperPermission.OBJECT_CREATE);

            final String namespace = bucketInfo.getNamespaceKey().getName();
            final Optional<Tenant> resourceTenant =
                metadataClient.getTenantByCompartmentId(bucketInfo.getCompartment(), namespace);
            if (resourceTenant.isPresent()) {
                wsRequestContext.setResourceTenantOcid(resourceTenant.get().getId());
            }

            if (resourceTenant.isPresent() &&
                !BucketBackend.InternalBucketType.isInternalBucket(createPartReq.getBucketName())) {
                evaluateStorageQuota(namespace, resourceTenant.get().getId(),
                        bucketInfo.getCompartment(), createPartReq.getSizeInBytes());
            }

            WSRequestContext.get(context).setBucketOcid(bucketInfo.getOcid());
            WSRequestContext.get(context).setBucketCreator(bucketInfo.getCreationUser());
            WSRequestContext.get(context).setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(bucketInfo.getOptions()));
            try {
                objIdRef.set(MetricScope.timeScopeF(scope, "objectMds::beginPart",
                        innerScope -> Failsafe.with(retryPolicy).get(() -> {
                            final BeginPartRequest.Builder builder = BeginPartRequest.newBuilder()
                                    .setBucketToken(bucketInfo.getMdsBucketToken().toByteString())
                                    .setObjectKey(Backend.objectKey(createPartReq.getObjectName(),
                                            bucketInfo.getNamespaceKey().getName(), bucketInfo.getBucketName(),
                                            bucketInfo.getNamespaceKey().getApi()))
                                    .setPartKey(MdsPartKey.newBuilder()
                                            .setUploadId(partKey.getUploadId())
                                            .setUploadPartNum(partKey.getUploadPartNum())
                                            .build());
                            if (ifMatchEtag != null) {
                                builder.setIfMatchEtag(ifMatchEtag);
                            }
                            if (ifNoneMatchEtag != null) {
                                builder.setIfNoneMatchEtag(ifNoneMatchEtag);
                            }
                            final BeginPartResponse response = PutObjectBackendHelper.invokeMds(
                                    MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_BEGIN_PART,
                                    false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                            c, objectDeadlineSeconds, commonContext).beginPart(builder.build())));
                            ServiceLogsHelper.logServiceEntry(context);
                            return new ObjectId(response.getObjectId());
                        })));
            } catch (ConcurrentObjectModificationException ex) {
                throw new ConcurrentObjectModificationException("Too much concurrency on the same upload", ex);
            }
            if (timerCallback != null) {
                timerCallback.execute();
            }
        }).thenComposeAsync(v ->
                createChunk(
                        bucketInfo,
                        null,  // previous stream
                        createPartReq.getObjectName(),
                        rs,
                        null,  // transaction ID
                        objIdRef.get(),
                        commonContext,
                        0,  // sequence number
                        createPartReq.getSizeInBytes(),
                        0,  // bytes sent so far
                        bucketInfo.getKmsKeyId().orElse(null),
                        metersDebugEnabled),
                VertxExecutor.eventLoop()
        ).thenApplyAsync(objId -> {
            validateStreamAfterObjectFinished(rs, null, null);

            try {
                final StoragePartInfo partInfo = MetricScope.timeScopeF(scope, "objectMds::finishPart",
                        innerScope -> Failsafe.with(retryPolicy).get(() -> {
                            final FinishPartRequest.Builder builder = FinishPartRequest.newBuilder()
                                    .setBucketToken(bucketInfo.getMdsBucketToken().toByteString())
                                    .setObjectKey(Backend.objectKey(createPartReq.getObjectName(),
                                            bucketInfo.getNamespaceKey().getName(), bucketInfo.getBucketName(),
                                            bucketInfo.getNamespaceKey().getApi()))
                                    .setPartKey(MdsPartKey.newBuilder()
                                            .setUploadId(partKey.getUploadId())
                                            .setUploadPartNum(partKey.getUploadPartNum())
                                            .build())
                                    .setObjectId(objIdRef.get().getId())
                                    .setMd5(rs.getCalculatedDigest().getBase64Encoded());
                            if (ifMatchEtag != null) {
                                builder.setIfMatchEtag(ifMatchEtag);
                            }
                            if (ifNoneMatchEtag != null) {
                                builder.setIfNoneMatchEtag(ifNoneMatchEtag);
                            }
                            final FinishPartResponse response = PutObjectBackendHelper.invokeMds(
                                    MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_FINISH_PART,
                                    false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                            c, objectDeadlineSeconds, commonContext).finishPart(builder.build())));
                            return BackendConversions.mdsPartInfoToStoragePartInfo(response.getPart());
                        }));
                return new PartMetadata(partInfo.getKey().getUploadPartNum(), partInfo.getETag(), partInfo.getMd5(),
                        partInfo.getTotalSizeInBytes(), Date.from(partInfo.getModificationTime()));
            } catch (ConcurrentModificationException ex) {
                throw new ConcurrentObjectModificationException("Too much concurrency on the same upload", ex);
            }
        }, VertxExecutor.workerThread())
        .whenCompleteAsync((result, throwable) -> {
            scope.annotate("totalBytesTransferred", rs.getBytesTransferred());
            if (throwable != null) {
                final ObjectId objectId = objIdRef.get();
                // If beginPart failed, objIdRef won't be set. In that case, we don't need to abortPendingPart.
                if (objectId == null) {
                    return;
                }
                final MetricScope innerScope = scope.child("objectMds::abortPendingPart");
                final AbortPartRequest request = AbortPartRequest.newBuilder()
                        .setBucketToken(bucketInfo.getMdsBucketToken().toByteString())
                        .setObjectKey(Backend.objectKey(createPartReq.getObjectName(),
                                bucketInfo.getNamespaceKey().getName(), bucketInfo.getBucketName(),
                                bucketInfo.getNamespaceKey().getApi()))
                        .setPartKey(MdsPartKey.newBuilder()
                                .setUploadId(partKey.getUploadId())
                                .setUploadPartNum(partKey.getUploadPartNum())
                                .build())
                        .setObjectId(objectId.getId())
                        .build();
                try {
                    PutObjectBackendHelper.invokeMds(
                            MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_ABORT_PENDING_PART,
                            false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                    c, objectDeadlineSeconds, commonContext).abortPart(request)));
                } catch (Exception ex) {
                    innerScope.fail(ex);
                    LOG.warn("Failed to abort a pending part with ID {} on key {}", objIdRef.get(), partKey, ex);
                } finally {
                    innerScope.end();
                }
            }
        }, VertxExecutor.workerThread());
    }

    private void evaluateStorageQuota(String namespace,
                                      String tenantId,
                                      String compartment,
                                      long size) {
        // No need to check storage limit for internal bucket.
        // If we fail to get destination tenant ocid from identity, then fail open.
        if (quotaEvaluationEnabled) {
            quotaEvaluator.evaluateAddUsage(namespace, tenantId, compartment, size);
        } else {
            verifyStorageCapacityIsEnough(namespace, tenantId, size);
        }
    }

    private void verifyStorageCapacityIsEnough(String namespace, String tenantId, Long newObjectSizeInBytes) {
        final long limitForTenant = getStorageLimitBytes(tenantId);

        if (limitForTenant == Limits.DEFAULT_NO_STORAGE_LIMIT_BYTES) {
            return;
        }

        if (limitForTenant == 0) {
            // Don't allow writing new objects/parts when the limit is 0. No need to check usage in this case.
            throw new StorageLimitExceededException("Storage limit exceeded for tenancy during upload");
        }

        final Map<String, Long> map = usageCache.getUsageFromCache(namespace);
        final long usageForTenant = Math.max(0, map.getOrDefault(namespace, 0L));

        // Make sure there is sufficient storage capacity remaining to add the new object. If capacity is full,
        // return StorageLimitExceeded error, even for 0 byte objects.
        // e.g. storage limit bytes is 10, usage is 5. We allow to put object with 5 bytes.
        // e.g. storage limit bytes is 10, usage is 10. We don't allow to put any object (including 0 byte object)
        // e.g. storage limit bytes is 0, usage is 0. We don't allow to put any object (including 0 byte object)
        final long remainingStorageCapacity = limitForTenant - usageForTenant;
        if (remainingStorageCapacity < newObjectSizeInBytes ||
                (remainingStorageCapacity == newObjectSizeInBytes && newObjectSizeInBytes == 0)) {
            throw new StorageLimitExceededException("Storage limit exceeded for tenancy during upload");
        }
    }

    private long getStorageLimitBytes(String tenantId) {
        final Long limitForTenant = limits.getStorageLimitBytes(tenantId);
        return Math.max(Limits.DEFAULT_NO_STORAGE_LIMIT_BYTES, limitForTenant);
    }

    /**
     * This is used to hold data passed back in an AuthorizationResponse when making an authorization call.
     * Previously we just needed a true/false answer to whether the passed-in permissions were authorized or not.
     * Now we need to know about the actual permissions that are authorized. When checking to see if permissions are
     * authorized, we pass in the set of permissions we want to check, and the response will return the permissions
     * that are authorized.
     */
    private static final class PutObjectAuthorizationResult {
        private final boolean authorized;
        private final Set<CasperPermission> casperPermissions;

        private PutObjectAuthorizationResult(boolean authorized, Set<CasperPermission> casperPermissions) {
            this.authorized = authorized;
            this.casperPermissions = Preconditions.checkNotNull(casperPermissions);
        }

        public boolean isAuthorized() {
            return authorized;
        }

        public Set<CasperPermission> getCasperPermissions() {
            return casperPermissions;
        }
    }
}
