package com.oracle.pic.casper.webserver.api.backend;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.oracle.bmc.objectstorage.model.BucketOptionsDetails;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.v2.BucketCacheConfiguration;
import com.oracle.pic.casper.common.config.v2.CompartmentCacheConfiguration;
import com.oracle.pic.casper.common.config.v2.ServicePrincipalConfiguration;
import com.oracle.pic.casper.common.config.v2.StorageMeteringConfiguration;
import com.oracle.pic.casper.common.encryption.EncryptionKey;
import com.oracle.pic.casper.common.encryption.EncryptionUtils;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.exceptions.BucketNotAssociateWithKmsException;
import com.oracle.pic.casper.common.exceptions.InternalServerErrorException;
import com.oracle.pic.casper.common.exceptions.NoSuchCORSException;
import com.oracle.pic.casper.common.exceptions.NoSuchLifecycleException;
import com.oracle.pic.casper.common.exceptions.NoSuchPolicyException;
import com.oracle.pic.casper.common.exceptions.NoSuchReplicationException;
import com.oracle.pic.casper.common.exceptions.NoSuchWebsiteException;
import com.oracle.pic.casper.common.json.JsonSerializer;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.metrics.MetricScopeWriter;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.model.BucketMeterFlag;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.common.model.LifecycleEngineConstants;
import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.model.ScanResults;
import com.oracle.pic.casper.common.replication.ReplicationOptions;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.tracing.Tracers;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.util.IdUtil;
import com.oracle.pic.casper.common.util.ListUtils;
import com.oracle.pic.casper.common.util.ListableResponse;
import com.oracle.pic.casper.common.util.ObjectLifecycleHelper;
import com.oracle.pic.casper.common.util.ParUtil;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.mds.loadbalanced.MdsExecutor;
import com.oracle.pic.casper.mds.object.GetStatForBucketRequest;
import com.oracle.pic.casper.mds.object.GetStatForBucketResponse;
import com.oracle.pic.casper.mds.object.ObjectServiceGrpc.ObjectServiceBlockingStub;
import com.oracle.pic.casper.mds.tenant.CreateBucketRequest;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.mds.tenant.UpdateBucketRequest;
import com.oracle.pic.casper.mds.tenant.UpdateNamespaceRequest;
import com.oracle.pic.casper.mds.workrequest.CreateWorkRequestDetails;
import com.oracle.pic.casper.mds.workrequest.CreateWorkRequestResponse;
import com.oracle.pic.casper.mds.workrequest.MdsWorkRequestType;
import com.oracle.pic.casper.mds.workrequest.WorkRequestMetadataServiceGrpc.WorkRequestMetadataServiceBlockingStub;
import com.oracle.pic.casper.metadata.serialization.MetadataSerialization;
import com.oracle.pic.casper.metadata.serialization.TagSerialization;
import com.oracle.pic.casper.metadata.utils.CryptoUtils;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.BucketKey;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.objectmeta.ObjectDbStats;
import com.oracle.pic.casper.objectmeta.TenantDb;
import com.oracle.pic.casper.objectmeta.UnexpectedEntityTagException;
import com.oracle.pic.casper.objectmeta.input.BucketSortBy;
import com.oracle.pic.casper.objectmeta.input.BucketSortOrder;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.AuthorizationResponse;
import com.oracle.pic.casper.webserver.api.auth.Authorizer;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.KmsKeyUpdateAuth;
import com.oracle.pic.casper.webserver.api.auth.TaggingOperation;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpPathHelpers;
import com.oracle.pic.casper.webserver.api.common.MdsTransformer;
import com.oracle.pic.casper.webserver.api.common.MetadataUpdater;
import com.oracle.pic.casper.webserver.api.common.OptionsUpdater;
import com.oracle.pic.casper.webserver.api.model.Bucket;
import com.oracle.pic.casper.webserver.api.model.BucketCreate;
import com.oracle.pic.casper.webserver.api.model.BucketProperties;
import com.oracle.pic.casper.webserver.api.model.BucketSummary;
import com.oracle.pic.casper.webserver.api.model.BucketUpdate;
import com.oracle.pic.casper.webserver.api.model.NamespaceMetadata;
import com.oracle.pic.casper.webserver.api.model.NamespaceSummary;
import com.oracle.pic.casper.webserver.api.model.ObjectLifecycleRule;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.model.PutObjectLifecyclePolicyDetails;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketSummary;
import com.oracle.pic.casper.webserver.api.model.exceptions.BucketAlreadyExistsException;
import com.oracle.pic.casper.webserver.api.model.exceptions.BucketNotEmptyException;
import com.oracle.pic.casper.webserver.api.model.exceptions.BucketNotReadOnlyException;
import com.oracle.pic.casper.webserver.api.model.exceptions.ConcurrentBucketUpdateException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InsufficientServicePermissionsException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidMetadataException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidTagsException;
import com.oracle.pic.casper.webserver.api.model.exceptions.MissingPublicBucketLimitsException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchCompartmentIdException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchNamespaceException;
import com.oracle.pic.casper.webserver.api.model.exceptions.ParStillExistsException;
import com.oracle.pic.casper.webserver.api.model.serde.OptionsSerialization;
import com.oracle.pic.casper.webserver.api.replication.ReplicationEnforcer;
import com.oracle.pic.casper.webserver.api.replication.ReplicationUtils;
import com.oracle.pic.casper.webserver.api.s3.model.BucketAccelerateConfiguration;
import com.oracle.pic.casper.webserver.api.s3.model.BucketLoggingStatus;
import com.oracle.pic.casper.webserver.api.s3.model.BucketNotificationConfiguration;
import com.oracle.pic.casper.webserver.api.s3.model.BucketRequestPayment;
import com.oracle.pic.casper.webserver.api.s3.model.LocationConstraint;
import com.oracle.pic.casper.webserver.api.usage.QuotaEvaluator;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.webserver.auth.limits.Limits;
import com.oracle.pic.casper.webserver.server.MdsClients;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.workrequest.reencrypt.ReencryptBucketRequestDetail;
import com.oracle.pic.identity.authentication.Claim;
import com.oracle.pic.identity.authentication.ClaimType;
import com.oracle.pic.identity.authentication.Constants;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.PrincipalImpl;
import com.oracle.pic.identity.authentication.PrincipalType;
import com.oracle.pic.tagging.client.tag.TagSet;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.oracle.pic.casper.common.util.ObjectLifecycleHelper.CLEAR_LIFECYCLE_POLICY;

public final class BucketBackend {

    private static final Logger LOG = LoggerFactory.getLogger(BucketBackend.class);

    private static final int S3_LIST_BUCKETS_CHUNK = 1000;

    private static final int INTERNAL_BUCKET_NUM_SHARDS = 1;

    private final TenantBackend tenantBackend;
    private final Authorizer authorizer;
    private final JsonSerializer jsonSerializer;
    private final DecidingKeyManagementService kms;
    private final CompartmentCacheConfiguration compartmentCacheConfiguration;
    private final ServicePrincipalConfiguration servicePrincipalConfiguration;
    private final Limits limits;
    private final QuotaEvaluator quotaEvaluator;
    private final boolean quotaEvaluationEnabled;
    private final MdsExecutor<WorkRequestMetadataServiceBlockingStub> workRequestClient;
    private final long maxWorkRequestPerTenant;
    private final long grpcWorkRequestDeadlineSeconds;
    private final MdsExecutor<ObjectServiceBlockingStub> objectClient;
    private final long grpcObjectDeadlineSeconds;
    private final long grpcListObjectDeadlineSeconds;
    private final int listObjectsMaxMessageBytes;
    private final LoadingCache<BucketKey, Optional<WSTenantBucketInfo>> cachedBuckets;
    private final BucketCacheConfiguration bucketCacheConfiguration;
    private final MetricScopeWriter metricScopeWriter;

    public enum InternalBucketType {

        PAR(ParUtil.PAR_BUCKET_NAME_SUFFIX),
        LIFECYCLE(ObjectLifecycleHelper.LIFECYCLE_NAME_SUFFIX);
        private String suffix;

        InternalBucketType(String suffix) {
            this.suffix = suffix;
        }

        public String getSuffix() {
            return suffix;
        }

        public static boolean isInternalBucket(String bucketName) {
            for (InternalBucketType type : values()) {
                if (bucketName.endsWith(type.getSuffix())) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final Predicate<BucketSummary> INTERNAL_BUCKET_FILTER = bucketSummary -> {
        try {
            Validator.validateBucket(bucketSummary.getBucketName());
            return true;
        } catch (Exception e) {
            return false;
        }
    };

    public BucketBackend(TenantBackend tenantBackend,
                         Authorizer authorizer,
                         JsonSerializer jsonSerializer,
                         DecidingKeyManagementService kms,
                         CompartmentCacheConfiguration compartmentCacheConfiguration,
                         ServicePrincipalConfiguration servicePrincipalConfiguration,
                         Limits limits,
                         QuotaEvaluator quotaEvaluator,
                         boolean quotaEvaluationEnabled,
                         MdsClients mdsClients,
                         long maxWorkRequestPerTenant,
                         BucketCacheConfiguration bucketCacheConfiguration,
                         Ticker ticker,
                         MetricScopeWriter metricScopeWriter) {
        this.tenantBackend = tenantBackend;
        this.authorizer = authorizer;
        this.jsonSerializer = jsonSerializer;
        this.kms = kms;
        this.compartmentCacheConfiguration = compartmentCacheConfiguration;
        this.servicePrincipalConfiguration = servicePrincipalConfiguration;
        this.limits = limits;
        this.quotaEvaluator = quotaEvaluator;
        this.quotaEvaluationEnabled = quotaEvaluationEnabled;
        this.workRequestClient = mdsClients.getWorkrequestMdsExecutor();
        this.maxWorkRequestPerTenant = maxWorkRequestPerTenant;
        this.grpcWorkRequestDeadlineSeconds = mdsClients.getWorkRequestDeadline().getSeconds();
        this.objectClient = mdsClients.getObjectMdsExecutor();
        this.grpcObjectDeadlineSeconds = mdsClients.getObjectRequestDeadline().getSeconds();
        this.grpcListObjectDeadlineSeconds = mdsClients.getListObjectRequestDeadline().getSeconds();
        this.listObjectsMaxMessageBytes = mdsClients.getListObjectsMaxMessageBytes();
        this.bucketCacheConfiguration = bucketCacheConfiguration;
        this.metricScopeWriter = metricScopeWriter;
        this.cachedBuckets = CacheBuilder.newBuilder()
                .maximumSize(bucketCacheConfiguration.getCacheConfiguration().getMaximumSize())
                .expireAfterWrite(bucketCacheConfiguration.getCacheConfiguration().getExpireAfterWrite())
                .refreshAfterWrite(bucketCacheConfiguration.getCacheConfiguration().getRefreshAfterWrite())
                .recordStats()
                .ticker(ticker)
                .build(new CacheLoader<BucketKey, Optional<WSTenantBucketInfo>>() {
                    @Override
                    public Optional<WSTenantBucketInfo> load(BucketKey bucketKey) {
                        final MetricScope metricScope = MetricScope.create("BucketBackend:cachedBuckets:load",
                                Tracers.BUCKET_CACHE_LOAD_TRACER
                                        .buildSpan("BucketBackend:cachedBuckets:load")
                                        // this is super important since we are not thread-per-request
                                        .ignoreActiveSpan()
                                        .startManual(),
                                Tracers.BUCKET_CACHE_LOAD_TRACER);

                        final Optional<WSTenantBucketInfo> wsTenantBucketInfo =
                                getBucketMetadataWithNullValue("internalCacheRefresh",
                                        metricScope,
                                        bucketKey.getNamespace(),
                                        bucketKey.getName(),
                                        bucketKey.getApi());
                        metricScope.end();
                        metricScopeWriter.accept(metricScope);
                        return wsTenantBucketInfo;
                    }
                });
        Metrics.gauge("webServerBucketCache.hitRate", () -> cachedBuckets.stats().hitRate());
    }

    /**
     * Create a new bucket in the given namespace with regular AuthZ.
     *
     * @param context      the common request context used for logging and metrics.
     * @param authInfo     information about the authenticated user making the request.
     * @param bucketCreate the bucket create structure.
     * @return a CompletableFuture that contains either the created bucket or an exception. A partial list of exceptions
     * that can be returned by this method includes: BucketAlreadyExistsException, InvalidBucketNameException,
     * InvalidMetadataException.
     */
    public Bucket createV2Bucket(RoutingContext context, AuthenticationInfo authInfo, BucketCreate bucketCreate) {
        Validator.validateV2Namespace(bucketCreate.getNamespaceName());
        Validator.validateBucket(bucketCreate.getBucketName());
        final Bucket bucket = createBucket(context, authInfo, bucketCreate, Api.V2);
        final WSRequestContext wsContext = WSRequestContext.get(context);
        wsContext.addNewState("bucketName", bucket.getBucketName());
        wsContext.addNewState("eTag", bucket.getETag());
        wsContext.setEtag(bucket.getETag());
        wsContext.setBucketPublicAccessType(bucket.getPublicAccessType());
        return bucket;
    }

    /**
     * Helper method that attempts to create an internal bucket.
     * The tenantOcid must be supplied to handle the case where cross tenant calls initial the internal bucket.
     * https://jira.oci.oraclecorp.com/browse/CASPER-3039
     *
     * @return Bucket
     */
    private Bucket createInternalBucket(RoutingContext context,
                                        AuthenticationInfo authInfo,
                                        String tenantOcid,
                                        String namespace,
                                        String bucketName,
                                        String suffix) {
        Validator.validateInternalBucket(bucketName, suffix);
        LOG.debug("{} internal bucket does not exist, creating it context {}", suffix, context);
        Map<String, String> bucketMetadata = new HashMap<>();
        bucketMetadata.put("CasperInternalBucket", "true");

        final BucketCreate bucketCreate = new BucketCreate(namespace, bucketName, tenantOcid,
                authInfo.getMainPrincipal().getSubjectId(), bucketMetadata);

        LOG.debug("Attempting to create {} internal bucket with authInfo: {}", suffix, authInfo);
        return createBucket(context, authInfo, bucketCreate,
                tags -> Optional.of(new AuthorizationResponse(null, tags, true, ImmutableSet.of())),
                Api.V2, Integer.MAX_VALUE, INTERNAL_BUCKET_NUM_SHARDS);
    }

    /**
     * Create a bucket with regular AuthZ. V1 handlers should directly call this method.
     */
    public Bucket createBucket(RoutingContext context, AuthenticationInfo authInfo, BucketCreate bucketCreate,
                               Api api) {
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final int maxBucketsForTenant = getMaxBucketsForTenant(wsRequestContext.getResourceTenantOcid(), api);
        final int numShardsForTenant = getNumShardsForTenant(wsRequestContext.getResourceTenantOcid(), api);

        return createBucket(context, authInfo, bucketCreate,
                tags -> authorizer.authorizeWithTags(
                        wsRequestContext,
                        authInfo,
                        new NamespaceKey(api, bucketCreate.getNamespaceName()),
                        bucketCreate.getBucketName(),
                        api == Api.V1 ? compartmentCacheConfiguration.getV1Compartment() :
                                bucketCreate.getCompartmentId(),
                        bucketCreate.getBucketPublicAccessType(),
                        CasperOperation.CREATE_BUCKET,
                        TaggingOperation.SET_NEW_TAGS,
                        tags,
                        null,
                        new KmsKeyUpdateAuth(bucketCreate.getKmsKeyId(), null, true),
                        true,
                        false, //Tenancy not deleted
                        CasperPermission.BUCKET_CREATE),
                api, maxBucketsForTenant, numShardsForTenant);
    }

    /**
     * This is somewhat of an internal method that understands the semantic difference between V1 & V2.
     * Most consumers will want to call the overloaded methods where they don't pass an API -- since V1 is deprecated.
     */
    private Bucket createBucket(
            RoutingContext context,
            AuthenticationInfo authInfo,
            BucketCreate bucketCreate,
            Function<TagSet, Optional<AuthorizationResponse>> authorizerProvider,
            Api api,
            int maxBucketsForTenant,
            int numShardsForTenant) {
        Validator.validateMetadata(bucketCreate.getMetadata(), jsonSerializer);
        Validator.validateTags(bucketCreate.getFreeformTags(), (Map) bucketCreate.getDefinedTags(), jsonSerializer);

        final String bucketName = bucketCreate.getBucketName();
        final String namespaceName = bucketCreate.getNamespaceName();
        final BucketKey bucketKey = new BucketKey(namespaceName, bucketName, api.getVersion());
        final TagSet.Builder tagBuilder = TagSet.builder();
        if (bucketCreate.getFreeformTags() != null) {
            tagBuilder.freeformTags(bucketCreate.getFreeformTags());
        }
        if (bucketCreate.getDefinedTags() != null) {
            tagBuilder.definedTags(bucketCreate.getDefinedTags());
        }
        final TagSet tags = tagBuilder.build();

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:createBucket");

        // Note:  The v2 createBucket API doesn't actually pass the bucket name in the path, but we go ahead and log
        // it with the other path parameters anyways.
        HttpPathHelpers.logPathParameters(rootScope, namespaceName, bucketName, null);

        Optional<AuthorizationResponse> authorizedResponse = MetricScope.timeScope(childScope, "authorizer",
                (Function<MetricScope, Optional<AuthorizationResponse>>) (innerScope) ->
                        authorizerProvider.apply(tags));

        // getEncryption will call KMS to generate dataEncryptionKey, KMs will try to authorize whether Casepr service
        // has permission to use kmsKey. This should happen after Casper call authZ to authorize user's permission to
        // use the key and the permission to associate key with bucket.
        final EncryptionKey metadataEncryptionKey = Backend.getEncryption(bucketCreate.getKmsKeyId(), kms);

        // call kms to decrypt the key to make sure Casper has the permission to decrypt key. This check only happens
        // during bucket creation.
        Backend.verifyDecryptKey(bucketCreate.getKmsKeyId(), metadataEncryptionKey, kms);

        if (!authorizedResponse.isPresent()) {
            throw new BucketAlreadyExistsException(bucketAlreadyExistsMsg(bucketName, namespaceName));
        }

        if (!authorizedResponse.get().areTagsAuthorized()) {
            final String errorMessage = authorizedResponse.get().getErrorMessage() == null ?
                    String.format("Failed to authorize/validate tags when create bucket '%s' in namespace '%s'.",
                            bucketName,
                            namespaceName) :
                    authorizedResponse.get().getErrorMessage();
            throw new InvalidTagsException(errorMessage);
        }

        final TagSet authorizedTags = authorizedResponse.get().getTagSet().orElse(TagSet.builder().build());
        // We need to validate tags here again, because identity may add default tags in addition to the user defined
        // tags which may exceed the the total tag size 3500 bytes.
        Validator.validateTags(authorizedTags.getFreeformTags(), (Map) authorizedTags.getDefinedTags(), jsonSerializer);
        final String opcRequestId = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        Optional<WSTenantBucketInfo> optTenantBucketInfo =
                tenantBackend.getBucketInfo(opcRequestId, childScope, api, namespaceName, bucketName);
        if (optTenantBucketInfo.isPresent()) {
            if (api == Api.V1) {
                return null;
            }
            throw new BucketAlreadyExistsException(bucketAlreadyExistsMsg(bucketName, namespaceName));
        }

        // Check with the limits service if the tenant is allowed to to create a public bucket, unless V1 was used
        final String tenantOcid = WSRequestContext.get(context).getResourceTenantOcid();
        if (bucketCreate.getBucketPublicAccessType() != BucketPublicAccessType.NoPublicAccess && !api.equals(Api.V1)) {
            // other 2 options are public bucket type
            boolean isPublicBucketEnabled = limits.isPublicBucketEnabled(tenantOcid);
            if (!isPublicBucketEnabled) {
                LOG.debug("Tenant does not have permission in limits to create public bucket");
                throw new MissingPublicBucketLimitsException(
                        tenantMissingPublicBucketLimitsPermission(bucketName, namespaceName));
            }
        }

        final String encryptedMetadata = EncryptionUtils.encryptString(
                MetadataSerialization.serializeMetadata(bucketCreate.getMetadata()), metadataEncryptionKey);

        CreateBucketRequest.Builder builder = CreateBucketRequest.newBuilder()
                .setBucketKey(MdsTransformer.toMdsBucketKey(bucketKey))
                .setOcid(IdUtil.newBucketOcid())
                .setCompartmentId(bucketCreate.getCompartmentId())
                .setCreationUser(bucketCreate.getCreatedBy())
                .setMetadata(encryptedMetadata)
                .setEncryptionKey(MdsTransformer.toMdsEncryptionKey(metadataEncryptionKey))
                .setAccessType(MdsTransformer.toPublicAccessType(bucketCreate.getBucketPublicAccessType()))
                .setStorageTier(MdsTransformer.toStorageTier(bucketCreate.getBucketStorageTier()))
                .setAuditMode(MdsTransformer.toAuditMode(
                        bucketCreate.getObjectLevelAuditMode().orElse(ObjectLevelAuditMode.Disabled)))
                .setMaxBuckets(maxBucketsForTenant)
                .setNumShards(numShardsForTenant)
                .setTags(TagSerialization.encodeTags(authorizedTags));
        if (bucketCreate.isObjectEventsEnabled()) {
            builder.setOptions(OptionsSerialization.serializeOptions(bucketCreate.getOptions()));
        }
        if (bucketCreate.getKmsKeyId() != null) {
            builder.setKmsKey(bucketCreate.getKmsKeyId());
        }
        CreateBucketRequest createBucketRequest = builder.build();

        switch (api) {
            case V1:
                return BackendConversions.wsBucketToBucket(
                        tenantBackend.createBucket(context, childScope, createBucketRequest), kms);
            case V2:
                try {
                    WSTenantBucketInfo bucketInfo =
                            tenantBackend.createBucket(context, childScope, createBucketRequest);
                    Bucket result = BackendConversions.wsBucketToBucket(bucketInfo, kms);
                    WSRequestContext.get(context).setNewkmsKeyId(bucketCreate.getKmsKeyId());
                    return result;
                } catch (com.oracle.pic.casper.objectmeta.BucketAlreadyExistsException ex) {
                    throw new BucketAlreadyExistsException(
                            bucketAlreadyExistsMsg(bucketCreate.getBucketName(), bucketCreate.getNamespaceName()), ex);
                } catch (com.oracle.pic.casper.objectmeta.InvalidMetadataException ex) {
                    throw new InvalidMetadataException(
                            invalidMetadataMsg(bucketCreate.getBucketName(), bucketCreate.getNamespaceName()), ex);
                }
            default:
                throw new IllegalStateException("Should never happen");
        }

    }

    private static String tenantMissingPublicBucketLimitsPermission(String bucketName, String namespaceName) {
        return "Tenant doesn't have permission to create public bucket '" + bucketName +
                "' in namespace '" + namespaceName + "'";
    }

    private static String bucketAlreadyExistsMsg(String bucketName, String namespaceName) {
        return "Either the bucket '" + bucketName + "' in namespace '" + namespaceName +
                "' already exists or you are not authorized to create it";
    }

    private static String invalidMetadataMsg(String bucketName, String namespaceName) {
        return "Metadata size is too large when creating bucket '" + bucketName + "' in namespace '" +
                namespaceName + "'";
    }

    /**
     * Head the requested bucket.
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param ns         the namespace in which to find the bucket.
     * @param bucketName the name of the bucket to fetch.
     * @return a CompletableFuture that contains either an optional bucket or an exception. If the optional is empty,
     * the requested bucket could not be found. A partial list of exceptions that can be returned by this method
     * includes: InvalidBucketNameException.
     */
    public Optional<Bucket> headBucket(RoutingContext context, AuthenticationInfo authInfo, String ns,
                                       String bucketName) {
        Validator.validateV2Namespace(ns);
        Validator.validateBucket(bucketName);
        return headBucket(context, ns, bucketName,
                bucket -> authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        bucket.getNamespaceKey(),
                        bucketName,
                        bucket.getCompartment(),
                        bucket.getPublicAccessType(),
                        CasperOperation.HEAD_BUCKET,
                        bucket.getKmsKeyId().orElse(null),
                        true,
                        bucket.isTenancyDeleted(),
                        CasperPermission.BUCKET_INSPECT).isPresent());
    }

    /**
     * Helper method that creates an internal bucket if it does not already exist.
     * The method first does a GET on the bucket, and will attempt to create it if it is not found.
     * In case there are concurrent attempts to create the same Bucket, the loser can just fall back to HEAD on
     * the Bucket and walk away.
     *
     * @param context            context needed by Backend
     * @param authInfo           information about the authenticated user making the request.
     * @param tenantOcid         the tenantOcid to create the bucket in -- necessary for cross tenant calls
     * @param namespace          the namespace in which this bucket may exist.
     * @param internalBucketName the name of the internal bucket.
     * @param suffix             the suffix added to internalBucketName of the type of internal bucket to create
     * @return the found or newly created bucket
     */
    public Bucket headOrCreateInternalBucket(RoutingContext context,
                                             AuthenticationInfo authInfo,
                                             String tenantOcid,
                                             String namespace,
                                             String internalBucketName,
                                             String suffix) {
        Validator.validateInternalBucket(internalBucketName, suffix);
        try {
            return headInternalBucket(context, namespace, internalBucketName).orElseGet(() ->
                    createInternalBucket(context, authInfo, tenantOcid, namespace, internalBucketName, suffix));
        } catch (BucketAlreadyExistsException baee) {
            // someone already beat us to creating the bucket. simply get it again
            LOG.debug("{} bucket already created before, getting it, context {}", suffix, context);
            return headInternalBucket(context, namespace, internalBucketName).get();
        }
    }

    /**
     * Get the requested internal bucket, bypassing AuthZ.
     */
    private Optional<Bucket> headInternalBucket(RoutingContext context, String namespace, String internalBucketName) {
        return headBucket(context, namespace, internalBucketName, bucket -> true);
    }

    /**
     * Head bucket with a custom authorizer, allowing bypassing AuthZ.
     */
    private Optional<Bucket> headBucket(RoutingContext context, String ns, String bucketName,
                                        Function<WSTenantBucketInfo, Boolean> authorizerProvider) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:headBucket");
        HttpPathHelpers.logPathParameters(rootScope, ns, bucketName, null);
        final String opcRequestId = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        return tenantBackend.getBucketInfo(opcRequestId, childScope, Api.V2, ns, bucketName)
                .filter(bucket -> MetricScope.timeScope(childScope,
                        "authorizer",
                        (Function<MetricScope, Boolean>) (innerScope) -> authorizerProvider.apply(bucket)))
                .map(bucket -> BackendConversions.wsBucketToBucket(bucket, kms));
    }

    /**
     * Get the requested bucket with regular AuthZ.
     *
     * @param context          the common request context used for logging and metrics.
     * @param authInfo         information about the authenticated user making the request.
     * @param namespace        the namespace in which to find the bucket.
     * @param bucketName       the name of the bucket to fetch.
     * @param bucketProperties the bucket properties to include
     * @return a CompletableFuture that contains either an optional bucket or an exception. If the optional is empty,
     * the requested bucket could not be found. A partial list of exceptions that can be returned includes:
     * InvalidBucketNameException.
     */
    public Optional<Bucket> getBucket(RoutingContext context, AuthenticationInfo authInfo, String namespace,
                                      String bucketName, Set<BucketProperties> bucketProperties) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:getBucket");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);
        final String opcRequestId = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        return tenantBackend.getBucketInfo(opcRequestId, childScope, Api.V2, namespace, bucketName)
                .filter(bucket -> MetricScope.timeScope(childScope, "authorizer",
                        (Function<MetricScope, Boolean>) (innerScope) -> authorizer.authorizeWithTags(
                                WSRequestContext.get(context),
                                authInfo,
                                bucket.getNamespaceKey(),
                                bucketName,
                                bucket.getCompartment(),
                                bucket.getPublicAccessType(),
                                CasperOperation.GET_BUCKET,
                                TaggingOperation.SET_EXISTING_TAGS,
                                null,
                                bucket.getTags(),
                                new KmsKeyUpdateAuth(null, // Setting newKmsKey=null implies no update for the key
                                        bucket.getKmsKeyId().orElse(null),
                                        true), // Setting this to true/false doens't have any affect in this case.
                                // Since there is no key update, no authz will be performed anyway.
                                true,
                                bucket.isTenancyDeleted(),
                                CasperPermission.BUCKET_READ).isPresent()))
                .map(bucketInfo -> {
                    Bucket bucket = BackendConversions.wsBucketToBucket(bucketInfo, kms);
                    if (bucketProperties.contains(BucketProperties.APPROXIMATE_COUNT) ||
                            bucketProperties.contains(BucketProperties.APPROXIMATE_SIZE)) {

                        ObjectDbStats bucketStats = getStatForBucket(context, bucketInfo);
                        if (bucketProperties.contains(BucketProperties.APPROXIMATE_COUNT)) {
                            bucket = Bucket.builder(bucket).approximateCount(bucketStats.getNumObjects()).build();
                        }
                        if (bucketProperties.contains(BucketProperties.APPROXIMATE_SIZE)) {
                            bucket = Bucket.builder(bucket)
                                    .approximateSize(
                                            bucketStats.getObjectStorageSize().longValue()).build();
                        }
                    }
                    return bucket;
                });
    }

    public void addBucketMeterSetting(RoutingContext context,
                                      AuthenticationInfo authInfo,
                                      String ns,
                                      String bucketName,
                                      BucketMeterFlag meterSetting,
                                      HttpServerResponse response) {
        Validator.validateBucket(bucketName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:addBucketMeterSetting");
        HttpPathHelpers.logPathParameters(rootScope, ns, bucketName, null);

        final WSTenantBucketInfo bucketInfo = getBucketMetadata(context, childScope, ns, bucketName);

        if (!MetricScope.timeScope(childScope,
                "authorizer", (Function<MetricScope, Boolean>) (innerScope) -> authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        bucketInfo.getNamespaceKey(),
                        bucketName,
                        bucketInfo.getCompartment(),
                        bucketInfo.getPublicAccessType(),
                        CasperOperation.ADD_BUCKET_METER_SETTING,
                        bucketInfo.getKmsKeyId().orElse(null),
                        true,
                        bucketInfo.isTenancyDeleted(),
                        CasperPermission.BUCKET_UPDATE).isPresent())) {
            throw new NoSuchBucketException(noSuchBucketMsg(bucketName, ns));
        }

        tenantBackend.addBucketMeterSetting(context, childScope,
                                            bucketInfo,
                                            meterSetting);

        response.putHeader(HttpHeaders.ETAG, bucketInfo.getEtag())
                .setStatusCode(HttpResponseStatus.OK)
                .end();
    }

    public void makeReplicationDestBucketWritable(RoutingContext context,
                                                  AuthenticationInfo authInfo,
                                                  String namespace,
                                                  String bucketName) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:makeReplicationDestBucketWritable");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);

        final WSTenantBucketInfo bucketInfo = getBucketMetadata(context, childScope, namespace, bucketName);

        if (!MetricScope.timeScope(childScope,
                "authorizer", (Function<MetricScope, Boolean>) (innerScope) -> authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        bucketInfo.getNamespaceKey(),
                        bucketName,
                        bucketInfo.getCompartment(),
                        bucketInfo.getPublicAccessType(),
                        CasperOperation.MAKE_BUCKET_WRITABLE,
                        null,
                        true,
                        bucketInfo.isTenancyDeleted(),
                        ReplicationUtils.REPLICATION_PERMISSIONS.toArray(new CasperPermission[0])).isPresent())) {
            throw new NoSuchBucketException(noSuchBucketMsg(bucketName, namespace));
        }

        setBucketUncacheable(context, childScope, bucketInfo.getKey());

        if (bucketInfo.isReplicationEnabled()) {
            throw new BucketNotReadOnlyException(String.format("Bucket %s in namespace %s is not in read-only state",
                    namespace, bucketName));
        }

        Map<String, Object> newOptions = new HashMap<>();
        newOptions.put(ReplicationOptions.XRR_POLICY_ID, null);
        newOptions.put(ReplicationOptions.XRR_POLICY_NAME, null);
        newOptions.put(ReplicationOptions.XRR_SOURCE_REGION, null);
        newOptions.put(ReplicationOptions.XRR_SOURCE_BUCKET, null);
        newOptions.put(ReplicationOptions.XRR_SOURCE_BUCKET_ID, null);

        tenantBackend.updateBucketOptions(context, childScope, bucketInfo, newOptions);
    }

    public void updateBucketReplicationOptions(RoutingContext context,
                                    AuthenticationInfo authInfo,
                                    String namespace,
                                    String bucketName,
                                    Supplier<Map<String, Object>> optionsSupplier) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:updateBucketReplicationOptions");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);

        final WSTenantBucketInfo bucketInfo = getBucketMetadata(context, childScope, namespace, bucketName);

        if (!MetricScope.timeScope(childScope,
                "authorizer", (Function<MetricScope, Boolean>) (innerScope) -> authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        bucketInfo.getNamespaceKey(),
                        bucketName,
                        bucketInfo.getCompartment(),
                        bucketInfo.getPublicAccessType(),
                        CasperOperation.UPDATE_REPLICATION_OPTIONS,
                        null,
                        true,
                        bucketInfo.isTenancyDeleted(),
                        ReplicationUtils.REPLICATION_PERMISSIONS.toArray(new CasperPermission[0]))
                        .isPresent())) {
            throw new NoSuchBucketException(noSuchBucketMsg(bucketName, namespace));
        }

        setBucketUncacheable(context, childScope, bucketInfo.getKey());

        Map<String, Object> newOptions = optionsSupplier.get();
        ReplicationUtils.validateOptions(namespace, bucketName, bucketInfo.getOptions(), newOptions);

        tenantBackend.updateBucketOptions(context, childScope, bucketInfo, newOptions);
    }

    public void updateBucketOptions(RoutingContext context,
                                    AuthenticationInfo authInfo,
                                    String namespace,
                                    String bucketName,
                                    Map<String, Object> newOptions) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:updateBucketOptions");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);

        final WSTenantBucketInfo bucketInfo = getBucketMetadata(context, childScope, namespace, bucketName);

        if (!MetricScope.timeScope(childScope,
                "authorizer", (Function<MetricScope, Boolean>) (innerScope) -> authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        bucketInfo.getNamespaceKey(),
                        bucketName,
                        bucketInfo.getCompartment(),
                        bucketInfo.getPublicAccessType(),
                        CasperOperation.UPDATE_BUCKET_OPTIONS,
                        null,
                        true,
                        bucketInfo.isTenancyDeleted(),
                        CasperPermission.BUCKET_UPDATE).isPresent())) {
            throw new NoSuchBucketException(noSuchBucketMsg(bucketName, namespace));
        }

        setBucketUncacheable(context, childScope, bucketInfo.getKey());

        tenantBackend.updateBucketOptions(context, childScope, bucketInfo, newOptions);
    }

    public BucketOptionsDetails getBucketOptions(RoutingContext context,
                                                 AuthenticationInfo authInfo,
                                                 String ns,
                                                 String bucketName) {
        Validator.validateBucket(bucketName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:getBucketOptions");
        HttpPathHelpers.logPathParameters(rootScope, ns, bucketName, null);

        final WSTenantBucketInfo bucketInfo = getBucketMetadata(context, childScope, ns, bucketName);

        if (!MetricScope.timeScope(childScope,
                "authorizer", (Function<MetricScope, Boolean>) (innerScope) -> authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        bucketInfo.getNamespaceKey(),
                        bucketName,
                        bucketInfo.getCompartment(),
                        bucketInfo.getPublicAccessType(),
                        CasperOperation.GET_BUCKET_OPTIONS,
                        null,
                        true,
                        bucketInfo.isTenancyDeleted(),
                        CasperPermission.BUCKET_READ).isPresent())) {
            throw new NoSuchBucketException(noSuchBucketMsg(bucketName, ns));
        }

        return BucketOptionsDetails.builder().freeformOptions(bucketInfo.getOptions()).build();
    }

    /**
     * Internal method to update bucket. The caller should perform authZ prior to calling this method.
     */
    private Bucket updateBucket(RoutingContext context,
                                AuthenticationInfo authInfo,
                                String namespace,
                                String compartment,
                                String bucketName,
                                String etag,
                                TagSet tagSet,
                                BucketPublicAccessType publicAccessType,
                                ObjectLevelAuditMode auditMode,
                                Map<String, String> metadata,
                                @Nullable String kmsKeyIdUpdate,
                                @Nullable String oldKmsKeyId,
                                @Nullable String updateCurrentLifecyclePolicyEtag,
                                Map<String, Object> options,
                                MetricScope childScope) {
        final UpdateBucketRequest.Builder updateBuilder = UpdateBucketRequest.newBuilder();

        final BucketKey bucketKey = new BucketKey(namespace, bucketName, Api.V2);
        updateBuilder.setBucketKey(MdsTransformer.toMdsBucketKey(bucketKey));
        updateBuilder.setCompartmentId(compartment);

        // Always set matching etag to avoid clashing with a parallel bucket update.
        updateBuilder.setIfMatchEtag(etag);
        updateBuilder.setTags(TagSerialization.encodeTags(tagSet));
        updateBuilder.setAccessType(MdsTransformer.toPublicAccessType(publicAccessType));
        updateBuilder.setAuditMode(MdsTransformer.toAuditMode(auditMode));

        if (kmsKeyIdUpdate != null) {
            updateBuilder.setKmsKey(kmsKeyIdUpdate);
        }

        final EncryptionKey metadataEncryptionKey = Backend.getEncryption(kmsKeyIdUpdate, kms);
        updateBuilder.setEncryptionKey(MdsTransformer.toMdsEncryptionKey(metadataEncryptionKey));

        final String encryptedMetadata = CryptoUtils.encryptMetadata(metadata, metadataEncryptionKey);

        updateBuilder.setMetadata(encryptedMetadata);

        if (updateCurrentLifecyclePolicyEtag != null) {
            updateBuilder.setLifecycleEtag(updateCurrentLifecyclePolicyEtag);
        }

        updateBuilder.setOptions(OptionsSerialization.serializeOptions(options));

        setBucketUncacheable(context, childScope, bucketKey);

        UpdateBucketRequest updateBucketRequest = updateBuilder.build();
        try {
            WSTenantBucketInfo bucketInfo = tenantBackend.updateBucket(context, childScope, updateBucketRequest);
            final WSRequestContext wsRequestContext = WSRequestContext.get(context);
            wsRequestContext.setNewkmsKeyId(kmsKeyIdUpdate);
            wsRequestContext.setOldKmsKeyId(oldKmsKeyId);
            wsRequestContext.setBucketPublicAccessType(bucketInfo.getPublicAccessType());
            wsRequestContext.setEtag(bucketInfo.getEtag());
            return BackendConversions.wsBucketToBucket(bucketInfo, kms);
        } catch (UnexpectedEntityTagException | NoSuchBucketException ex) {
            throw new ConcurrentBucketUpdateException("A concurrent update to bucket '" +
                    bucketName + "' in namespace '" + namespace +
                    "' caused this update to be aborted (no changes were made due to this request)");
        } catch (com.oracle.pic.casper.objectmeta.InvalidMetadataException ex) {
            throw new InvalidMetadataException("Metadata size is too large when update bucket " + bucketName +
                    " in namespace " + namespace);
        }
    }

    /**
     * Partially update a bucket.
     * <p>
     * The bucketUpdate argument is interpreted as follows:
     * - If bucketUpdate.isMissingMetadata() is true, this operation makes no changes.
     * - If bucketUpdate.getMetadata() returns null, this operation removes all existing metadata.
     * - Otherwise, keys in bucketUpdate.getMetadata() that have null values are removed, and any other keys are added
     * or updated (if they already existed). Any keys that already exist, but are not in the new metadata map
     * are left unchanged.
     * <p>
     * - if bucketUpdate.getFreeformTags() is null, no change to freeformTags,
     * otherwise full replace existing freeformTags
     * - if bucketUpdate.getDefinedTags() is null, no change to getDefinedTags,
     * otherwise full replace existing definedTags
     * - objectLifecyclePolicyEtag will be replaced by bucketUpdate.getObjectLifecyclePolicyEtag().
     * if bucketUpdate.getObjectLifecyclePolicyEtag() is empty, no change to objectLifecyclePolicyEtag;
     * if bucketUpdate.getObjectLifecyclePolicyEtag() is CLEAR_LIFECYCLE_POLICY, reset objectLifecyclePolicyEtag to
     * empty, indicating there is no active lifecycle policy available on the bucket.
     * <p>
     * Everything else about the bucket is left unchanged.</p>
     *
     * @param context      the common request context used for logging and metrics.
     * @param authInfo     information about the authenticated user making the request.
     * @param bucketUpdate the update description, interpreted as noted above.
     * @param expectedETag the expected entity tag, or null to match any existing entity tag.
     * @return a CompletableFuture that will contain either the updated bucket or an exception. A partial list of
     * exceptions that can be returned includes: UnexpectedEntityTagException, InvalidBucketNameException and
     * InvalidMetadataException.
     */
    public Bucket updateBucketPartially(RoutingContext context, AuthenticationInfo authInfo, BucketUpdate bucketUpdate,
                                        String expectedETag) {
        Validator.validateV2Namespace(bucketUpdate.getNamespaceName());
        Validator.validateBucket(bucketUpdate.getBucketName());
        Validator.validateTags(bucketUpdate.getFreeformTags(), (Map) bucketUpdate.getDefinedTags(), jsonSerializer);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:updateBucketPartially");
        final String bucketName = bucketUpdate.getBucketName();
        final BucketKey bucketKey = new BucketKey(bucketUpdate.getNamespaceName(), bucketName, Api.V2);
        HttpPathHelpers.logPathParameters(rootScope, bucketUpdate.getNamespaceName(), bucketName, null);

        final UpdateBucketRequest.Builder updateBuilder = UpdateBucketRequest.newBuilder();
        updateBuilder.setBucketKey(MdsTransformer.toMdsBucketKey(bucketKey));
        final String opcRequestId = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        final WSTenantBucketInfo curBucket =
                tenantBackend.getBucketInfo(opcRequestId, childScope, Api.V2, bucketUpdate.getNamespaceName(),
                        bucketName).orElseThrow(() -> new NoSuchBucketException(noSuchBucketMsg(bucketName,
                        bucketUpdate.getNamespaceName())));
        final TagSet oldTagSet = curBucket.getTags();
        final TagSet newTagSet = getUpdatedTagSet(bucketUpdate.getFreeformTags(), bucketUpdate.getDefinedTags(),
                oldTagSet);
        TagSet validatedTagSet;
        final String kmsKeyIdUpdate = getUpdateKmsKeyId(bucketUpdate.getKmsKeyId(),
                curBucket.getKmsKeyId().orElse(null));

        final BucketPublicAccessType publicAccessType = bucketUpdate.getPublicAccessType()
                .orElse(curBucket.getPublicAccessType());
        // Moving a bucket between compartments is supported, but it requires some funky permissions:
        //  - the user must be able to both create and update buckets in the source compartment
        //  - the user must be able to create buckets in the destination compartment
        if (bucketUpdate.getCompartmentId().isPresent() &&
                !bucketUpdate.getCompartmentId().get().equals(curBucket.getCompartment())) {
            WSRequestContext.get(context).getVisa().ifPresent(visa -> visa.setAllowReEntry(true));
            final String compartmentId = bucketUpdate.getCompartmentId().get();
            boolean sourceAuth = MetricScope.timeScope(childScope,
                    "authorize",
                    (Function<MetricScope, Boolean>) (innerScope) -> authorizer.authorize(
                            WSRequestContext.get(context),
                            authInfo,
                            curBucket.getNamespaceKey(),
                            curBucket.getBucketName(),
                            curBucket.getCompartment(),
                            publicAccessType,
                            CasperOperation.UPDATE_BUCKET,
                            curBucket.getKmsKeyId().orElse(null),
                            true,
                            curBucket.isTenancyDeleted(),
                            CasperPermission.BUCKET_CREATE,
                            CasperPermission.BUCKET_UPDATE).isPresent());
            Optional<AuthorizationResponse> desAuthResp = MetricScope.timeScope(childScope,
                    "authorize",
                    (Function<MetricScope, Optional<AuthorizationResponse>>) (innerScope) ->
                            authorizer.authorizeWithTags(
                                    WSRequestContext.get(context),
                                    authInfo,
                                    curBucket.getNamespaceKey(),
                                    curBucket.getBucketName(),
                                    compartmentId,
                                    publicAccessType,
                                    CasperOperation.CREATE_BUCKET,
                                    TaggingOperation.SET_CHANGE_TAGS,
                                    newTagSet,
                                    oldTagSet,
                                    new KmsKeyUpdateAuth(bucketUpdate.getKmsKeyId(),
                                            curBucket.getKmsKeyId().orElse(null), true),
                                    true,
                                    curBucket.isTenancyDeleted(),
                                    CasperPermission.BUCKET_CREATE));
            if (!sourceAuth) {
                throw new NoSuchBucketException(noSuchBucketMsg(bucketName, authInfo.getMainPrincipal().getTenantId()));
            } else {
                if (!desAuthResp.isPresent()) {
                    throw new NoSuchCompartmentIdException(
                            "Either the compartment with id '" + bucketUpdate.getCompartmentId() +
                                    "' does not exist or you are not authorized to access it");
                }
                if (!desAuthResp.get().areTagsAuthorized()) {
                    final String errorMessage = desAuthResp.get().getErrorMessage() == null ?
                            "Failed to authorize or validate tags." : desAuthResp.get().getErrorMessage();
                    throw new InvalidTagsException(errorMessage);
                }
                validatedTagSet = desAuthResp.get().getTagSet().get();
            }

            if (quotaEvaluationEnabled && !InternalBucketType.isInternalBucket(curBucket.getBucketName())) {
                ObjectDbStats bucketStats = getStatForBucket(context, curBucket);
                quotaEvaluator.evaluateMoveUsage(bucketUpdate.getNamespaceName(),
                        WSRequestContext.get(context).getResourceTenantOcid(),
                        curBucket.getCompartment(),
                        compartmentId,
                        bucketStats.getObjectStorageSize().longValue() + bucketStats.getPartSize().longValue());
            }
        } else {
            Optional<AuthorizationResponse> authResp = MetricScope.timeScope(childScope,
                    "authorize",
                    (Function<MetricScope, Optional<AuthorizationResponse>>) (innerScope) ->
                            authorizer.authorizeWithTags(
                                    WSRequestContext.get(context),
                                    authInfo,
                                    curBucket.getNamespaceKey(),
                                    curBucket.getBucketName(),
                                    curBucket.getCompartment(),
                                    publicAccessType,
                                    CasperOperation.UPDATE_BUCKET,
                                    TaggingOperation.SET_CHANGE_TAGS,
                                    newTagSet,
                                    oldTagSet,
                                    new KmsKeyUpdateAuth(bucketUpdate.getKmsKeyId(),
                                            curBucket.getKmsKeyId().orElse(null), true),
                                    true,
                                    curBucket.isTenancyDeleted(),
                                    CasperPermission.BUCKET_UPDATE));
            if (!authResp.isPresent()) {
                throw new NoSuchBucketException(noSuchBucketMsg(bucketName, bucketUpdate.getNamespaceName()));
            }

            if (!authResp.get().areTagsAuthorized()) {
                final String errorMessage = authResp.get().getErrorMessage() == null ?
                        "Failed to authorize or validate tags." : authResp.get().getErrorMessage();
                throw new InvalidTagsException(errorMessage);
            }

            validatedTagSet = authResp.get().getTagSet().get();
        }

        if (expectedETag != null && !expectedETag.equals(curBucket.getEtag())) {
            throw new UnexpectedEntityTagException("The current entity tag '" + curBucket.getEtag() +
                    "' does not match the expected entity tag '" + expectedETag + "'");
        }

        final String tenantOcid = authInfo.getMainPrincipal().getTenantId();
        //publicAccessType is optional parameter in bucket update. check if someone is trying to change bucket
        //access type to public.
        if (bucketUpdate.getPublicAccessType().isPresent() &&
                publicAccessType != BucketPublicAccessType.NoPublicAccess) {
            // other 2 options are public bucket type
            boolean isPublicBucketEnabled = limits.isPublicBucketEnabled(tenantOcid);
            if (!isPublicBucketEnabled) {
                LOG.debug("Tenant does not have permission in limits to create public bucket");
                throw new MissingPublicBucketLimitsException(
                        tenantMissingPublicBucketLimitsPermission(bucketName, curBucket.getNamespaceKey().getName()));
            }
        }

        String updateCurrentLifecyclePolicyEtag = bucketUpdate.getObjectLifecyclePolicyEtag().orElse(null);
        if (updateCurrentLifecyclePolicyEtag == null) {
            //not update objectLifecyclePolicyEtag, get the original value
            updateCurrentLifecyclePolicyEtag = curBucket.getObjectLifecyclePolicyEtag().orElse(null);
        } else if (updateCurrentLifecyclePolicyEtag.equals(CLEAR_LIFECYCLE_POLICY)) {
            //reset objectLifecyclePolicyEtag to null, indicating no lifecycle policy on this bucket
            updateCurrentLifecyclePolicyEtag = null;
        }

        final Map<String, String> metadata = MetadataUpdater.updateMetadata(bucketUpdate.isMissingMetadata(),
                curBucket.getMetadata(kms), bucketUpdate.getMetadata());
        Validator.validateMetadata(metadata, jsonSerializer);

        String compartment = bucketUpdate.getCompartmentId()
                .orElse(curBucket.getCompartment());
        BucketPublicAccessType accessType = bucketUpdate.getPublicAccessType()
                .orElse(curBucket.getPublicAccessType());
        ObjectLevelAuditMode auditMode = bucketUpdate.getObjectLevelAuditMode()
                .orElse(curBucket.getObjectLevelAuditMode());

        final WSRequestContext wsContext = WSRequestContext.get(context);
        if (bucketUpdate.getCompartmentId().isPresent()) {
            wsContext.addOldState("compartment", curBucket.getCompartment());
            wsContext.addNewState("compartment", bucketUpdate.getCompartmentId().get());
        }
        if (bucketUpdate.getPublicAccessType().isPresent()) {
            wsContext.addOldState("publicAccessType", curBucket.getPublicAccessType().getValue());
            wsContext.addNewState("publicAccessType", bucketUpdate.getPublicAccessType().get().getValue());
        }
        if (bucketUpdate.getObjectLevelAuditMode().isPresent()) {
            wsContext.addOldState("objectLevelAuditMode", curBucket.getObjectLevelAuditMode().getValue());
            wsContext.addNewState("objectLevelAuditMode", bucketUpdate.getObjectLevelAuditMode().get().getValue());
        }
        if (bucketUpdate.getDefinedTags() != null || bucketUpdate.getFreeformTags() != null) {
            wsContext.addOldState("tags", oldTagSet);
            wsContext.addNewState("tags", validatedTagSet);
        }
        if (bucketUpdate.getKmsKeyId() != null) {
            wsContext.addOldState("kmsKeyId", curBucket.getKmsKeyId().orElse(null));
            wsContext.addNewState("kmsKeyId", bucketUpdate.getKmsKeyId());
        }
        if (bucketUpdate.getObjectLifecyclePolicyEtag().isPresent()) {
            //if the etag is CLEAR_LIFECYCLE_POLICY it is actually to remove policy
            final String newEtag = bucketUpdate.getObjectLifecyclePolicyEtag().get();
            wsContext.addOldState("objectLifecyclePolicyETag", curBucket.getObjectLifecyclePolicyEtag().orElse(null));
            wsContext.addNewState("objectLifecyclePolicyETag", newEtag.equals(CLEAR_LIFECYCLE_POLICY) ? null : newEtag);
        }
        if (bucketUpdate.isObjectEventsEnabled() != null
                && (bucketUpdate.isObjectEventsEnabled() != curBucket.isObjectEventsEnabled())) {
            wsContext.addOldState("objectEventsEnabled", curBucket.isObjectEventsEnabled());
            wsContext.addNewState("objectEventsEnabled", bucketUpdate.isObjectEventsEnabled());
        }
        final Map<String, Object> options = bucketUpdate.isObjectEventsEnabled() != null ?
            OptionsUpdater.updateOptions(curBucket.getOptions(), bucketUpdate.getOptions()) : curBucket.getOptions();

        return updateBucket(context, authInfo, bucketUpdate.getNamespaceName(), compartment, bucketName,
                curBucket.getEtag(), validatedTagSet, accessType, auditMode, metadata, kmsKeyIdUpdate,
                curBucket.getKmsKeyId().orElse(null), updateCurrentLifecyclePolicyEtag, options, childScope);
    }

    /**
     * Update a bucket with purged list of deleted tags
     *
     * @param context      the common request context used for logging and metrics.
     * @param authInfo     information about the authenticated user making the request.
     * @param namespace    the namespace in which to find the bucket.
     * @param bucketName   the name of the bucket to fetch.
     * @return a CompletableFuture that will contain either the updated bucket or an exception. A partial list of
     * exceptions that can be returned includes: UnexpectedEntityTagException, InvalidBucketNameException and
     * InvalidMetadataException.
     */
    public Bucket purgeDeletedDefinedTags(RoutingContext context,
                                          AuthenticationInfo authInfo,
                                          String namespace,
                                          String bucketName) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);

        // Get current bucket info
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:purgeDeletedDefinedTags");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);
        final String opcRequestId = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        final WSTenantBucketInfo curBucket =
                tenantBackend.getBucketInfo(opcRequestId, childScope, Api.V2, namespace, bucketName)
                        .orElseThrow(() -> new NoSuchBucketException(noSuchBucketMsg(bucketName, namespace)));

        /*
        For purging deleted tags from a bucket, construct a tag-only authorization request by passing all the
        existing tags currently associated with the bucket. Get back a list of tags in response with the deleted
        tags removed and write the new tag set to the bucket.
        */
        Optional<AuthorizationResponse> authResp = MetricScope.timeScope(childScope,
                "authorize",
                (Function<MetricScope, Optional<AuthorizationResponse>>) (innerScope) ->
                        authorizer.authorizeWithTags(
                                WSRequestContext.get(context),
                                authInfo,
                                curBucket.getNamespaceKey(),
                                curBucket.getBucketName(),
                                curBucket.getCompartment(),
                                curBucket.getPublicAccessType(),
                                CasperOperation.PURGE_DELETED_TAGS,
                                TaggingOperation.SET_PURGE_TAGS,
                                curBucket.getTags(),
                                curBucket.getTags(),
                                new KmsKeyUpdateAuth(null, // Setting newKmsKey=null implies no update for the key
                                        curBucket.getKmsKeyId().orElse(null),
                                        true), // Setting this to true/false doens't have any affect in this case.
                                               // Since there is no key update, no authz will be performed anyway.
                                true,
                                curBucket.isTenancyDeleted(),
                                CasperPermission.BUCKET_PURGE_TAGS));

        if (!authResp.isPresent()) {
            throw new NoSuchBucketException(noSuchBucketMsg(bucketName, curBucket.getNamespaceKey().getName()));
        }


        if (!authResp.get().areTagsAuthorized()) {
            final String errorMessage = authResp.get().getErrorMessage() == null ?
                    "Failed to authorize or validate tags." : authResp.get().getErrorMessage();
            throw new InvalidTagsException(errorMessage);
        }

        // Retrieve the new tag set from auth response
        TagSet validatedTagSet = authResp.get().getTagSet().orElse(TagSet.builder().build());

        final String kmsKeyIdUpdate = curBucket.getKmsKeyId().orElse(null);
        String updateCurrentLifecyclePolicyEtag = curBucket.getObjectLifecyclePolicyEtag().orElse(null);

        // Update bucket with the new tag set
        return updateBucket(context, authInfo, namespace, curBucket.getCompartment(), bucketName, curBucket.getEtag(),
                validatedTagSet, curBucket.getPublicAccessType(),
                curBucket.getObjectLevelAuditMode(), curBucket.getMetadata(kms),
                kmsKeyIdUpdate, null, updateCurrentLifecyclePolicyEtag, curBucket.getOptions(), childScope);
    }

    /**
     * List all the buckets in the given namespace for which the user specified in
     * {@link AuthenticationInfo authenticationInfo} has authorization.
     *
     * @param context   the common request context used for logging and metrics.
     * @param authInfo  information about the authenticated user making the request.
     * @param namespace the namespace in which to list buckets.
     * @return A {@link PaginatedList} containing an unlimited number of {@link BucketSummary} objects such that
     * {@link PaginatedList#isTruncated()} returns {@code false}.
     */
    public PaginatedList<BucketSummary> listAllBucketsInNamespace(
            RoutingContext context, AuthenticationInfo authInfo, String namespace) {
        Validator.validateV2Namespace(namespace);
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope().child("backend:listBuckets");
        // Lazily populated authorization for each compartment
        final Map<String, Boolean> authorizedCompartmentIds = Maps.newHashMap();
        return fetchAllPages(
                cursor -> tenantBackend.listBucketsInNamespace(context, scope,
                        new NamespaceKey(Api.V2, namespace),
                        S3_LIST_BUCKETS_CHUNK,
                        false,
                        cursor), bucket -> authorizedCompartmentIds.computeIfAbsent(bucket.getCompartment(),
                        compartmentId -> MetricScope.timeScope(scope, "authorizer",
                                (Function<MetricScope, Boolean>) (innerScope) -> authorizer.authorize(
                                        WSRequestContext.get(context),
                                        authInfo,
                                        bucket.getNamespaceKey(),
                                        bucket.getBucketName(),
                                        compartmentId,
                                        null,
                                        CasperOperation.LIST_BUCKETS,
                                        null,
                                        true,
                                        false,
                                        CasperPermission.BUCKET_INSPECT).isPresent())
                )
        );
    }

    /**
     * List buckets in the given namespace without <b>any</b> check for authorization/authentication.
     *
     * @param context   the common request context used for logging and metrics.
     * @param namespace The namespace to search for buckets
     * @param pageSize  The maximum number of elements to return
     * @return A single page of bucket summaries
     */
    public PaginatedList<BucketSummary> listAllBucketsInNamespaceUnauthenticated(
            RoutingContext context,
            String namespace,
            int pageSize,
            String cursor,
            Set<BucketProperties> bucketProperties) {
        Validator.validateV2Namespace(namespace);
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope().child("backend:listBuckets");
        final boolean returnObjectLifecyclePolicyEtag = bucketProperties
                .contains(BucketProperties.OBJECT_LIFECYCLE_POLICY_ETAG);
        return BackendConversions.bucketScanResultsToPaginatedList(
                tenantBackend.listBucketsInNamespace(
                    context,
                    scope, new NamespaceKey(Api.V2, namespace), pageSize, false, cursor),
                returnObjectLifecyclePolicyEtag);
    }


    /**
     * List all namespaces that casper knows about for a particular API
     *
     * @param context  the common request context used for logging and metrics.
     * @param api      The api we should select namespaces for
     * @param pageSize The maximum number of elements to return
     * @param cursor   The starting namespace to start to list from
     * @return A {@link PaginatedList} containing up to pageSize entries of {@link NamespaceSummary}
     */
    public PaginatedList<NamespaceSummary> listAllNamespacesUnauthenticated(
            RoutingContext context, Api api, int pageSize, @Nullable NamespaceKey cursor) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope().child("backend:listNamespace");

        return BackendConversions.namespaceKeyResultsToPaginatedList(
                tenantBackend.listNamespaces(context, scope, api, pageSize, Optional.ofNullable(cursor))
                        .stream()
                        .map(info -> new NamespaceKey(api, info.getNamespaceName()))
                        .collect(Collectors.toList()));
    }

    /**
     * List all of the buckets in the given compartment and namespace.
     *
     * @param context       the common request context used for logging and metrics.
     * @param authInfo      information about the authenticated user making the request.
     * @param namespace     the namespace in which to list buckets.
     * @param compartmentId compartment of the namespace in which to list buckets.
     * @return A {@link PaginatedList} of an unlimited number of {@link BucketSummary} objects such
     * that {@link PaginatedList#isTruncated()} returns {@code false}.
     */
    public PaginatedList<BucketSummary> listAllBucketsInCompartment(
            RoutingContext context, AuthenticationInfo authInfo, String namespace, String compartmentId) {
        Validator.validateV2Namespace(namespace);
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        MetricScope scope = commonContext.getMetricScope().child("backend:listBuckets").
                annotate("compartmentId", compartmentId);
        if (!MetricScope.timeScope(
                scope,
                "authorizer",
                (Function<MetricScope, Boolean>) (innerScope) -> authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        null,
                        null,
                        compartmentId,
                        null, CasperOperation.LIST_BUCKETS,
                        null,
                        true,
                        false,
                        CasperPermission.BUCKET_INSPECT).isPresent())) {
            throw new NoSuchNamespaceException(noSuchNamespaceMsg());
        }

        return fetchAllPages(
                cursor -> tenantBackend.listBucketsInCompartment(context, scope,
                        new NamespaceKey(Api.V2, namespace),
                        compartmentId,
                        S3_LIST_BUCKETS_CHUNK,
                        false,
                        new Pair<>(cursor, null),
                        null,
                        null),
                b -> true);
    }

    /**
     * List buckets in a V1 scope (namespace).
     * Returns a paginated list of simply the bucket names.
     */
    @Deprecated
    public PaginatedList<String> listBucketsInV1(RoutingContext context,
                                                 String namespace, int pageSize, @Nullable String cursor) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        MetricScope scope = commonContext.getMetricScope().child("backend:listAllBucketsInV1").
                annotate("compartmentId", TenantDb.V1_COMPARTMENT_ID);
        if (!MetricScope.timeScope(scope, "authorizer", (Function<MetricScope, Boolean>) (innerScope) ->
                //We need to be super-user to avoid auth checks
                authorizer.authorize(
                        WSRequestContext.get(context),
                        AuthenticationInfo.V1_USER,
                        null,
                        null,
                        compartmentCacheConfiguration.getV1Compartment(),
                        null,
                        CasperOperation.LIST_BUCKETS,
                        null,
                        true,
                        false,
                        CasperPermission.BUCKET_INSPECT).isPresent())) {
            throw new NoSuchNamespaceException(noSuchNamespaceMsg());
        }

        ScanResults<WSTenantBucketSummary> results = tenantBackend.listBucketsInNamespace(context, scope,
                new NamespaceKey(Api.V1, namespace),
                pageSize,
                false,
                cursor);
        return new PaginatedList<>(
                results.getKeys().stream().map(WSTenantBucketSummary::getBucketName).collect(
                        Collectors.toList()),
                results.isTruncated());
    }

    // TODO: lin, This does filter out all internal buckets, but we need to do it more appropriate in BucketBackend.
    public static PaginatedList<BucketSummary> filterInternalBucket(PaginatedList<BucketSummary> bucketList) {
        return new PaginatedList<>(bucketList.getItems().stream()
                .filter(BucketBackend.INTERNAL_BUCKET_FILTER)
                .collect(Collectors.toList()), bucketList.isTruncated());
    }

    /**
     * Returns *all* pages inside one {@link PaginatedList} such that {@link PaginatedList#isTruncated()} returns false.
     */
    private PaginatedList<BucketSummary> fetchAllPages(
            Function<String, ScanResults<WSTenantBucketSummary>> chunkProvider,
            Predicate<WSTenantBucketSummary> bucketFilter) {
        return new PaginatedList<>(
                ListUtils.listEntities(chunkProvider.andThen(chunk ->
                        ListableResponse.create(chunk::getKeys, last -> last.getKey().getName())))
                        .filter(bucketFilter)
                        .map(bucket -> BackendConversions.wsBucketToBucketSummary(bucket, false, false))
                        .sorted(Comparator.comparing(BucketSummary::getBucketName))
                        ::iterator, false);
    }

    public PaginatedList<BucketSummary> listBucketsInCompartment(
            RoutingContext context,
            AuthenticationInfo authInfo,
            String namespace,
            String compartmentId,
            int limit,
            @Nullable String cursor) {
        return listBucketsInCompartment(context, authInfo, namespace, compartmentId, limit, new Pair<>(cursor, null),
                Collections.emptySet(), null, null);
    }

    /**
     * List a page of the buckets in the given compartment and namespace.
     *
     * @param context       the common request context used for logging and metrics.
     * @param authInfo      information about the authenticated user making the request.
     * @param namespace     the namespace in which to list buckets.
     * @param compartmentId compartment of the namespace in which to list buckets.
     * @param limit         the maximum number of buckets to list, if the client passed one in.
     * @param cursor        the bucket name and creationTime after which to start listing, both of which can be null.
     * @return a CompletableFuture containing either a paginated list of (no more than limit) buckets or an exception.
     * partial list of exceptions that can be returned includes: InvalidBucketNameException.
     */
    public PaginatedList<BucketSummary> listBucketsInCompartment(
            RoutingContext context,
            AuthenticationInfo authInfo,
            String namespace,
            String compartmentId,
            int limit,
            Pair<String, Instant> cursor,
            Set<BucketProperties> bucketProperties,
            @Nullable BucketSortBy sortBy,
            @Nullable BucketSortOrder sortOrder) {
        Validator.validateV2Namespace(namespace);
        if (cursor.getFirst() != null) {
            Validator.validateBucket(cursor.getFirst());
        }

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:listBuckets")
                .annotate("limit", limit)
                .annotate("cursor", cursor);
        final boolean returnTags = bucketProperties.contains(BucketProperties.TAGS);
        HttpPathHelpers.logPathParameters(rootScope, namespace, null, null);

        if (!MetricScope.timeScope(childScope, "authorizer",
                (Function<MetricScope, Boolean>) (innerScope) ->
                        authorizer.authorize(
                                WSRequestContext.get(context),
                                authInfo,
                                null,
                                null,
                                compartmentId,
                                null,
                                CasperOperation.LIST_BUCKETS,
                                null,
                                true,
                                false,
                                CasperPermission.BUCKET_INSPECT)
                                .isPresent())) {
            throw new NoSuchNamespaceException(noSuchNamespaceMsg());
        }

        ScanResults<WSTenantBucketSummary> scanResults = tenantBackend.listBucketsInCompartment(context, childScope,
                new NamespaceKey(Api.V2, namespace),
                compartmentId,
                limit + InternalBucketType.values().length,
                returnTags,
                cursor,
                sortBy,
                sortOrder);
        List<BucketSummary> listBucketCache = scanResults.getKeys().stream()
                .map(bucket -> BackendConversions.wsBucketToBucketSummary(bucket, false, returnTags))
                .filter(INTERNAL_BUCKET_FILTER)
                .collect(Collectors.toList());

        return listBucketCache.size() > limit ?
                new PaginatedList<>(listBucketCache.subList(0, limit), true) :
                new PaginatedList<>(listBucketCache, scanResults.isTruncated());
    }

    /**
     * This method is only used by Object operations, NOT for bucket update/delete/get
     */
    public WSTenantBucketInfo getBucketMetadataWithCache(RoutingContext context,
                                                         MetricScope scope,
                                                         String namespace,
                                                         String bucketName,
                                                         Api api) {
        final BucketKey bucketKey = new BucketKey(namespace, bucketName, api);
        try {
            // getBucketMetadata method only throws runtime exception, we cast the cause of ExecutionException and
            // UncheckedExecutionException to runtime exception and pop it up to the caller.
            final Optional<WSTenantBucketInfo> wsTenantBucketInfo = cachedBuckets.get(bucketKey);

            // The reason we want to return an Optional<WSTenantBucketInfo> is that if there was already any entry of
            // a bucket after a bucket was deleted. Why this can happen? This will happen if there was no Object relate
            // operation hit this method after the delete bucket operation set the bucket uncacheable, and it will
            // happen for the first Object related operation that hit this method. To solve this problem we let the
            // cacheloader load the entry by calling getBucketMetadataWithNullValue which return an Optional.emtpy() for
            // deleted bucket and we throw the NoSuchBucketException here.
            if (!wsTenantBucketInfo.isPresent()) {
                cachedBuckets.invalidate(bucketKey);
                throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketName, namespace));
            }
            // if the bucket was set to not cacheable we need to invalidate the cache to prevent it having old value so
            // long.
            if (!wsTenantBucketInfo.get().isCacheable()) {
                cachedBuckets.invalidate(bucketKey);
            }

            return wsTenantBucketInfo.get();
        } catch (ExecutionException | UncheckedExecutionException ex) {
            // we configure expireAfterWrite = 10 minutes refreshAfterWrite = 1 seconds
            // 1. if the value is not in cache, the cache will load from tenantDb, if tenantDb throw exception, we
            //    propagate it. If not, load successfully and return it.
            // 2. if value is in cache and not expire and less than 1s, just return, no need to load
            // 3. if value is in cache and older than 1s, will load if load failed, it will still return the value and
            //    swallow the exception
            // 4. expireAfterWrite expires will reload and if reload failed, it will just throw exception.

            // tenantBackend.getBucketInfo currently never throw checked exception, here we re-throw the inner cause
            // to keep the current behavior.
            Throwables.throwIfUnchecked(ex.getCause());

            // If for some reason, it is checked exception, we return 500 to user.
            throw new InternalServerErrorException(ex.getCause());
        }
    }

    /**
     * This method is only used by Object operations, NOT for bucket update/delete/get
     */
    public WSTenantBucketInfo getBucketMetadataWithCache(String namespace,
                                                         String bucketName,
                                                         Api api) {
        final BucketKey bucketKey = new BucketKey(namespace, bucketName, api);
        try {
            // getBucketMetadata method only throws runtime exception, we cast the cause of ExecutionException and
            // UncheckedExecutionException to runtime exception and pop it up to the caller.
            final Optional<WSTenantBucketInfo> wsTenantBucketInfo = cachedBuckets.get(bucketKey);

            // The reason we want to return an Optional<WSTenantBucketInfo> is that if there was already any entry of
            // a bucket after a bucket was deleted. Why this can happen? This will happen if there was no Object relate
            // operation hit this method after the delete bucket operation set the bucket uncacheable, and it will
            // happen for the first Object related operation that hit this method. To solve this problem we let the
            // cacheloader load the entry by calling getBucketMetadataWithNullValue which return an Optional.emtpy() for
            // deleted bucket and we throw the NoSuchBucketException here.
            if (!wsTenantBucketInfo.isPresent()) {
                cachedBuckets.invalidate(bucketKey);
                throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketName, namespace));
            }
            // if the bucket was set to not cacheable we need to invalidate the cache to prevent it having old value so
            // long.
            if (!wsTenantBucketInfo.get().isCacheable()) {
                cachedBuckets.invalidate(bucketKey);
            }

            return wsTenantBucketInfo.get();
        } catch (ExecutionException | UncheckedExecutionException ex) {
            // we configure expireAfterWrite = 10 minutes refreshAfterWrite = 1 seconds
            // 1. if the value is not in cache, the cache will load from tenantDb, if tenantDb throw exception, we
            //    propagate it. If not, load successfully and return it.
            // 2. if value is in cache and not expire and less than 1s, just return, no need to load
            // 3. if value is in cache and older than 1s, will load if load failed, it will still return the value and
            //    swallow the exception
            // 4. expireAfterWrite expires will reload and if reload failed, it will just throw exception.

            // tenantBackend.getBucketInfo currently never throw checked exception, here we re-throw the inner cause
            // to keep the current behavior.
            Throwables.throwIfUnchecked(ex.getCause());

            // If for some reason, it is checked exception, we return 500 to user.
            throw new InternalServerErrorException(ex.getCause());
        }
    }

    /**
     * Gets bucket metadata. Throws exception if bucket doesn't exist.
     * Call this only on Vertx worker thread.
     */
    public WSTenantBucketInfo getBucketMetadata(
        RoutingContext context, MetricScope scope, String namespace, String bucketName) {
        final String opcRequestId = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        return getBucketMetadata(opcRequestId, scope, namespace, bucketName, Api.V2);
    }

    /**
     * Gets bucket metadata. Throws exception if bucket doesn't exist.
     * Call this only on Vertx worker thread.
     * If the API is {@link Api#V1} then the compartment is swapped from the metadata default in the table
     * to one defined in {@link StorageMeteringConfiguration}
     */
    private WSTenantBucketInfo getBucketMetadata(
        String opcRequestId, MetricScope scope, String namespace, String bucketName, Api api) {
        return tenantBackend.getBucketInfo(opcRequestId, scope, api, namespace, bucketName)
            .orElseThrow(() -> new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketName, namespace)));
    }

    private Optional<WSTenantBucketInfo> getBucketMetadataWithNullValue(
            String opcRequestId, MetricScope scope, String namespace, String bucketName, Api api) {
        return tenantBackend.getBucketInfo(opcRequestId, scope, api, namespace, bucketName);
    }

    /**
     * Gets namespace metadata.
     *
     * @param context
     * @param authInfo      information about the authenticated user making the request.
     * @param namespace     the namespace in which to list buckets.
     * @param compartmentId
     * @return namespaceMetadata for specific namespace
     */
    public NamespaceMetadata getNamespaceMetadata(MetricScope scope,
                                                  RoutingContext context,
                                                  AuthenticationInfo authInfo,
                                                  String namespace,
                                                  String compartmentId,
                                                  Api api) {
        Validator.validateV2Namespace(namespace);
        authorizeOperationForPermissionsInternal(scope,
                context,
                authInfo,
                compartmentId,
                CasperOperation.GET_NAMESPACE_METADATA,
                true,
                noSuchNamespaceMsg(),
                CasperPermission.OBJECTSTORAGE_NAMESPACE_READ);
        MdsNamespace mdsNamespace =
                tenantBackend.getOrCreateNamespace(context, scope, new NamespaceKey(api, namespace), compartmentId);
        return BackendConversions.namepsaceInfoToNamespaceMetadata(mdsNamespace);
    }

    /**
     * Update default s3/swift compartmentID, the new compartmentID must not be null or empty string
     */
    public void updateNamespaceMetadata(MetricScope scope,
                                        RoutingContext context,
                                        AuthenticationInfo authInfo,
                                        String namespace,
                                        String compartmentId,
                                        NamespaceMetadata metadata) {
        Validator.validateV2Namespace(namespace);
        authorizeOperationForPermissionsInternal(scope, context, authInfo, compartmentId,
                CasperOperation.UPDATE_NAMESPACE_METADATA, true,
                noSuchNamespaceMsg(), CasperPermission.OBJECTSTORAGE_NAMESPACE_UPDATE);
        UpdateNamespaceRequest.Builder builder = UpdateNamespaceRequest.newBuilder();
        if (metadata.getDefaultS3CompartmentId() != null) {
            authorizeOperationForPermissionsInternal(scope, context, authInfo, metadata.getDefaultS3CompartmentId(),
                    CasperOperation.UPDATE_NAMESPACE_METADATA, true, "The defaultS3compartment either " +
                            "does not exist, or you are not authorized to create buckets in it",
                    CasperPermission.BUCKET_CREATE);
            builder.setDefaultS3Compartment(metadata.getDefaultS3CompartmentId());
            WSRequestContext.get(context)
                .addNewState("defaultS3CompartmentId", metadata.getDefaultS3CompartmentId());
        }
        if (metadata.getDefaultSwiftCompartmentId() != null) {
            authorizeOperationForPermissionsInternal(scope, context, authInfo, metadata.getDefaultSwiftCompartmentId(),
                    CasperOperation.UPDATE_NAMESPACE_METADATA, true, "The defaultSwiftCompartment either " +
                            "does not exist, or you are not authorized to create buckets in it",
                    CasperPermission.BUCKET_CREATE);
            builder.setDefaultSwiftCompartment(metadata.getDefaultSwiftCompartmentId());
            WSRequestContext.get(context)
                .addNewState("defaultSwiftCompartmentId", metadata.getDefaultSwiftCompartmentId());
        }
        tenantBackend.updateNamespace(context, scope,
                builder.setNamespaceName(namespace).setApi(MdsTransformer.toMdsNamespaceApi(Api.V2)).build(),
                authInfo);
    }

    private void authorizeOperationForPermissionsInternal(MetricScope scope,
                                                          RoutingContext context,
                                                          AuthenticationInfo authInfo,
                                                          String compartmentId,
                                                          CasperOperation operation,
                                                          boolean allPermissionRequired,
                                                          String errorMsg,
                                                          CasperPermission... permissions) {
        VertxUtil.assertOnVertxWorkerThread();
        WSRequestContext.get(context).getVisa().ifPresent(visa -> visa.setAllowReEntry(true));
        MetricScope.timeScope(scope,
                String.format("authorizeOperationForPermissions:%s:%s",
                        operation, Arrays.asList(permissions)),
                (innerScope) -> {
                    authorizer.authorize(
                            WSRequestContext.get(context),
                            authInfo,
                            null,
                            null,
                            compartmentId,
                            null,
                            operation,
                            null,
                            allPermissionRequired,
                            false,
                            permissions).orElseThrow(() -> new NoSuchCompartmentIdException(errorMsg));
                });
    }

    /**
     * Delete a bucket in the given namespace.
     * <p>
     * NOTE: This tries to make sure the bucket is empty, but a concurrent insert can slip through.
     * </p>
     *
     * @param context      the common request context used for logging and metrics.
     * @param authInfo     information about the authenticated user making the request.
     * @param namespace    the namespace in which to delete the bucket.
     * @param bucketName   the name of the bucket to delete.
     * @param expectedETag the expected entity tag or null.
     */
    public void deleteBucket(RoutingContext context,
                             AuthenticationInfo authInfo,
                             String namespace,
                             String bucketName,
                             String expectedETag) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:deleteBucket");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);

        final WSTenantBucketInfo bucketInfo = getBucketMetadata(context, childScope, namespace, bucketName);
        if (!MetricScope.timeScope(childScope,
                "authorizer", (Function<MetricScope, Boolean>) (innerScope) -> authorizer.authorizeWithTags(
                        WSRequestContext.get(context),
                        authInfo,
                        bucketInfo.getNamespaceKey(),
                        bucketName,
                        bucketInfo.getCompartment(),
                         bucketInfo.getPublicAccessType(),
                        CasperOperation.DELETE_BUCKET,
                        TaggingOperation.SET_EXISTING_TAGS,
                        null,
                        bucketInfo.getTags(),
                        new KmsKeyUpdateAuth(null, // Setting newKmsKey=null implies no update for the key
                                bucketInfo.getKmsKeyId().orElse(null),
                                true), // Setting this to true/false doens't have any affect in this case.
                        // Since there is no key update, no authz will be performed anyway.
                        true,
                        bucketInfo.isTenancyDeleted(),
                        CasperPermission.BUCKET_DELETE).isPresent())) {
            throw new NoSuchBucketException(noSuchBucketMsg(bucketName, namespace));
        }

        ReplicationEnforcer.throwIfReadOnly(context, bucketInfo);
        ReplicationEnforcer.throwIfReplicationEnabled(bucketName, bucketInfo);

        if (Backend.listMultipartUploads(
                objectClient, grpcObjectDeadlineSeconds, bucketInfo,
                commonContext, null, false,
                1, null, null,
                false).getUploadsCount() != 0) {
            throw new BucketNotEmptyException("Bucket named '" + bucketName + "' has pending multipart uploads. " +
                "Stop all multipart uploads first.");
        }

        if (!Backend.listObjectsFromPrefix(context, objectClient, grpcListObjectDeadlineSeconds,
                bucketInfo, 1, null, listObjectsMaxMessageBytes).isEmpty()) {
            throw new BucketNotEmptyException("Bucket named '" + bucketName + "' is not empty. " +
                    "Delete all objects first.");
        }

        // block bucket deletion if PARs for the bucket still exist
        if (bucketHasPars(context, childScope, namespace, bucketInfo.getImmutableResourceId())) {
            throw new ParStillExistsException("Active Preauthenticated Requests still exist for bucket '" +
                    bucketName + "'. Delete them first.");
        }
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.addOldState("bucketName", bucketInfo.getBucketName());
        wsRequestContext.setEtag(bucketInfo.getEtag());
        wsRequestContext.setBucketPublicAccessType(bucketInfo.getPublicAccessType());
        WSRequestContext.get(context).addOldState("bucketName", bucketInfo.getBucketName());

        tenantBackend.setBucketUncacheable(context, childScope, bucketInfo.getKey(),
                bucketCacheConfiguration.getCacheConfiguration().getRefreshAfterWrite().toMillis() * 10);
        try {
            Thread.sleep(bucketCacheConfiguration.getCacheConfiguration().getRefreshAfterWrite().toMillis());
        } catch (InterruptedException ex) {
            // do nothing
        }
        tenantBackend.deleteBucket(context, childScope, bucketInfo, expectedETag);
        cachedBuckets.invalidate(new BucketKey(namespace, bucketName, Api.V2));
    }

    public static String noSuchNamespaceMsg() {
        return "You do not have authorization to perform this request, or the requested resource could not be found.";
    }

    public static String noSuchBucketMsg(String bucketName, String namespaceName) {
        return "Either the bucket named '" + bucketName + "' does not exist in the namespace '" + namespaceName +
                "' or you are not authorized to access it";
    }

    public static String noSuchBucketMsg() {
        return "Either the bucket does not exist in or you are not authorized to access it";
    }

    private boolean bucketHasPars(
        RoutingContext context,
        MetricScope scope,
        String namespace,
        String bucketImmutableResourceId) {
        final String opcRequestId = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        Optional<WSTenantBucketInfo> parBucket = this.tenantBackend.getBucketInfo(opcRequestId, scope, Api.V2,
                namespace, ParUtil.toParBucketName(namespace));

        if (parBucket.isPresent()) {
            String prefix = Joiner.on(ParUtil.PAR_ID_SEPARATOR_BACKEND).join(bucketImmutableResourceId, "");
            return !Backend.listObjectsFromPrefix(context,
                    objectClient,
                    grpcListObjectDeadlineSeconds,
                    parBucket.get(),
                    1,
                    prefix,
                    listObjectsMaxMessageBytes).isEmpty();
        }
        return false;
    }

    /**
     * Get Bucket Location
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param namespace  the namespace in which this bucket may exist.
     * @param bucketName the name of the bucket.
     * @return a location
     */
    public LocationConstraint getBucketLocation(RoutingContext context,
                                                AuthenticationInfo authInfo,
                                                String namespace,
                                                String bucketName) {
        authorizeGetBucketSubResources(context, authInfo, namespace, bucketName);
        String region = ConfigRegion.fromSystemProperty().getFullName();
        return new LocationConstraint(region);
    }

    /**
     * Get Bucket Request Payment Configuration
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param namespace  the namespace in which this bucket may exist.
     * @param bucketName the name of the bucket.
     * @return a bucket request payment configuration
     */
    public BucketRequestPayment getBucketRequestPayment(RoutingContext context,
                                                        AuthenticationInfo authInfo,
                                                        String namespace,
                                                        String bucketName) {
        authorizeGetBucketSubResources(context, authInfo, namespace, bucketName);
        return new BucketRequestPayment();
    }

    /**
     * Get Bucket Logging status
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param namespace  the namespace in which this bucket may exist..
     * @param bucketName the name of the bucket.
     * @return a bucket logging status
     */
    public BucketLoggingStatus getBucketLoggingStatus(RoutingContext context,
                                                      AuthenticationInfo authInfo,
                                                      String namespace,
                                                      String bucketName) {
        authorizeGetBucketSubResources(context, authInfo, namespace, bucketName);
        return new BucketLoggingStatus();
    }

    /**
     * Get Bucket Notification Configuration
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param namespace  the namespace in which this bucket may exist.
     * @param bucketName the name of the bucket.
     * @return a bucket notification configuration
     */
    public BucketNotificationConfiguration getBucketNotificationConfiguration(RoutingContext context,
                                                                              AuthenticationInfo authInfo,
                                                                              String namespace,
                                                                              String bucketName) {
        authorizeGetBucketSubResources(context, authInfo, namespace, bucketName);
        return new BucketNotificationConfiguration();
    }

    /**
     * Get Bucket Acceleration Configuration
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param namespace  the namespace in which this bucket may exist.
     * @param bucketName the name of the bucket.
     * @return a bucket acceleration configuration
     */
    public BucketAccelerateConfiguration getBucketAccelerateConfiguration(RoutingContext context,
                                                                          AuthenticationInfo authInfo,
                                                                          String namespace,
                                                                          String bucketName) {
        authorizeGetBucketSubResources(context, authInfo, namespace, bucketName);
        return new BucketAccelerateConfiguration();
    }

    /**
     * Get Bucket CORS Configuration
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param namespace  the namespace in which this bucket may exist.
     * @param bucketName the name of the bucket.
     */
    public void getBucketCORSConfiguration(RoutingContext context,
                                           AuthenticationInfo authInfo,
                                           String namespace,
                                           String bucketName) {
        authorizeGetBucketSubResources(context, authInfo, namespace, bucketName);
        throw new NoSuchCORSException("The CORS configuration does not exist");
    }

    /**
     * Get Bucket Policy Configuration
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param namespace  the namespace in which this bucket may exist.
     * @param bucketName the name of the bucket.
     */
    public void getBucketPolicyConfiguration(RoutingContext context,
                                             AuthenticationInfo authInfo,
                                             String namespace,
                                             String bucketName) {
        authorizeGetBucketSubResources(context, authInfo, namespace, bucketName);
        throw new NoSuchPolicyException("The bucket policy does not exist");
    }

    /**
     * Get Bucket Replication Configuration
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param namespace  the namespace in which this bucket may exist.
     * @param bucketName the name of the bucket.
     */
    public void getBucketReplicationConfiguration(RoutingContext context,
                                                  AuthenticationInfo authInfo,
                                                  String namespace,
                                                  String bucketName) {
        authorizeGetBucketSubResources(context, authInfo, namespace, bucketName);
        throw new NoSuchReplicationException("The replication configuration does not exist");
    }

    /**
     * Get Bucket Website Configuration
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param namespace  the namespace in which this bucket may exist.
     * @param bucketName the name of the bucket.
     */
    public void getBucketWebsiteConfiguration(RoutingContext context,
                                              AuthenticationInfo authInfo,
                                              String namespace,
                                              String bucketName) {
        authorizeGetBucketSubResources(context, authInfo, namespace, bucketName);
        throw new NoSuchWebsiteException("The specified bucket does not have a website configuration");
    }

    /**
     * Get Bucket LifeCycle Configuration
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param namespace  the namespace in which this bucket may exist.
     * @param bucketName the name of the bucket.
     */
    public void getBucketLifecycleConfiguration(RoutingContext context,
                                                AuthenticationInfo authInfo,
                                                String namespace,
                                                String bucketName) {
        authorizeGetBucketSubResources(context, authInfo, namespace, bucketName);
        throw new NoSuchLifecycleException("The lifecycle configuration does not exist");
    }

    private void authorizeGetBucketSubResources(RoutingContext context,
                                                AuthenticationInfo authInfo,
                                                String namespace,
                                                String bucketName) {
        Validator.validateBucket(bucketName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:getBucket");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);
        final WSTenantBucketInfo bucket = getBucketMetadata(context, childScope, namespace, bucketName);
        if (!MetricScope.timeScope(childScope,
                "authorizer",
                (Function<MetricScope, Boolean>) (innerScope) -> authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        bucket.getNamespaceKey(),
                        bucketName,
                        bucket.getCompartment(),
                        bucket.getPublicAccessType(),
                        CasperOperation.GET_BUCKET,
                        null,
                        true,
                        bucket.isTenancyDeleted(),
                        CasperPermission.BUCKET_INSPECT).isPresent())) {
            throw new NoSuchBucketException(noSuchBucketMsg(bucketName, namespace));
        }
    }

    public void updateBucketTags(RoutingContext context,
                                 AuthenticationInfo authInfo,
                                 String ns,
                                 String bucketName,
                                 Map<String, String> freeformTags,
                                 Map<String, Map<String, Object>> definedTags) {
        Validator.validateBucket(bucketName);
        Validator.validateTags(freeformTags, (Map) definedTags, jsonSerializer);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:updateBucketTags");
        HttpPathHelpers.logPathParameters(rootScope, ns, bucketName, null);
        final String opcRequestId = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        Optional<WSTenantBucketInfo> tenantBucketInfo =
            tenantBackend.getBucketInfo(opcRequestId, childScope, Api.V2, ns, bucketName)
                .filter(bucket -> MetricScope.timeScope(childScope,
                    "authorizer",
                    (Function<MetricScope, Boolean>) (innerScope) -> authorizer.authorize(WSRequestContext.get(context),
                        authInfo,
                        new NamespaceKey(Api.V2, ns),
                        bucketName,
                        bucket.getCompartment(),
                        null,
                        CasperOperation.UPDATE_BUCKET,
                        bucket.getKmsKeyId().orElse(null),
                        true,
                         bucket.isTenancyDeleted(),
                        CasperPermission.BUCKET_UPDATE).isPresent()));

        if (!tenantBucketInfo.isPresent()) {
            throw new NoSuchBucketException(
                    noSuchBucketMsg(bucketName, ns));
        }

        // prepare BucketUpdate data to call updateBucketPartially to update tags.
        // 1. set isMissingMetadata to true to let updateBucketPartially use the metadata in current DB.
        // 2. set compartmentId to null, so that updateBucketPartially will not do compartmentId change, and will use
        //    the compartmentId from tenantDb
        final BucketUpdate bucketUpdate = BucketUpdate.builder()
                .namespace(ns)
                .bucketName(bucketName)
                .isMissingMetadata(true)
                .freeformTags(freeformTags)
                .definedTags(definedTags)
                .build();
        updateBucketPartially(context, authInfo, bucketUpdate, null);
    }

    /**
     * Helper method to delete a bucket's lifecycle policy ETag,
     *
     * Includes logic to check conditional headers against the previous lifecycle policy's ETag.
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param namespace  the namespace in which this bucket may exist.
     * @param bucketName the name of the bucket.
     * @return the newly updated bucket.
     */
    public Bucket deleteBucketCurrentLifecyclePolicyEtag(RoutingContext context,
                                                         AuthenticationInfo authInfo,
                                                         String namespace,
                                                         String bucketName) {
        Validator.validateBucket(bucketName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:deleteBucketCurrentLifecyclePolicyEtag");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);
        final String opcRequestId = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        final WSTenantBucketInfo bucketInfo = getBucketMetadata(opcRequestId, childScope, namespace, bucketName,
                Api.V2);

        // Validate ifMatch/ifNoneMatch here, since this is the right place where we get the previous
        // eTag before updating it.
        bucketInfo.getObjectLifecyclePolicyEtag().ifPresent(eTag ->
                HttpMatchHelpers.checkConditionalHeaders(context.request(), eTag));

        final BucketUpdate bucketUpdate = BucketUpdate.builder()
                .namespace(namespace)
                .bucketName(bucketName)
                .isMissingMetadata(true)
                .objectLifecyclePolicyEtag(CLEAR_LIFECYCLE_POLICY)
                .build();
        return updateBucketPartially(context, authInfo, bucketUpdate, null);
    }

    /**
     * Helper method to overwrite a bucket's lifecycle policy ETag.
     *
     * This method makes two authorization calls.  First, we need to verify that the user is allowed to write the
     * policy, and then we need to verify that Casper is authorized to execute their policy.  User authorization must
     * be checked first or an unauthorized user would be able to discover whether or not the target compartment has
     * authorized OLM.
     *
     * @param context    the common request context used for logging and metrics.
     * @param authInfo   information about the authenticated user making the request.
     * @param namespace  the namespace in which this bucket may exist.
     * @param bucketName the name of the bucket.
     * @return the newly updated bucket.
     */
    public Bucket overwriteBucketCurrentLifecyclePolicyEtag(RoutingContext context,
                                                            AuthenticationInfo authInfo,
                                                            String namespace,
                                                            String bucketName,
                                                            PutObjectLifecyclePolicyDetails policy,
                                                            String objectLifecyclePolicyEtag) {
        Validator.validateBucket(bucketName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:overwriteBucketCurrentLifecyclePolicyEtag");
        final String opcRequestId = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        final WSTenantBucketInfo bucketInfo = getBucketMetadata(opcRequestId, childScope, namespace, bucketName,
                Api.V2);
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);

        checkPutLifecyclePolicyUserAuthz(wsRequestContext, authInfo, bucketInfo);
        checkPutLifecyclePolicyServiceAuthz(wsRequestContext, bucketInfo, policy);

        // Validate ifMatch/ifNoneMatch here, since this is the right place where we get the previous
        // eTag before updating it.
        bucketInfo.getObjectLifecyclePolicyEtag().ifPresent(eTag ->
                HttpMatchHelpers.checkConditionalHeaders(context.request(), eTag));

        final BucketUpdate bucketUpdate = BucketUpdate.builder()
                .namespace(namespace)
                .bucketName(bucketName)
                .isMissingMetadata(true)
                .objectLifecyclePolicyEtag(objectLifecyclePolicyEtag)
                .build();
        return updateBucketPartially(context, authInfo, bucketUpdate, null);
    }

    /**
     * Verify that the user is authorized to create a lifecycle policy.
     *
     * Since lifecycle policies can be used to archive and delete objects, the user will need to have not only
     * BUCKET_UPDATE, but also OBJECT_CREATE and OBJECT_DELETE.  OBJECT_CREATE was chosen to represent permissions
     * to archive objects because it's the strongest object-level permission in terms of Identity metaverbs
     * (conversation with Rachna Thusoo, summer 2018).
     */
    private void checkPutLifecyclePolicyUserAuthz(WSRequestContext wsRequestContext,
                                                  AuthenticationInfo authInfo,
                                                  WSTenantBucketInfo bucketInfo) {
        // Note:  updateBucketPartially will make a separate authZ check on just BUCKET_UPDATE.  We really need to
        // refactor the authorization layer.
        boolean userAuthorized = authorizer.authorize(
                wsRequestContext,
                authInfo,
                bucketInfo.getNamespaceKey(),
                bucketInfo.getBucketName(),
                bucketInfo.getCompartment(),
                bucketInfo.getPublicAccessType(),
                CasperOperation.PUT_OBJECT_LIFECYCLE_POLICY,
                bucketInfo.getKmsKeyId().orElse(null),
                true,
                bucketInfo.isTenancyDeleted(),
                CasperPermission.BUCKET_UPDATE,
                CasperPermission.OBJECT_CREATE,
                CasperPermission.OBJECT_DELETE)
                .isPresent();
        if (!userAuthorized) {
            LOG.debug("Failed to authorize {}/{} in compartment {} to put a lifecycle policy",
                    bucketInfo.getNamespaceKey().getName(),
                    bucketInfo.getBucketName(),
                    bucketInfo.getCompartment());
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(
                    bucketInfo.getBucketName(),
                    bucketInfo.getNamespaceKey().getName()));
        }
    }

    /**
     * Verify that Casper is authorized to execute the given policy in the given bucket.
     *
     * Using OLM requires explicit authorization from customers; without it, the LE will not be allowed to even read
     * their lifecycle policies.  This extra step trips up a lot of customers though, so this method executes a
     * pre-flight check on the LE's permissions at the time the customer writes the policy.  If they haven't
     * authorized the LE correctly, their PutObjectLifecyclePolicy request will return a friendly 400 error message.
     */
    private void checkPutLifecyclePolicyServiceAuthz(WSRequestContext wsRequestContext,
                                                     WSTenantBucketInfo bucketInfo,
                                                     PutObjectLifecyclePolicyDetails policy) {
        // We begin by constructing the necessary set of permissions based on the lifecycle policy's rules:
        final Set<CasperPermission> permissionsRequired = EnumSet.of(
                CasperPermission.BUCKET_INSPECT,
                CasperPermission.BUCKET_READ,
                CasperPermission.OBJECT_INSPECT);
        for (ObjectLifecycleRule rule : policy.getItems()) {
            if (rule.getAction().equalsIgnoreCase(LifecycleEngineConstants.RULE_ACTION_ARCHIVE)) {
                permissionsRequired.add(CasperPermission.OBJECT_CREATE);
            } else if (rule.getAction().equalsIgnoreCase(LifecycleEngineConstants.RULE_ACTION_DELETE)) {
                permissionsRequired.add(CasperPermission.OBJECT_DELETE);
            }
        }

        // Then we construct an Identity Principal to represent the local service principal, and create one of our own
        // AuthenticationInfo's from it.
        // Warning:  this is fragile.  The PrincipalImpl constructor used here isn't meant to be public, but there's
        // no better public API available.  You're supposed to get Principal objects by authenticating a request,
        // but we don't have a request from the service principal to authenticate.
        // After speaking with Helali from Identity (November, 2018), this seems like the best available approach.
        final String certTenantId = servicePrincipalConfiguration.getTenantId();
        final String certCommonName = servicePrincipalConfiguration.getCertCommonName();
        final Principal servicePrincipal = new PrincipalImpl(certTenantId, certCommonName);
        final Set<Claim> claimsToAdd = Sets.newHashSet(
                new Claim(ClaimType.PRINCIPAL_TYPE.value(), PrincipalType.SERVICE.value(), Constants.TOKEN_ISSUER),
                new Claim(ClaimType.SERVICE_NAME.value(), certCommonName, Constants.TOKEN_ISSUER)
        );
        servicePrincipal.getClaims().addAll(claimsToAdd);
        final AuthenticationInfo servicePrincipalAuthInfo = AuthenticationInfo.fromPrincipal(servicePrincipal);

        boolean servicePrincipalAuthorized = authorizer.authorize(
                wsRequestContext,
                servicePrincipalAuthInfo,
                bucketInfo.getNamespaceKey(),
                bucketInfo.getBucketName(),
                bucketInfo.getCompartment(),
                bucketInfo.getPublicAccessType(),
                CasperOperation.PUT_OBJECT_LIFECYCLE_POLICY,
                bucketInfo.getKmsKeyId().orElse(null),
                true,
                bucketInfo.isTenancyDeleted(),
                permissionsRequired.toArray(new CasperPermission[permissionsRequired.size()]))
                .isPresent();

        if (!servicePrincipalAuthorized) {
            LOG.debug("Service principal was not authorized to execute lifecycle policy for {}/{}",
                    bucketInfo.getNamespaceKey().getName(),
                    bucketInfo.getBucketName());
            // This is a 400 Bad Request, not a typical authorization error.  The user was authorized, but _we_ weren't.
            throw new InsufficientServicePermissionsException("Permissions granted to the object storage service in " +
                    "this region are insufficient to execute this policy.");
        }
    }

    public Optional<TagSet> getBucketTags(RoutingContext context,
                                          AuthenticationInfo authInfo,
                                          String ns,
                                          String bucketName) {
        Optional<Bucket> bucket = getBucket(context, authInfo, ns, bucketName, Sets.newHashSet(BucketProperties.TAGS));
        return bucket.map(b -> {
            final TagSet.Builder builder = TagSet.builder();
            if (b.getFreeformTags() != null) {
                builder.freeformTags(b.getFreeformTags());
            }
            if (b.getDefinedTags() != null) {
                builder.definedTags(b.getDefinedTags());
            }
            return builder.build();
        });
    }

    private static TagSet getUpdatedTagSet(Map<String, String> updateFreeformTags,
                                           Map<String, Map<String, Object>> updateDefinedTags,
                                           TagSet curTagSet) {
        // build the new TagSet from input and current TagSet, if the input is null use current TagSet
        final TagSet.Builder builder = TagSet.builder();
        if (updateFreeformTags != null) {
            builder.freeformTags(updateFreeformTags);
        } else if (curTagSet != null && curTagSet.getFreeformTags() != null) {
            builder.freeformTags(curTagSet.getFreeformTags());
        }

        if (updateDefinedTags != null) {
            builder.definedTags(updateDefinedTags);
        } else if (curTagSet != null && curTagSet.getDefinedTags() != null) {
            builder.definedTags(curTagSet.getDefinedTags());
        }
        return builder.build();
    }

    private int getMaxBucketsForTenant(String tenantId, Api api) {
        if (tenantId == null || api.equals(Api.V1)) {
            return Integer.MAX_VALUE;
        }

        return Math.toIntExact(Math.min(
            Integer.MAX_VALUE,
            limits.getMaxBuckets(tenantId)));
    }

    private int getNumShardsForTenant(String tenantId, Api api) {
        if (tenantId == null || api.equals(Api.V1)) {
            return 1;
        }

        return Math.toIntExact(Math.min(
                Integer.MAX_VALUE,
                limits.getNumShards(tenantId)));
    }

    private static String getUpdateKmsKeyId(String kmsKeyIdToUpdate, String curKmsKeyId) {
        if (kmsKeyIdToUpdate == null) {
            return curKmsKeyId;
        }

        return kmsKeyIdToUpdate;
    }

    void updateBucketShardId(NamespaceKey namespace, String bucketName, String sourceBucketId,
                             String destinationBucketId, String destinationShardId) {
        tenantBackend.updateBucketShardId(namespace, bucketName, sourceBucketId,
                destinationBucketId, destinationShardId);
    }

    public String reencryptBucketEncryptionKey(RoutingContext context,
                                               String namespaceName,
                                               String bucketName,
                                               AuthenticationInfo authInfo) {
        Preconditions.checkNotNull(authInfo, "AuthInfo cannot be null");
        Validator.validateV2Namespace(namespaceName);
        Validator.validateBucket(bucketName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("bucketBackend:reencryptBucketEncryptionKey");
        final String opcRequestId = WSRequestContext.getCommonRequestContext(context).getOpcRequestId();
        final WSTenantBucketInfo bucket =
            tenantBackend.getBucketInfo(opcRequestId, childScope, Api.V2, namespaceName, bucketName)
                .orElseThrow(() -> new NoSuchBucketException(noSuchBucketMsg(bucketName, namespaceName)));
        final String kmsKeyId = bucket.getKmsKeyId().orElse(null);
        final Optional<AuthorizationResponse> authorizationResponse = MetricScope.timeScope(
            childScope,
            "authorize",
            (Function<MetricScope, Optional<AuthorizationResponse>>) (innerScope) -> authorizer.authorize(
                WSRequestContext.get(context),
                authInfo,
                bucket.getNamespaceKey(),
                bucket.getBucketName(),
                bucket.getCompartment(),
                bucket.getPublicAccessType(),
                CasperOperation.REENCRYPT_BUCKET,
                kmsKeyId,
                true,
                bucket.isTenancyDeleted(),
                CasperPermission.BUCKET_UPDATE));
        if (!authorizationResponse.isPresent()) {
            throw new NoSuchBucketException(noSuchBucketMsg(bucketName, authInfo.getMainPrincipal().getTenantId()));
        }

        if (Strings.isNullOrEmpty(kmsKeyId)) {
            throw new BucketNotAssociateWithKmsException("You can only re-encrypt a bucket that is encrypted by KMS.");
        }

        // for audit logs
        WSRequestContext.get(context).setNewkmsKeyId(kmsKeyId);
        final EncryptionKey reencryptedKey = EncryptionUtils.rewrapDataEncryptionKey(bucket.getEncryptionKey(kms),
                kmsKeyId, kms);
        tenantBackend.setBucketEncryptionKey(context, childScope, bucket.getKey(), bucket.getEtag(),
            reencryptedKey.getEncryptedDataEncryptionKey());

        // create bucket reencryption work request
        final ReencryptBucketRequestDetail reencryptBucketRequestDetail = new ReencryptBucketRequestDetail(
                Instant.now(), namespaceName, bucketName);
        final String tenantId = authInfo.getMainPrincipal().getTenantId();
        final String reencryptBucketDetailJson = jsonSerializer.toJson(reencryptBucketRequestDetail);
        final CreateWorkRequestDetails createRequest = CreateWorkRequestDetails.newBuilder()
                .setBucketName(bucketName)
                .setCompartmentOcid(bucket.getCompartment())
                .setTenantOcid(tenantId)
                .setRequestBlob(reencryptBucketDetailJson)
                .setSize(0)
                .setMaxRequests(maxWorkRequestPerTenant)
                .setMaxTotalSize(1)
                .setType(MdsWorkRequestType.WR_TYPE_BUCKET_REENCRYPTION)
                .build();
        final CreateWorkRequestResponse createResponse =
                MetricScope.timeScope(childScope, "WorkRequestMds:createRequest",
                        (Function<MetricScope, CreateWorkRequestResponse>) innerScope ->
                                WorkRequestBackend.WorkRequestBackendHelper.invokeMds(
                                        MdsMetrics.WORK_REQUEST_MDS_BUNDLE, MdsMetrics.WORK_REQUEST_CREATE, false,
                                    () -> workRequestClient.execute(c -> c
                                                .withDeadlineAfter(grpcWorkRequestDeadlineSeconds, TimeUnit.SECONDS)
                                                .createWorkRequest(createRequest))));
        return createResponse.getWork().getWorkRequestId();
    }

    private ObjectDbStats getStatForBucket(RoutingContext context, WSTenantBucketInfo bucket) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final GetStatForBucketRequest request = GetStatForBucketRequest.newBuilder()
                .setBucketKey(Backend.bucketKey(bucket.getNamespaceKey().getName(), bucket.getBucketName(),
                        bucket.getNamespaceKey().getApi()))
                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                .build();
        final GetStatForBucketResponse response = Backend.BackendHelper.invokeMds(
                MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_GET_STAT_FOR_BUCKET,
                true, () -> objectClient.execute(
                        c -> Backend.getClientWithOptions(c, grpcObjectDeadlineSeconds, commonContext)
                                .getStatForBucket(request)));
        return MdsTransformer.toObjectDbStats(response);
    }

    private void setBucketUncacheable(RoutingContext context, MetricScope childScope, BucketKey bucketKey) {
        // wait for all the other webServers to invalidate this bucket from the cache
        tenantBackend.setBucketUncacheable(context, childScope, bucketKey,
                bucketCacheConfiguration.getCacheConfiguration().getRefreshAfterWrite().toMillis() * 10);
        try {
            Thread.sleep(bucketCacheConfiguration.getCacheConfiguration().getRefreshAfterWrite().toMillis());
        } catch (InterruptedException ex) {
            // do nothing
        }
    }
}
