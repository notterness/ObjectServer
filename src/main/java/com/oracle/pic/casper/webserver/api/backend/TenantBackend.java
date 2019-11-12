package com.oracle.pic.casper.webserver.api.backend;

import com.google.protobuf.ByteString;
import com.oracle.pic.casper.common.config.v2.CompartmentCacheConfiguration;
import com.oracle.pic.casper.common.config.v2.MdsClientConfiguration;
import com.oracle.pic.casper.common.exceptions.InternalServerErrorException;
import com.oracle.pic.casper.common.exceptions.TooBusyException;
import com.oracle.pic.casper.common.exceptions.TooManyBucketsException;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.metrics.MetricsBundle;
import com.oracle.pic.casper.common.model.BucketMeterFlag;
import com.oracle.pic.casper.common.model.BucketMeterFlagSet;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.model.ScanResults;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.mds.MdsBucketKey;
import com.oracle.pic.casper.mds.MdsNamespaceApi;
import com.oracle.pic.casper.mds.MdsSortOrder;
import com.oracle.pic.casper.mds.common.client.MdsRequestId;
import com.oracle.pic.casper.mds.common.exceptions.MdsDbTooBusyException;
import com.oracle.pic.casper.mds.common.exceptions.MdsException;
import com.oracle.pic.casper.mds.common.exceptions.MdsResourceTenantException;
import com.oracle.pic.casper.mds.loadbalanced.MdsExecutor;
import com.oracle.pic.casper.mds.tenant.AddBucketMeterSettingRequest;
import com.oracle.pic.casper.mds.tenant.CreateBucketRequest;
import com.oracle.pic.casper.mds.tenant.CreateNamespaceRequest;
import com.oracle.pic.casper.mds.tenant.DeleteBucketRequest;
import com.oracle.pic.casper.mds.tenant.GetBucketRequest;
import com.oracle.pic.casper.mds.tenant.GetBucketResponse;
import com.oracle.pic.casper.mds.tenant.GetNamespaceRequest;
import com.oracle.pic.casper.mds.tenant.ListBucketsPageToken;
import com.oracle.pic.casper.mds.tenant.ListBucketsRequest;
import com.oracle.pic.casper.mds.tenant.ListBucketsResponse;
import com.oracle.pic.casper.mds.tenant.ListNamespacesRequest;
import com.oracle.pic.casper.mds.tenant.MdsBucket;
import com.oracle.pic.casper.mds.tenant.MdsBucketSummary;
import com.oracle.pic.casper.mds.tenant.MdsBucketsSortBy;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.mds.tenant.ReclaimNamespaceRequest;
import com.oracle.pic.casper.mds.tenant.SetBucketEncryptionKeyRequest;
import com.oracle.pic.casper.mds.tenant.SetBucketUncacheableRequest;
import com.oracle.pic.casper.mds.tenant.TenantMetadataServiceGrpc.TenantMetadataServiceBlockingStub;
import com.oracle.pic.casper.mds.tenant.UpdateBucketOptionsRequest;
import com.oracle.pic.casper.mds.tenant.UpdateBucketRequest;
import com.oracle.pic.casper.mds.tenant.UpdateBucketShardIdRequest;
import com.oracle.pic.casper.mds.tenant.UpdateNamespaceRequest;
import com.oracle.pic.casper.mds.tenant.exceptions.MdsBucketAlreadyExistsException;
import com.oracle.pic.casper.mds.tenant.exceptions.MdsBucketIfMatchException;
import com.oracle.pic.casper.mds.tenant.exceptions.MdsBucketLimitExceededException;
import com.oracle.pic.casper.mds.tenant.exceptions.MdsBucketNotFoundException;
import com.oracle.pic.casper.mds.tenant.exceptions.MdsInvalidMetadataException;
import com.oracle.pic.casper.mds.tenant.exceptions.MdsNamespaceNotFoundException;
import com.oracle.pic.casper.mds.tenant.exceptions.TenantExceptionClassifier;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.BucketAlreadyExistsException;
import com.oracle.pic.casper.objectmeta.BucketKey;
import com.oracle.pic.casper.objectmeta.InvalidMetadataException;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.objectmeta.NoSuchBucketException;
import com.oracle.pic.casper.objectmeta.NoSuchNamespaceException;
import com.oracle.pic.casper.objectmeta.UnexpectedEntityTagException;
import com.oracle.pic.casper.objectmeta.input.BucketSortBy;
import com.oracle.pic.casper.objectmeta.input.BucketSortOrder;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.common.MdsClientHelper;
import com.oracle.pic.casper.webserver.api.common.MdsTransformer;
import com.oracle.pic.casper.webserver.api.common.OptionsUpdater;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketSummary;
import com.oracle.pic.casper.webserver.api.model.serde.OptionsSerialization;
import com.oracle.pic.casper.webserver.server.MdsClients;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Backend for TenantDb operations.
 */
public class TenantBackend {

    private static final String SERVICE_UNAVAILABLE_MSG = "The service is currently unavailable.";

    private final MdsExecutor<TenantMetadataServiceBlockingStub> tenantClient;
    private final Duration grpcRequestDeadline;
    private final CompartmentCacheConfiguration compartmentCacheConfiguration;
    private final MdsClientConfiguration mdsClientConfiguration;

    static final class TenantBackendHelper {

        private TenantBackendHelper() {
        }

        private static RuntimeException toRuntimeException(StatusRuntimeException ex) {
            final MdsException mdsException = TenantExceptionClassifier.fromStatusRuntimeException(ex);
            if (mdsException instanceof MdsNamespaceNotFoundException) {
                return new NoSuchNamespaceException(mdsException.getMessage());
            } else if (mdsException instanceof MdsBucketLimitExceededException) {
                return new TooManyBucketsException(ex.getMessage(), ex.getCause());
            } else if (mdsException instanceof MdsBucketAlreadyExistsException) {
                return new BucketAlreadyExistsException(ex.getMessage());
            } else if (mdsException instanceof MdsInvalidMetadataException) {
                return new InvalidMetadataException(ex.getMessage());
            } else if (mdsException instanceof MdsBucketNotFoundException) {
                return new NoSuchBucketException(ex.getMessage());
            } else if (mdsException instanceof MdsBucketIfMatchException) {
                return new UnexpectedEntityTagException(ex.getMessage());
            } else if (mdsException instanceof MdsResourceTenantException) {
                return new TooBusyException(SERVICE_UNAVAILABLE_MSG, mdsException);
            } else if (mdsException instanceof MdsDbTooBusyException) {
                return new TooBusyException();
            } else {
                return new InternalServerErrorException(ex.getMessage(), ex.getCause());
            }
        }

        static <T> T invokeMds(
                MetricsBundle aggregateBundle, MetricsBundle apiBundle, boolean retryable, Callable<T> callable) {
            return MdsClientHelper.invokeMds(aggregateBundle, apiBundle, retryable,
                    callable, TenantBackendHelper::toRuntimeException);
        }
    }

    public TenantBackend(
            MdsClients mdsClients,
            CompartmentCacheConfiguration compartmentCacheConfiguration,
            MdsClientConfiguration mdsClientConfiguration) {
        this.tenantClient = mdsClients.getTenantMdsExecutor();
        this.grpcRequestDeadline = mdsClients.getTenantRequestDeadline();
        this.compartmentCacheConfiguration = compartmentCacheConfiguration;
        this.mdsClientConfiguration = mdsClientConfiguration;
    }

    private TenantMetadataServiceBlockingStub getClientWithOptions(
        TenantMetadataServiceBlockingStub stub,
        RoutingContext context) {
        return getClientWithOptions(stub, WSRequestContext.getCommonRequestContext(context).getOpcRequestId(),
                grpcRequestDeadline);
    }

    private TenantMetadataServiceBlockingStub getClientWithOptions(
        TenantMetadataServiceBlockingStub stub,
        String opcRequestId,
        Duration timeout) {
        Metadata metadata = new Metadata();
        metadata.put(
            MdsRequestId.OPC_REQUEST_ID_KEY,
            opcRequestId);
        return MetadataUtils.attachHeaders(
            stub.withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS),
            metadata);
    }

    void updateBucketShardId(NamespaceKey namespace, String bucketName, String sourceBucketId,
                             String destinationBucketId, String destinationShardId) {
        final UpdateBucketShardIdRequest updateBucketShardIdRequest = UpdateBucketShardIdRequest.newBuilder()
                .setBucketKey(MdsBucketKey.newBuilder()
                        .setNamespaceName(namespace.getName())
                        .setApi(MdsTransformer.toMdsNamespaceApi(namespace.getApi()))
                        .setBucketName(bucketName)
                        .build())
                .setOldBucketId(sourceBucketId)
                .setNewBucketId(destinationBucketId)
                .setNewShardId(destinationShardId)
                .build();
        TenantBackendHelper.invokeMds(MdsMetrics.TENANT_MDS_BUNDLE, MdsMetrics.TENANT_UPDATE_BUCKET_SHARD_ID,
            false,
            () -> tenantClient.execute(c ->
                c.withDeadlineAfter(grpcRequestDeadline.toMillis(), TimeUnit.MILLISECONDS)
                        .updateBucketShardId(updateBucketShardIdRequest)));
    }

    public Optional<WSTenantBucketInfo> getBucketInfo(
        String opcRequestId, MetricScope scope, Api apiVersion, String namespaceName, String bucketName) {

        VertxUtil.assertOnNonVertxEventLoop();

        Function<MetricScope, Optional<WSTenantBucketInfo>> function =
                (innerScope) -> {
                    MdsBucket resultBucket;
                    GetBucketRequest getBucketRequest = GetBucketRequest.newBuilder()
                            .setBucketKey(MdsBucketKey.newBuilder()
                                    .setNamespaceName(namespaceName)
                                    .setBucketName(bucketName)
                                    .setApi(MdsTransformer.toMdsNamespaceApi(apiVersion)))
                            .build();
                    GetBucketResponse getBucketResponse;
                    try {
                        getBucketResponse = TenantBackendHelper.invokeMds(MdsMetrics.TENANT_MDS_BUNDLE,
                                MdsMetrics.TENANT_GET_BUCKET,
                                true,
                                () -> tenantClient.execute(c -> getClientWithOptions(c,
                                        opcRequestId,
                                        mdsClientConfiguration.getTenantMdsBucketReadRequestDeadline())
                                        .getBucket(getBucketRequest)));
                    } catch (NoSuchBucketException ex) {
                        return Optional.empty();
                    }
                    MdsBucket mdsBucket = getBucketResponse.getBucket();
                    if (mdsBucket.getBucketKey().getApi().equals(MdsNamespaceApi.MDS_NS_API_V1)) {
                        resultBucket = MdsBucket.newBuilder().mergeFrom(mdsBucket)
                                .setCompartmentId(compartmentCacheConfiguration.getV1Compartment()).build();
                    } else {
                        resultBucket = mdsBucket;
                    }
                    return Optional.of(BackendConversions.mdsBucketInfoToWSBucketInfo(resultBucket));
                };

        return MetricScope.timeScope(scope, "tenantMds:getBucketInfo", function);
    }

    public WSTenantBucketInfo createBucket(
        RoutingContext context, MetricScope scope, CreateBucketRequest request) {
        VertxUtil.assertOnNonVertxEventLoop();

        return MetricScope.timeScope(scope, "tenantMds:createBucket",
                (Function<MetricScope, WSTenantBucketInfo>) innerScope ->
                        TenantBackendHelper.invokeMds(MdsMetrics.TENANT_MDS_BUNDLE, MdsMetrics.TENANT_CREATE_BUCKET,
                            false,
                            () -> BackendConversions.mdsBucketInfoToWSBucketInfo(tenantClient.execute(c ->
                                getClientWithOptions(c, context).createBucket(request).getBucket()))));
    }

    /**
     * Called after a bucket is deleted. This removes the bucket from the
     * tenant MDS.
     */
    public void deleteBucket(
        RoutingContext context, MetricScope scope, WSTenantBucketInfo bucket, @Nullable String matchingEtag) {
        VertxUtil.assertOnNonVertxEventLoop();

        DeleteBucketRequest.Builder builder = DeleteBucketRequest.newBuilder()
                .setBucketKey(MdsTransformer.toMdsBucketKey(bucket.getKey()));
        if (matchingEtag != null) {
            builder.setIfMatchEtag(matchingEtag);
        }
        final DeleteBucketRequest deleteBucketRequest = builder.build();
        MetricScope.timeScope(scope, "tenantMds:deleteBucket",
                (Consumer<MetricScope>) innerScope ->
                        TenantBackendHelper.invokeMds(MdsMetrics.TENANT_MDS_BUNDLE, MdsMetrics.TENANT_DELETE_BUCKET,
                            false,
                            () -> tenantClient.execute(c -> getClientWithOptions(c, context).deleteBucket(
                                    deleteBucketRequest))));
    }

    WSTenantBucketInfo updateBucket(
            RoutingContext context, MetricScope scope, UpdateBucketRequest request) {
        VertxUtil.assertOnNonVertxEventLoop();
        return MetricScope.timeScope(scope, "tenantMds:updateBucket",
                (Function<MetricScope, WSTenantBucketInfo>) innerScope ->
                        TenantBackendHelper.invokeMds(MdsMetrics.TENANT_MDS_BUNDLE, MdsMetrics.TENANT_UPDATE_BUCKET,
                            false,
                            () -> BackendConversions.mdsBucketInfoToWSBucketInfo(tenantClient.execute(c ->
                                    getClientWithOptions(c,
                                    context).updateBucket(request).getBucket()))));
    }

    ScanResults<WSTenantBucketSummary> listBucketsInNamespace(
            RoutingContext context,
            MetricScope scope,
            NamespaceKey namespace,
            int pageSize,
            boolean returnTags,
            @Nullable String cursor) {
        VertxUtil.assertOnNonVertxEventLoop();

        final ListBucketsRequest.Builder builder = ListBucketsRequest.newBuilder()
                .setNamespaceName(namespace.getName())
                .setApi(MdsTransformer.toMdsNamespaceApi(namespace.getApi()))
                .setPageSize(pageSize)
                .setIncludeTags(returnTags)
                .setSortBy(MdsBucketsSortBy.BS_BY_NAME)
                .setSortOrder(MdsSortOrder.MDS_SORT_ASCENDING);
        if (cursor != null) {
            builder.setPageToken(ListBucketsPageToken.newBuilder().setBucketName(cursor).build());
        }
        return getTenantBucketSummary(context, scope, builder);
    }

    private ScanResults<WSTenantBucketSummary> getTenantBucketSummary(RoutingContext context, MetricScope scope,
                                                                      ListBucketsRequest.Builder builder) {
        final ListBucketsRequest listBucketsRequest = builder.build();
        return MetricScope.timeScope(scope,
            "tenantMds:listBuckets",
            (Function<MetricScope, ScanResults<WSTenantBucketSummary>>) innerScope -> TenantBackendHelper.invokeMds(
                MdsMetrics.TENANT_MDS_BUNDLE,
                MdsMetrics.TENANT_LIST_BUCKETS,
                true,
                () -> {
                    final ListBucketsResponse listBucketsResponse = tenantClient.execute(c ->
                        getClientWithOptions(c, context).listBuckets(listBucketsRequest));
                    final List<MdsBucketSummary> mdsBucketSummaries = listBucketsResponse.getBucketsList();
                    return new ScanResults<>(mdsBucketSummaries.stream()
                        .map(BackendConversions::mdsBucketSummaryToWSBucketSummary)
                        .collect(Collectors.toList()),
                        !listBucketsResponse.getNextPageToken().getBucketName().isEmpty());
                }));
    }

    List<MdsNamespace> listNamespaces(RoutingContext context, MetricScope scope, Api api, int maxNamespaces,
            Optional<NamespaceKey> startingAfter) {
        VertxUtil.assertOnNonVertxEventLoop();

        final ListNamespacesRequest listNamespacesRequest = ListNamespacesRequest.newBuilder()
                .setApi(MdsTransformer.toMdsNamespaceApi(api))
                .setPageSize(maxNamespaces)
                .setPageToken(startingAfter.isPresent() ? startingAfter.get().getName() : "")
                .build();
        return MetricScope.timeScope(scope, "tenantMds:listNamespaces",
                (Function<MetricScope, List<MdsNamespace>>) innerScope ->
                        TenantBackendHelper.invokeMds(MdsMetrics.TENANT_MDS_BUNDLE, MdsMetrics.TENANT_LIST_NAMESPACES,
                            true,
                            () -> tenantClient.execute(c -> getClientWithOptions(c, context)
                                        .listNamespaces(listNamespacesRequest).getNamespaceList())));
    }

    ScanResults<WSTenantBucketSummary> listBucketsInCompartment(
            RoutingContext context,
            MetricScope scope,
            NamespaceKey namespace,
            String compartment,
            int pageSize,
            boolean returnTags,
            Pair<String, Instant> cursor,
            @Nullable BucketSortBy sortBy,
            @Nullable BucketSortOrder sortOrder) {

        VertxUtil.assertOnNonVertxEventLoop();
        final ListBucketsRequest.Builder builder = ListBucketsRequest.newBuilder()
                .setNamespaceName(namespace.getName())
                .setApi(MdsTransformer.toMdsNamespaceApi(namespace.getApi()))
                .setCompartmentId(compartment)
                .setPageSize(pageSize)
                .setIncludeTags(returnTags);
        if (cursor.getFirst() != null || cursor.getSecond() != null) {
            ListBucketsPageToken.Builder pageTokenBuilder = ListBucketsPageToken.newBuilder();
            if (cursor.getFirst() != null) {
                pageTokenBuilder.setBucketName(cursor.getFirst());
            }
            if (cursor.getSecond() != null) {
                pageTokenBuilder.setCreationTime(MdsTransformer.toTimestamp(cursor.getSecond()));
            }
            builder.setPageToken(pageTokenBuilder.build());
        }
        if (sortBy == null) {
            sortBy = BucketSortBy.NAME;
        }
        if (sortOrder == null) {
            sortOrder = sortBy.getDefaultSortOrder();
        }
        builder.setSortBy(MdsTransformer.toMdsSortBy(sortBy));
        builder.setSortOrder(MdsTransformer.toMdsSortOrder(sortOrder));
        return getTenantBucketSummary(context, scope, builder);

    }

    MdsNamespace updateNamespace(
            RoutingContext context, MetricScope scope, UpdateNamespaceRequest request, AuthenticationInfo authInfo) {
        return MetricScope.timeScope(scope, "tenantMds:updateNamespace",
                (Function<MetricScope, MdsNamespace>) innerScope ->
                        TenantBackendHelper.invokeMds(
                                MdsMetrics.TENANT_MDS_BUNDLE, MdsMetrics.TENANT_UPDATE_NAMESPACE, false, () -> {
                                    final MdsNamespace updatedNamespace =
                                            tenantClient.execute(c -> getClientWithOptions(c, context).updateNamespace(
                                                    request).getNamespace());
                                    return handleNamespaceNullOcids(
                                            updatedNamespace, authInfo.getMainPrincipal().getTenantId());
                                }));
    }

    public Optional<MdsNamespace> getNamespace(
        RoutingContext context, MetricScope scope, NamespaceKey namespaceKey) {
        Validator.validateV2Namespace(namespaceKey.getName());
        final GetNamespaceRequest getNamespaceRequest = GetNamespaceRequest.newBuilder()
                .setNamespaceName(namespaceKey.getName())
                .setApi(MdsTransformer.toMdsNamespaceApi(namespaceKey.getApi()))
                .build();
        MdsNamespace mdsNamespace;
        try {
            mdsNamespace = MetricScope.timeScope(scope, "tenantMds:getNamespaceInfo",
                    (Function<MetricScope, MdsNamespace>) innerScope ->
                            TenantBackendHelper.invokeMds(
                                    MdsMetrics.TENANT_MDS_BUNDLE, MdsMetrics.TENANT_GET_NAMESPACE, true, () ->
                                            tenantClient.execute(c -> getClientWithOptions(c, context)
                                                    .getNamespace(getNamespaceRequest)
                                                    .getNamespace())));
        } catch (NoSuchNamespaceException ex) {
            mdsNamespace = null;
        }
        return Optional.ofNullable(mdsNamespace);
    }

    /**
     * Get information about the namespace, creating the namespace if necessary.
     */
    public MdsNamespace getOrCreateNamespace(
        RoutingContext context, MetricScope scope, NamespaceKey namespaceKey, String tenantOcid) {
        Validator.validateV2Namespace(namespaceKey.getName());

        Optional<MdsNamespace> namespaceInfo = getNamespace(context, scope, namespaceKey);

        if (!namespaceInfo.isPresent()) {
            CreateNamespaceRequest.Builder builder = CreateNamespaceRequest.newBuilder()
                    .setNamespaceName(namespaceKey.getName())
                    .setApi(MdsTransformer.toMdsNamespaceApi(namespaceKey.getApi()));
            if (tenantOcid != null) {
                builder.setTenantOcid(tenantOcid);
            }
            final CreateNamespaceRequest createNamespaceRequest = builder.build();
            MetricScope.timeScope(scope,
                "tenantMds:createNamespace",
                (Consumer<MetricScope>) innerScope -> TenantBackendHelper.invokeMds(MdsMetrics.TENANT_MDS_BUNDLE,
                    MdsMetrics.TENANT_CREATE_NAMESPACE,
                    false,
                    () -> tenantClient.execute(c -> getClientWithOptions(c, context).createNamespace(
                            createNamespaceRequest))));
            // It is possible this could fail if another session is creating the namespace
            // but has not committed so we can not see the namespace here
            namespaceInfo = getNamespace(context, scope, namespaceKey);
        }
        return handleNamespaceNullOcids(namespaceInfo.get(), tenantOcid);
    }

    //Used for Internal Testing only
    public void reclaimNamespace(NamespaceKey namespaceKey, String tenantOcid, RoutingContext context) {
        ReclaimNamespaceRequest reclaimNamespaceRequest = ReclaimNamespaceRequest.newBuilder()
                .setApi(MdsTransformer.toMdsNamespaceApi(namespaceKey.getApi()))
                .setNamespaceName(namespaceKey.getName())
                .setTenantOcid(tenantOcid)
                .build();
        tenantClient.execute(c ->
                getClientWithOptions(c, context).reclaimNamespace(reclaimNamespaceRequest));
    }

    private MdsNamespace handleNamespaceNullOcids(MdsNamespace mdsNamespace, String tenantOcid) {
        final String tenantId = !mdsNamespace.getTenantOcid().isEmpty() ? mdsNamespace.getTenantOcid() : tenantOcid;
        final String s3CompartmentId =
            !mdsNamespace.getDefaultS3Compartment().isEmpty() ? mdsNamespace.getDefaultS3Compartment() : tenantId;
        final String swiftCompartmentId =
            !mdsNamespace.getDefaultSwiftCompartment().isEmpty() ? mdsNamespace.getDefaultSwiftCompartment() : tenantId;
        return MdsNamespace.newBuilder()
                .mergeFrom(mdsNamespace)
                .setTenantOcid(tenantId)
                .setDefaultSwiftCompartment(swiftCompartmentId)
                .setDefaultS3Compartment(s3CompartmentId)
                .build();
    }

    void addBucketMeterSetting(
            RoutingContext context, MetricScope scope, WSTenantBucketInfo bucket, BucketMeterFlag meterSetting) {
        VertxUtil.assertOnNonVertxEventLoop();

        BucketMeterFlagSet meterFlagSet = bucket.getMeterFlagSet().newCopy();
        boolean addedNewFlag = meterFlagSet.add(meterSetting);

        if (addedNewFlag) {
            final AddBucketMeterSettingRequest addBucketMeterSettingRequest = AddBucketMeterSettingRequest.newBuilder()
                    .setBucketKey(MdsBucketKey.newBuilder()
                            .setNamespaceName(bucket.getNamespaceKey().getName())
                            .setApi(MdsTransformer.toMdsNamespaceApi(bucket.getNamespaceKey().getApi()))
                            .setBucketName(bucket.getBucketName())
                            .build())
                    .setMatchingEtag(bucket.getEtag())
                    .addAllMeterFlagSet(MdsTransformer.toBucketMeterFlagSet(meterFlagSet))
                    .build();
            MetricScope.timeScope(scope,
                "tenantMds:addBucketMeterSetting",
                (Consumer<MetricScope>) innerScope -> TenantBackendHelper.invokeMds(MdsMetrics.TENANT_MDS_BUNDLE,
                    MdsMetrics.TENANT_ADD_BUCKET_METER_FLAG_SET,
                    false,
                    () -> tenantClient.execute(c -> getClientWithOptions(c, context).addBucketMeterSetting(
                            addBucketMeterSettingRequest))));
        }
    }

    void updateBucketOptions(
            RoutingContext context, MetricScope scope, WSTenantBucketInfo bucket,
            Map<String, Object> newOptions) {
        VertxUtil.assertOnNonVertxEventLoop();

        // If the incoming options is empty/null, then all existing options are removed by persisting a
        // serialized version of an empty map to the tenant mds db
        // Otherwise, key/values in the incoming map are merged with the existing one and the resultant
        // map is serialized and persisted to the tenant mds db
        final Map<String, Object> oldOptions = bucket.getOptions();
        final Map<String, Object> mergedOptions = OptionsUpdater.updateOptions(oldOptions, newOptions);

        final UpdateBucketOptionsRequest updateBucketOptionsRequest = UpdateBucketOptionsRequest.newBuilder()
                .setBucketKey(MdsBucketKey.newBuilder()
                        .setNamespaceName(bucket.getNamespaceKey().getName())
                        .setApi(MdsTransformer.toMdsNamespaceApi(bucket.getNamespaceKey().getApi()))
                        .setBucketName(bucket.getBucketName())
                        .build())
                .setMatchingEtag(bucket.getEtag())
                .setOptions(OptionsSerialization.serializeOptions(mergedOptions))
                .build();
        MetricScope.timeScope(scope,
            "tenantMds:updateBucketOptions",
            (Consumer<MetricScope>) innerScope -> TenantBackendHelper.invokeMds(MdsMetrics.TENANT_MDS_BUNDLE,
                MdsMetrics.TENANT_UPDATE_BUCKET_OPTIONS,
                false,
                () -> tenantClient.execute(c -> getClientWithOptions(c, context).updateBucketOptions(
                        updateBucketOptionsRequest))));
    }

    void setBucketEncryptionKey(RoutingContext context, MetricScope scope, BucketKey bucketKey, String etag,
                                byte[] encryptedEncryptionKey) {
        final SetBucketEncryptionKeyRequest setBucketEncryptionKeyRequest = SetBucketEncryptionKeyRequest.newBuilder()
                .setBucketKey(MdsTransformer.toMdsBucketKey(bucketKey))
                .setEtag(etag)
                .setEncryptionKeyPayload(ByteString.copyFrom(encryptedEncryptionKey))
                .build();
        MetricScope.timeScope(scope,
            "tenantMds:setBucketEncryptionKey",
            (Consumer<MetricScope>) innerScope -> TenantBackendHelper.invokeMds(MdsMetrics.TENANT_MDS_BUNDLE,
                MdsMetrics.TENANT_SET_BUCKET_ENCRYPTION_KEY,
                false,
                () -> tenantClient.execute(c -> getClientWithOptions(
                        c, context).setBucketEncryptionKey(setBucketEncryptionKeyRequest))));
    }

    void setBucketUncacheable(RoutingContext context, MetricScope scope, BucketKey bucketKey, long durationInMs) {
        final SetBucketUncacheableRequest setBucketUncacheableRequest = SetBucketUncacheableRequest.newBuilder()
                .setBucketKey(MdsTransformer.toMdsBucketKey(bucketKey))
                .setUncacheableDurationInMs(durationInMs)
                .build();
        MetricScope.timeScope(scope,
                "tenantMds:setBucketUncacheable",
                (Consumer<MetricScope>) innerScope -> TenantBackendHelper.invokeMds(MdsMetrics.TENANT_MDS_BUNDLE,
                        MdsMetrics.TENANT_SET_BUCKET_UNCACHEABLE,
                        false,
                        () -> tenantClient.execute(c -> getClientWithOptions(
                                c, context).setBucketUncacheable(setBucketUncacheableRequest))));
    }
}
