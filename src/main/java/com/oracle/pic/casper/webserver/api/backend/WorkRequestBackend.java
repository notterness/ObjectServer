package com.oracle.pic.casper.webserver.api.backend;

import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.encryption.EncryptionKey;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.exceptions.IfMatchException;
import com.oracle.pic.casper.common.exceptions.IfNoneMatchException;
import com.oracle.pic.casper.common.exceptions.InternalServerErrorException;
import com.oracle.pic.casper.common.exceptions.NotFoundException;
import com.oracle.pic.casper.common.exceptions.RequestAlreadyExistsException;
import com.oracle.pic.casper.common.exceptions.TooBusyException;
import com.oracle.pic.casper.common.exceptions.TooManyRequestsException;
import com.oracle.pic.casper.common.json.JsonSerDe;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.metrics.MetricsBundle;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.replication.ReplicationOptions;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.vertx.HttpServerUtil;
import com.oracle.pic.casper.webserver.auth.limits.Limits;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.mds.common.client.MdsRequestId;
import com.oracle.pic.casper.mds.common.exceptions.MdsDbTooBusyException;
import com.oracle.pic.casper.mds.common.exceptions.MdsException;
import com.oracle.pic.casper.mds.common.exceptions.MdsIfMatchException;
import com.oracle.pic.casper.mds.common.exceptions.MdsIfNoneMatchException;
import com.oracle.pic.casper.mds.common.exceptions.MdsInvalidArgumentException;
import com.oracle.pic.casper.mds.loadbalanced.MdsExecutor;
import com.oracle.pic.casper.mds.workrequest.CancelWorkRequestDetails;
import com.oracle.pic.casper.mds.workrequest.CreateReplicationPolicyDetails;
import com.oracle.pic.casper.mds.workrequest.CreateReplicationPolicyResponse;
import com.oracle.pic.casper.mds.workrequest.CreateWorkRequestDetails;
import com.oracle.pic.casper.mds.workrequest.CreateWorkRequestResponse;
import com.oracle.pic.casper.mds.workrequest.DeleteReplicationPolicyDetails;
import com.oracle.pic.casper.mds.workrequest.GetReplicationPolicyDetails;
import com.oracle.pic.casper.mds.workrequest.GetReplicationPolicyResponse;
import com.oracle.pic.casper.mds.workrequest.GetWorkRequestDetails;
import com.oracle.pic.casper.mds.workrequest.GetWorkRequestResponse;
import com.oracle.pic.casper.mds.workrequest.ListReplicationPoliciesDetails;
import com.oracle.pic.casper.mds.workrequest.ListReplicationPoliciesResponse;
import com.oracle.pic.casper.mds.workrequest.ListWorkRequestsDetails;
import com.oracle.pic.casper.mds.workrequest.ListWorkRequestsResponse;
import com.oracle.pic.casper.mds.workrequest.MdsWorkRequest;
import com.oracle.pic.casper.mds.workrequest.MdsWorkRequestType;
import com.oracle.pic.casper.mds.workrequest.WorkRequestMetadataServiceGrpc.WorkRequestMetadataServiceBlockingStub;
import com.oracle.pic.casper.mds.workrequest.exceptions.MdsReplicationPolicyExistException;
import com.oracle.pic.casper.mds.workrequest.exceptions.MdsReplicationPolicyNotFoundException;
import com.oracle.pic.casper.mds.workrequest.exceptions.MdsRequestAlreadyExistsException;
import com.oracle.pic.casper.mds.workrequest.exceptions.MdsTooManyRequestsException;
import com.oracle.pic.casper.mds.workrequest.exceptions.MdsTotalSizeExceededException;
import com.oracle.pic.casper.mds.workrequest.exceptions.MdsWorkRequestNotFoundException;
import com.oracle.pic.casper.mds.workrequest.exceptions.MdsWrongStateException;
import com.oracle.pic.casper.mds.workrequest.exceptions.WorkRequestExceptionClassifier;
import com.oracle.pic.casper.metadata.utils.CryptoUtils;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authorizer;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.NotAuthenticatedException;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledServicePrincipalClient;
import com.oracle.pic.casper.webserver.api.common.HttpPathHelpers;
import com.oracle.pic.casper.webserver.api.common.MdsClientHelper;
import com.oracle.pic.casper.webserver.api.common.MdsTransformer;
import com.oracle.pic.casper.webserver.api.common.MetadataUpdater;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.ReplicationSource;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;
import com.oracle.pic.casper.webserver.api.model.WorkRequestJsonHelper;
import com.oracle.pic.casper.webserver.api.model.WsReplicationPolicy;
import com.oracle.pic.casper.webserver.api.model.WsReplicationPolicyList;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchNamespaceException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchObjectException;
import com.oracle.pic.casper.webserver.api.model.exceptions.ReplicationPolicyConflictException;
import com.oracle.pic.casper.webserver.api.replication.ReplicationUtils;
import com.oracle.pic.casper.webserver.server.MdsClients;
import com.oracle.pic.casper.webserver.util.BucketOptionsUtil;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.casper.workrequest.WorkRequestDetail;
import com.oracle.pic.casper.workrequest.WorkRequestType;
import com.oracle.pic.casper.workrequest.WorkRequestWrongStateException;
import com.oracle.pic.casper.workrequest.bulkrestore.BulkRestoreRequestDetail;
import com.oracle.pic.casper.workrequest.copy.CopyRequestDetail;
import com.oracle.pic.identity.authentication.Principal;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public class WorkRequestBackend {

    private static final Logger LOG = LoggerFactory.getLogger(WorkRequestBackend.class);
    private final MdsExecutor<WorkRequestMetadataServiceBlockingStub> workRequestClient;
    private final long grpcRequestDeadlineSeconds;
    private final JsonSerDe jsonSerDe;
    private final Authorizer authorizer;
    private final BucketBackend bucketBackend;
    private final Backend backend;
    private final ResourceControlledServicePrincipalClient resourceControlledServicePrincipalClient;
    private final DecidingKeyManagementService kms;
    private final Limits limits;

    static final class WorkRequestBackendHelper {

        private WorkRequestBackendHelper() {
        }

        private static RuntimeException toRuntimeException(StatusRuntimeException ex) {
            final MdsException mdsException = WorkRequestExceptionClassifier.fromStatusRuntimeException(ex);
            if (mdsException instanceof MdsRequestAlreadyExistsException) {
                return new RequestAlreadyExistsException("Request already exists.");
            } else if (mdsException instanceof MdsReplicationPolicyExistException) {
                return new ReplicationPolicyConflictException("Policy already exists.");
            } else if (mdsException instanceof MdsWorkRequestNotFoundException ||
                    mdsException instanceof MdsReplicationPolicyNotFoundException) {
                return new NotFoundException(ex.getMessage(), ex.getCause());
            } else if (mdsException instanceof MdsInvalidArgumentException) {
                return new InternalServerErrorException(ex.getMessage(), ex.getCause());
            } else if (mdsException instanceof MdsWrongStateException) {
                MdsWrongStateException mdsWrongStateException = (MdsWrongStateException) mdsException;
                return new WorkRequestWrongStateException(mdsWrongStateException.getId(),
                        MdsTransformer.mdsWorkRequestStateToWorkRequestState(
                                mdsWrongStateException.getMdsWorkRequestState()));
            } else if (mdsException instanceof MdsTooManyRequestsException) {
                return new TooManyRequestsException("Too many requests; please try again later.");
            } else if (mdsException instanceof MdsTotalSizeExceededException) {
                return new TooManyRequestsException("Total request size exceeded; please try again later.");
            } else if (mdsException instanceof MdsDbTooBusyException) {
                return new TooBusyException();
            } else if (mdsException instanceof MdsIfMatchException) {
                return new IfMatchException(ex.getMessage(), ex.getCause());
            } else if (mdsException instanceof MdsIfNoneMatchException) {
                return new IfNoneMatchException(ex.getMessage(), ex.getCause());
            } else {
                return new InternalServerErrorException(ex.getMessage(), ex.getCause());
            }
        }

        public static <T> T invokeMds(
                MetricsBundle aggregateBundle, MetricsBundle apiBundle, boolean retryable, Callable<T> callable) {
            return MdsClientHelper.invokeMds(aggregateBundle, apiBundle, retryable,
                    callable, WorkRequestBackendHelper::toRuntimeException);
        }
    }

    public WorkRequestBackend(JsonSerDe jsonSerDe, MdsClients mdsClients, Authorizer authorizer,
                              BucketBackend bucketBackend, Backend backend,
                              ResourceControlledServicePrincipalClient resourceControlledServicePrincipalClient,
                              DecidingKeyManagementService kms,
                              Limits limits) {
        this.jsonSerDe = jsonSerDe;
        this.authorizer = authorizer;
        this.bucketBackend = bucketBackend;
        this.backend = backend;
        this.resourceControlledServicePrincipalClient = resourceControlledServicePrincipalClient;
        this.kms = kms;
        this.limits = limits;
        this.workRequestClient = mdsClients.getWorkrequestMdsExecutor();
        this.grpcRequestDeadlineSeconds = mdsClients.getWorkRequestDeadline().getSeconds();
    }

    /**
     * @return a WorkRequestClient with a deadline set and opcRequestId as metadata.
     */
    private WorkRequestMetadataServiceBlockingStub getClientWithOptions(
            WorkRequestMetadataServiceBlockingStub stub,
            RoutingContext context) {
        Metadata metadata = new Metadata();
        metadata.put(
                MdsRequestId.OPC_REQUEST_ID_KEY,
                WSRequestContext.getCommonRequestContext(context).getOpcRequestId());
        return MetadataUtils.attachHeaders(
                stub.withDeadlineAfter(grpcRequestDeadlineSeconds, TimeUnit.SECONDS),
                metadata);
    }

    public String createCopyRequest(RoutingContext context, AuthenticationInfo authInfo, CopyRequestDetail detail,
                                    boolean isMissingMetadata, Map<String, String> metadata, String namespace) {
        Validator.validateV2Namespace(detail.getSourceNamespace());
        Validator.validateBucket(detail.getSourceBucket());
        Validator.validateObjectName(detail.getSourceObject());
        Validator.validateV2Namespace(detail.getDestNamespace());
        Validator.validateBucket(detail.getDestBucket());
        Validator.validateObjectName(detail.getDestObject());

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope()
                .annotate("namespaceName", detail.getSourceNamespace())
                .annotate("bucketName", detail.getSourceBucket())
                .annotate("objectName", detail.getSourceObject());
        final MetricScope scope = rootScope.child("WorkRequestBackend:createCopyRequest")
                .annotate("destRegion", detail.getDestConfigRegion())
                .annotate("destNamespace", detail.getDestNamespace())
                .annotate("destBucket", detail.getDestBucket())
                .annotate("destObject", detail.getDestObject());

        //authZ
        final WSTenantBucketInfo sourceBucketInfo = bucketBackend.getBucketMetadata(context, scope,
                detail.getSourceNamespace(),
                detail.getSourceBucket());
        WSRequestContext.get(context).setObjectLevelAuditMode(sourceBucketInfo.getObjectLevelAuditMode());
        WSRequestContext.get(context).setBucketOcid(sourceBucketInfo.getOcid());
        WSRequestContext.get(context).setBucketCreator(sourceBucketInfo.getCreationUser());
        WSRequestContext.get(context).setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(sourceBucketInfo.getOptions()));

        final String compartmentId = sourceBucketInfo.getCompartment();
        if (!MetricScope.timeScope(scope, "authorizer:authorize", (Function<MetricScope, Boolean>) innerScope ->
                authorizer.authorize(WSRequestContext.get(context), authInfo, sourceBucketInfo.getNamespaceKey(),
                        sourceBucketInfo.getBucketName(), compartmentId, sourceBucketInfo.getPublicAccessType(),
                        CasperOperation.CREATE_COPY_REQUEST, null, true,
                        sourceBucketInfo.isTenancyDeleted(), CasperPermission.OBJECT_READ)
                        .isPresent())) {
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(detail.getSourceBucket(),
                    detail.getSourceNamespace()));
        }

        //source object
        WSStorageObject wsStorageObject = MetricScope.timeScope(scope, "objectMds:get",
                (Function<MetricScope, WSStorageObject>) innerScope -> backend.getObject(
                        context, sourceBucketInfo, detail.getSourceObject())
                        .orElseThrow(() -> new NoSuchObjectException("Source Object not found.")));

        final String sourceIfMatch;
        if (detail.getSourceIfMatch() == null) {
            //if not specified by request, use current etag
            sourceIfMatch = wsStorageObject.getETag();
        } else {
            //otherwise check if etag matches
            sourceIfMatch = detail.getSourceIfMatch();
            if (!sourceIfMatch.equals("*") && !sourceIfMatch.equals(wsStorageObject.getETag())) {
                throw new IfMatchException("Source object etag mismatch, found " + wsStorageObject.getETag());
            }
        }

        final String oboToken = createOboToken(context, authInfo, namespace);

        //compute metadata
        final Map<String, String> destMetadata = MetadataUpdater.updateMetadata(isMissingMetadata,
                wsStorageObject.getMetadata(kms), metadata);

        //encrypt metadata
        final EncryptionKey encryptionKey =
                PutObjectBackend.getEncryption(sourceBucketInfo.getKmsKeyId().orElse(null), kms);
        final String encryptedMetadata = CryptoUtils.encryptMetadata(destMetadata, encryptionKey);

        final long sourceSize = wsStorageObject.getTotalSizeInBytes();
        //store request into queue
        final CopyRequestDetail detailWithSourceInfo = new CopyRequestDetail.Builder(detail)
                .sourceSize(sourceSize)
                .sourceMd5(wsStorageObject.getMd5())
                .sourceIfMatch(sourceIfMatch)
                .oboToken(oboToken)
                .encryptedMetadata(encryptedMetadata)
                .encryptionKey(encryptionKey.getEncryptedDataEncryptionKey())
                .encryptionKeyVersion(encryptionKey.getVersion())
                .build();
        final String requestBlob = jsonSerDe.toJson(detailWithSourceInfo);

        final String tenantId = authInfo.getMainPrincipal().getTenantId();
        final long maxCopyRequests = MetricScope.timeScope(scope, "Limits:getMaxCopyRequests",
                (Function<MetricScope, Long>) innerScope -> limits.getMaxCopyRequests(tenantId));
        final long maxCopyBytes = MetricScope.timeScope(scope, "Limits:getMaxCopyBytes",
                (Function<MetricScope, Long>) innerScope -> limits.getMaxCopyBytes(tenantId));

        CreateWorkRequestDetails createRequest = CreateWorkRequestDetails.newBuilder()
                .setBucketName(detail.getSourceBucket())
                .setCompartmentOcid(compartmentId)
                .setTenantOcid(tenantId)
                .setRequestBlob(requestBlob)
                .setSize(sourceSize)
                .setMaxRequests(maxCopyRequests)
                .setMaxTotalSize(maxCopyBytes)
                .setType(MdsWorkRequestType.WR_TYPE_COPY)
                .build();

        final CreateWorkRequestResponse createResponse = MetricScope.timeScope(
                scope,
                "WorkRequestMds:createRequest",
                (Function<MetricScope, CreateWorkRequestResponse>) innerScope -> WorkRequestBackendHelper.invokeMds(
                        MdsMetrics.WORK_REQUEST_MDS_BUNDLE,
                        MdsMetrics.WORK_REQUEST_CREATE,
                        false,
                        () -> workRequestClient.execute(c -> getClientWithOptions(c, context).createWorkRequest(
                                createRequest))));
        WebServerMetrics.COPY_REQUEST_BYTES.get(detail.getDestConfigRegion()).mark(sourceSize);
        ServiceLogsHelper.logServiceEntry(context);
        return createResponse.getWork().getWorkRequestId();
    }

    private String createOboToken(RoutingContext context, AuthenticationInfo authInfo, String namespace) {
        final HttpServerRequest request = context.request();
        final Principal userPrincipal = authInfo.getUserPrincipal().orElse(null);
        final Principal servicePrincipal = authInfo.getServicePrincipal().orElse(null);
        final String oboToken;
        try {
            switch (authInfo.getRequestKind()) {
                case SERVICE:
                    oboToken = resourceControlledServicePrincipalClient.getOboToken(request.method().name(),
                            request.path(), HttpServerUtil.getHeaders(request), servicePrincipal,
                            null, namespace);
                    break;
                case USER:
                case INSTANCE:
                    oboToken = resourceControlledServicePrincipalClient.getOboToken(request.method().name(),
                            request.path(), HttpServerUtil.getHeaders(request), userPrincipal,
                            null, namespace);
                    break;
                case OBO:
                case DELEGATION:
                    oboToken = resourceControlledServicePrincipalClient.getOboToken(request.method().name(),
                            request.path(), HttpServerUtil.getHeaders(request), userPrincipal,
                            servicePrincipal, namespace);
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected auth kind: " + authInfo.getRequestKind());
            }
        } catch (Exception e) {
            throw new NotAuthenticatedException("Failed to get obo token", e);
        }
        return oboToken;
    }

    public String createBulkRestoreRequest(RoutingContext context, AuthenticationInfo authInfo,
                                           BulkRestoreRequestDetail detail) {
        Validator.validateV2Namespace(detail.getNamespace());
        Validator.validateBucket(detail.getBucket());

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        HttpPathHelpers.logPathParameters(rootScope, detail.getNamespace(), detail.getBucket(), null);
        final MetricScope scope = rootScope.child("WorkRequestBackend:createBulkRestoreRequest")
                .annotate("restoreDurationInHours", detail.getRestoreDurationInHours());

        //authZ
        final WSTenantBucketInfo bucketInfo =
                bucketBackend.getBucketMetadata(context, scope, detail.getNamespace(), detail.getBucket());
        WSRequestContext.get(context).setObjectLevelAuditMode(bucketInfo.getObjectLevelAuditMode());
        WSRequestContext.get(context).setBucketOcid(bucketInfo.getOcid());
        WSRequestContext.get(context).setBucketCreator(bucketInfo.getCreationUser());

        final String compartmentId = bucketInfo.getCompartment();
        if (!MetricScope.timeScope(scope, "authorizer:authorize", (Function<MetricScope, Boolean>) innerScope ->
                authorizer.authorize(WSRequestContext.get(context), authInfo, bucketInfo.getNamespaceKey(),
                        bucketInfo.getBucketName(), compartmentId, bucketInfo.getPublicAccessType(),
                        CasperOperation.CREATE_BULK_RESTORE_REQUEST, null, true,
                        bucketInfo.isTenancyDeleted(), CasperPermission.OBJECT_RESTORE).isPresent())) {
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(detail.getBucket(), detail.getNamespace()));
        }

        final String requestBlob = jsonSerDe.toJson(detail);

        final String tenantId = authInfo.getMainPrincipal().getTenantId();

        final long maxBulkRestoreRequests = MetricScope.timeScope(scope, "Limits:getMaxBulkRestoreRequests",
                (Function<MetricScope, Long>) innerScope -> limits.getMaxBulkRestoreRequests(tenantId));
        final CreateWorkRequestDetails bulkRestoreRequest = CreateWorkRequestDetails.newBuilder()
                .setType(MdsWorkRequestType.WR_TYPE_BULK_RESTORE)
                .setRequestBlob(requestBlob)
                .setTenantOcid(tenantId)
                .setCompartmentOcid(compartmentId)
                .setBucketName(detail.getBucket())
                .setMaxRequests(maxBulkRestoreRequests)
                .setSize(0)
                .setMaxTotalSize(1)
                .build();

        final CreateWorkRequestResponse createResponse =
                MetricScope.timeScope(scope, "WorkRequestMds:createRequest",
                        (Function<MetricScope, CreateWorkRequestResponse>) innerScope ->
                                WorkRequestBackendHelper.invokeMds(
                                        MdsMetrics.WORK_REQUEST_MDS_BUNDLE, MdsMetrics.WORK_REQUEST_CREATE,
                                        false,
                                        () -> workRequestClient.execute(c -> getClientWithOptions(c, context)
                                                .createWorkRequest(bulkRestoreRequest)))
                );
        return createResponse.getWork().getWorkRequestId();
    }

    public Pair<MdsWorkRequest, WorkRequestDetail> getRequest(RoutingContext context,
                                                              AuthenticationInfo authInfo,
                                                              String requestId) {
        Validator.validateWorkRequestId(requestId);
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope scope = rootScope.child("WorkRequestBackend:getRequest")
                .annotate("workRequestId", requestId);

        GetWorkRequestDetails getRequest =
                GetWorkRequestDetails.newBuilder().setWorkRequestId(requestId).build();

        final GetWorkRequestResponse getResponse =
                MetricScope.timeScope(scope, "WorkRequestMds:getRequest",
                        (Function<MetricScope, GetWorkRequestResponse>) innerScope ->
                                WorkRequestBackendHelper.invokeMds(
                                        MdsMetrics.WORK_REQUEST_MDS_BUNDLE, MdsMetrics.WORK_REQUEST_GET,
                                        true,
                                        () -> workRequestClient.execute(c -> getClientWithOptions(c, context)
                                                .getWorkRequest(getRequest))));
        MdsWorkRequest workRequest = getResponse.getWork();
        final String requestBlob = workRequest.getRequestBlob();
        final WorkRequestDetail detail = getWorkRequestDetail(workRequest, requestBlob);
        //Now we know its namespace, compartment, bucket, we check for authZ
        final WSTenantBucketInfo bucketInfo =
                bucketBackend.getBucketMetadata(context, scope, detail.getNamespace(), detail.getBucket());
        WSRequestContext.get(context).setObjectLevelAuditMode(bucketInfo.getObjectLevelAuditMode());
        WSRequestContext.get(context).setBucketOcid(bucketInfo.getOcid());
        if (!MetricScope.timeScope(scope,
                "authorizer:authorize",
                (Function<MetricScope, Boolean>) innerScope ->
                        authorizer.authorize(WSRequestContext.get(context), authInfo,
                                new NamespaceKey(Api.V2, detail.getNamespace()), detail.getBucket(),
                                workRequest.getCompartmentOcid(), null,
                                CasperOperation.GET_WORK_REQUEST, null, true, false,
                                CasperPermission.OBJECT_READ).isPresent())) {
            LOG.info("Request {} unauthorized", requestId);
            throw new NotFoundException("Either the request " + requestId +
                    " was not found or you are not authorized to read it");
        }
        return Pair.pair(workRequest, detail);
    }

    private WorkRequestDetail getWorkRequestDetail(MdsWorkRequest mdsWorkRequest, String requestBlob) {
        Preconditions.checkState(mdsWorkRequest.getType() == MdsWorkRequestType.WR_TYPE_COPY
                || mdsWorkRequest.getType() == MdsWorkRequestType.WR_TYPE_BULK_RESTORE
                || mdsWorkRequest.getType() == MdsWorkRequestType.WR_TYPE_BUCKET_REENCRYPTION);
        final WorkRequestType type = MdsTransformer.mdsWorkRequestTypeToWorkRequestType(
                mdsWorkRequest.getType());
        return jsonSerDe.fromJson(requestBlob,
                WorkRequestJsonHelper.WORK_REQUEST_DETAIL_CLASS.get(type));
    }

    public ListWorkRequestsResponse listRequests(RoutingContext context,
                                                 AuthenticationInfo authInfo,
                                                 WorkRequestType type,
                                                 String compartmentId,
                                                 String bucketName,
                                                 String startAfter,
                                                 int limit) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope scope = rootScope.child("WorkRequestBackend:listRequests")
                .annotate("limit", limit)
                .annotate("startAfter", startAfter);
        final String tenantId = authInfo.getMainPrincipal().getTenantId();

        if (!MetricScope.timeScope(scope, "authorizer:authorize", (Function<MetricScope, Boolean>) (innerScope) ->
                authorizer.authorize(WSRequestContext.get(context), authInfo, null, null, compartmentId, null,
                        CasperOperation.LIST_WORK_REQUESTS, null, true,
                        false, CasperPermission.OBJECT_INSPECT).isPresent())) {
            throw new NoSuchNamespaceException("You do not have authorization to perform this request, " +
                    "or the requested resource could not be found.");
        }

        ListWorkRequestsDetails listRequest = ListWorkRequestsDetails.newBuilder()
                .setTenantOcid(tenantId)
                .setCompartmentOcid(compartmentId)
                .setBucketName(bucketName == null ? "" : bucketName)
                .setType(MdsTransformer.workRequestTypeToMdsWorkRequestType(type))
                .setPageToken(startAfter == null ? "" : startAfter)
                .setPageSize(limit)
                .build();


        return MetricScope.timeScope(scope, "WorkRequestMds:listRequests",
                (Function<MetricScope, ListWorkRequestsResponse>) innerScope ->
                        WorkRequestBackendHelper.invokeMds(
                                MdsMetrics.WORK_REQUEST_MDS_BUNDLE, MdsMetrics.WORK_REQUEST_LIST,
                                true,
                                () -> workRequestClient.execute(c -> getClientWithOptions(c, context)
                                        .listWorkRequests(listRequest)))
        );
    }

    public void cancelRequest(RoutingContext context, AuthenticationInfo authInfo, String requestId) {
        Validator.validateWorkRequestId(requestId);
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope scope = rootScope.child("WorkRequestBackend:cancelRequest")
                .annotate("workRequestId", requestId);

        //first get the request to get its namespace, compartment, bucket for authZ
        final GetWorkRequestDetails getRequest = GetWorkRequestDetails.newBuilder().setWorkRequestId(requestId).build();
        GetWorkRequestResponse getResponse =
                MetricScope.timeScope(scope, "WorkRequestMds:getRequest",
                        (Function<MetricScope, GetWorkRequestResponse>) innerScope ->
                                WorkRequestBackendHelper.invokeMds(
                                        MdsMetrics.WORK_REQUEST_MDS_BUNDLE, MdsMetrics.WORK_REQUEST_GET,
                                        true,
                                        () -> workRequestClient.execute(c -> getClientWithOptions(c, context)
                                                .getWorkRequest(getRequest)))
                );
        final MdsWorkRequest workRequest = getResponse.getWork();
        final String requestBlob = workRequest.getRequestBlob();
        final WorkRequestDetail detail = getWorkRequestDetail(workRequest, requestBlob);
        final WSTenantBucketInfo bucketInfo =
                bucketBackend.getBucketMetadata(context, scope, detail.getNamespace(), detail.getBucket());
        WSRequestContext.get(context).setObjectLevelAuditMode(bucketInfo.getObjectLevelAuditMode());
        WSRequestContext.get(context).setBucketOcid(bucketInfo.getOcid());
        if (!MetricScope.timeScope(scope, "authorizer:authorize",
                (Function<MetricScope, Boolean>) innerScope ->
                        authorizer.authorize(WSRequestContext.get(context), authInfo,
                                new NamespaceKey(Api.V2, detail.getNamespace()), detail.getBucket(),
                                workRequest.getCompartmentOcid(), null, CasperOperation.CANCEL_WORK_REQUEST,
                                null, true, false, CasperPermission.OBJECT_DELETE).isPresent())) {
            LOG.info("Request {} unauthorized", requestId);
            throw new NotFoundException("Either the request " + requestId +
                    " was not found or you are not authorized to read it");
        }

        CancelWorkRequestDetails cancelRequest = CancelWorkRequestDetails.newBuilder()
                .setWorkRequestId(requestId).build();


        //cancel the request
        MetricScope.timeScope(scope, "WorkRequestMds:cancelRequest",
                (Consumer<MetricScope>) innerScope -> WorkRequestBackendHelper.invokeMds(
                        MdsMetrics.WORK_REQUEST_MDS_BUNDLE, MdsMetrics.WORK_REQUEST_CANCEL,
                        false,
                        () -> workRequestClient.execute(c -> getClientWithOptions(c, context)
                                .cancelWorkRequest(cancelRequest)))
        );
    }

    public WsReplicationPolicy createReplicationPolicy(RoutingContext context,
                                                       AuthenticationInfo authInfo,
                                                       String namespace,
                                                       String bucketName,
                                                       String policyName,
                                                       String destRegionString,
                                                       String destBucket) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);
        Validator.validatePolicyName(policyName);
        Validator.validateBucket(destBucket);
        final ConfigRegion destRegion = Validator.validateRegion(destRegionString);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);

        final MetricScope rootScope = commonContext.getMetricScope();
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);

        final MetricScope scope = rootScope.child("WorkRequestBackend:createReplicationPolicy");

        final WSTenantBucketInfo bucket = bucketBackend.getBucketMetadata(
                context, scope, namespace, bucketName);

        // authZ
        final NamespaceKey namespaceKey = bucket.getNamespaceKey();
        if (!MetricScope.timeScopeF(scope, "authorize", (innerScope) ->
                authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        namespaceKey,
                        bucketName,
                        bucket.getCompartment(),
                        bucket.getPublicAccessType(),
                        CasperOperation.CREATE_REPLICATION_POLICY,
                        bucket.getKmsKeyId().orElse(null),
                        true,
                        bucket.isTenancyDeleted(),
                        ReplicationUtils.REPLICATION_PERMISSIONS.toArray(new CasperPermission[0])).isPresent())) {
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketName, namespace));
        }

        if (bucket.isReadOnly() || bucket.isReplicationEnabled()) {
            throw new ReplicationPolicyConflictException(
                    String.format("Replication policy exists for bucket '%s' in namespace '%s'.",
                            bucketName, namespace));
        }

        // Create OBO token
        final String oboToken = createOboToken(context, authInfo, namespace);

        CreateReplicationPolicyDetails.Builder builder = CreateReplicationPolicyDetails.newBuilder()
                .setName(policyName)
                .setSourceNamespace(namespace)
                .setSourceTenantOcid(authInfo.getMainPrincipal().getTenantId())
                .setSourceBucketId(bucket.getOcid())
                .setSourceBucketName(bucketName)
                .setDestRegion(destRegion.toString())
                .setDestBucketName(destBucket)
                .setOboToken(oboToken);

        CreateReplicationPolicyResponse response = MetricScope.timeScopeF(scope,
                "WorkRequestMds:createReplicationPolicy",
                innerScope -> WorkRequestBackendHelper.invokeMds(MdsMetrics.WORK_REQUEST_MDS_BUNDLE,
                        MdsMetrics.REPLICATION_POLICY_CREATE,
                        true,
                        () -> workRequestClient.execute(c -> getClientWithOptions(c, context)
                                .createReplicationPolicy(builder.build()))));

        return MdsTransformer.fromMdsReplicationPolicy(response.getReplicationPolicy(), bucket);
    }

    public WsReplicationPolicy getReplicationPolicy(RoutingContext context,
                                                    AuthenticationInfo authInfo,
                                                    String namespace,
                                                    String bucketName,
                                                    String policyId,
                                                    CasperOperation operation) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);
        Validator.validateReplicationPolicyId(policyId);

        final MetricScope rootScope = WSRequestContext.getMetricScope(context);

        if (operation == CasperOperation.GET_REPLICATION_POLICY) {
            HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);
        }

        final MetricScope scope = rootScope.child("WorkRequestBackend:getReplicationPolicy");

        final WSTenantBucketInfo bucket = bucketBackend.getBucketMetadata(context, scope, namespace, bucketName);

        if (operation == CasperOperation.GET_REPLICATION_POLICY) {
            // authZ
            final NamespaceKey namespaceKey = bucket.getNamespaceKey();
            if (!MetricScope.timeScopeF(scope, "authorize", (innerScope) ->
                    authorizer.authorize(
                            WSRequestContext.get(context),
                            authInfo,
                            namespaceKey,
                            bucketName,
                            bucket.getCompartment(),
                            bucket.getPublicAccessType(),
                            operation,
                            bucket.getKmsKeyId().orElse(null),
                            true,
                            bucket.isTenancyDeleted(),
                            ReplicationUtils.REPLICATION_PERMISSIONS.toArray(new CasperPermission[0])).isPresent())) {
                throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketName, namespace));
            }
        }

        final GetReplicationPolicyDetails request = GetReplicationPolicyDetails.newBuilder()
                .setPolicyId(policyId)
                .build();

        GetReplicationPolicyResponse response =
                MetricScope.timeScopeF(scope, "WorkRequestMds:getReplicationPolicy",
                        innerScope -> WorkRequestBackendHelper.invokeMds(MdsMetrics.WORK_REQUEST_MDS_BUNDLE,
                                MdsMetrics.REPLICATION_POLICY_GET,
                                true,
                                () -> workRequestClient.execute(c ->
                                        getClientWithOptions(c, context).getReplicationPolicy(request))));


        return MdsTransformer.fromMdsReplicationPolicy(response.getReplicationPolicy(), bucket);
    }

    public void deleteReplicationPolicy(RoutingContext context,
                                        AuthenticationInfo authInfo,
                                        String namespace,
                                        String bucketName,
                                        String policyId) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);
        Validator.validateReplicationPolicyId(policyId);

        final MetricScope rootScope = WSRequestContext.getMetricScope(context);
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);
        final MetricScope scope = rootScope.child("WorkRequestBackend:deleteReplicationPolicy");

        final WSTenantBucketInfo bucket = bucketBackend.getBucketMetadata(context, scope, namespace, bucketName);
        final NamespaceKey namespaceKey = bucket.getNamespaceKey();
        if (!MetricScope.timeScopeF(scope, "authorize", (innerScope) ->
                authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        namespaceKey,
                        bucketName,
                        bucket.getCompartment(),
                        bucket.getPublicAccessType(),
                        CasperOperation.DELETE_REPLICATION_POLICY,
                        bucket.getKmsKeyId().orElse(null),
                        true,
                        bucket.isTenancyDeleted(),
                        ReplicationUtils.REPLICATION_PERMISSIONS.toArray(new CasperPermission[0])).isPresent())) {
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketName, namespace));
        }

        // Create OBO token
        final String oboToken = createOboToken(context, authInfo, namespace);

        final DeleteReplicationPolicyDetails request = DeleteReplicationPolicyDetails.newBuilder()
                .setPolicyId(policyId)
                .setOboToken(oboToken)
                .build();

        MetricScope.timeScopeF(scope, "WorkRequestMds:deleteReplicationPolicy",
                innerScope -> WorkRequestBackendHelper.invokeMds(MdsMetrics.WORK_REQUEST_MDS_BUNDLE,
                        MdsMetrics.REPLICATION_POLICY_DELETE,
                        true,
                        () -> workRequestClient.execute(c -> getClientWithOptions(c, context).deleteReplicationPolicy(
                                request))));
    }

    public WsReplicationPolicyList listReplicationPolicies(RoutingContext context,
                                                           AuthenticationInfo authInfo,
                                                           String namespace,
                                                           String bucketName,
                                                           int pageSize,
                                                           @Nullable String startAfter) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);
        Preconditions.checkArgument(pageSize > 0, String.format("Invalid value for page size: %d", pageSize));

        final MetricScope rootScope = WSRequestContext.getMetricScope(context);
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);
        final MetricScope scope = rootScope.child("WorkRequestBackend:listReplicationPolicies");

        // authZ
        final WSTenantBucketInfo bucket = bucketBackend.getBucketMetadata(context, scope, namespace, bucketName);
        final NamespaceKey namespaceKey = bucket.getNamespaceKey();
        if (!MetricScope.timeScopeF(scope, "authorize", (innerScope) ->
                authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        namespaceKey,
                        bucketName,
                        bucket.getCompartment(),
                        bucket.getPublicAccessType(),
                        CasperOperation.LIST_REPLICATION_POLICIES,
                        bucket.getKmsKeyId().orElse(null),
                        true,
                        bucket.isTenancyDeleted(),
                        ReplicationUtils.REPLICATION_PERMISSIONS.toArray(new CasperPermission[0])).isPresent())) {
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketName, namespace));
        }

        final ListReplicationPoliciesDetails.Builder builder = ListReplicationPoliciesDetails.newBuilder()
                .setSourceNamespace(namespace).setSourceBucketName(bucketName).setPageSize(pageSize);

        if (startAfter != null) {
            builder.setStartAfter(startAfter);
        }

        ListReplicationPoliciesResponse response =
                MetricScope.timeScopeF(scope, "WorkRequestMds:listReplicationPolicies",
                        innerScope -> WorkRequestBackendHelper.invokeMds(MdsMetrics.WORK_REQUEST_MDS_BUNDLE,
                                MdsMetrics.REPLICATION_POLICY_LIST,
                                true,
                                () -> workRequestClient.execute(c -> getClientWithOptions(c, context)
                                        .listReplicationPolicies(builder.build()))));
        List<WsReplicationPolicy> policyList = MdsTransformer
                .fromMdsReplicationPolicies(response.getPoliciesList(), bucket);
        WsReplicationPolicyList result = new WsReplicationPolicyList();
        result.setReplicationPolicyList(policyList);

        return result;
    }

    //currently there is only ever 1, so skipping over list logics
    public ReplicationSource listReplicationSources(RoutingContext context,
                                                    AuthenticationInfo authInfo,
                                                    String namespace,
                                                    String bucketName,
                                                    int limit,
                                                    String page) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);

        final MetricScope rootScope = WSRequestContext.getMetricScope(context);
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);
        final MetricScope scope = rootScope.child("WorkRequestBackend:listReplicationSources");

        // authZ
        final WSTenantBucketInfo bucket = bucketBackend.getBucketMetadata(context, scope, namespace, bucketName);
        final NamespaceKey namespaceKey = bucket.getNamespaceKey();
        if (!MetricScope.timeScopeF(scope, "authorize", (innerScope) ->
            authorizer.authorize(
                WSRequestContext.get(context),
                authInfo,
                namespaceKey,
                bucketName,
                bucket.getCompartment(),
                bucket.getPublicAccessType(),
                CasperOperation.LIST_REPLICATION_SOURCES,
                bucket.getKmsKeyId().orElse(null),
                true,
                bucket.isTenancyDeleted(),
                ReplicationUtils.REPLICATION_PERMISSIONS.toArray(new CasperPermission[0])).isPresent())) {
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketName, namespace));
        }

        return bucket.isReadOnly() ?
            ReplicationSource.builder()
                .policyName(bucket.getOptions().get(ReplicationOptions.XRR_POLICY_NAME).toString())
                .sourceRegionName(bucket.getOptions().get(ReplicationOptions.XRR_SOURCE_REGION).toString())
                .sourceBucketName(bucket.getOptions().get(ReplicationOptions.XRR_SOURCE_BUCKET).toString())
                .build() : null;
    }
}
