package com.oracle.pic.casper.webserver.api.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.oracle.bmc.objectstorage.model.ReplicationPolicy;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.encryption.EncryptionKey;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.model.BucketMeterFlag;
import com.oracle.pic.casper.common.model.BucketMeterFlagSet;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.common.model.BucketStorageTier;
import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;
import com.oracle.pic.casper.common.model.ObjectStorageTier;
import com.oracle.pic.casper.mds.MdsBound;
import com.oracle.pic.casper.mds.MdsBoundType;
import com.oracle.pic.casper.mds.MdsBucketKey;
import com.oracle.pic.casper.mds.MdsBucketToken;
import com.oracle.pic.casper.mds.MdsEncryptionKey;
import com.oracle.pic.casper.mds.MdsNamespaceApi;
import com.oracle.pic.casper.mds.MdsObjectKey;
import com.oracle.pic.casper.mds.MdsRange;
import com.oracle.pic.casper.mds.MdsSortOrder;
import com.oracle.pic.casper.mds.MdsTenantBucketShardInfo;
import com.oracle.pic.casper.mds.MdsTenantBucketShardRangeMapEntry;
import com.oracle.pic.casper.mds.common.grpc.TimestampUtils;
import com.oracle.pic.casper.mds.object.GetStatForBucketResponse;
import com.oracle.pic.casper.mds.object.MdsArchivalState;
import com.oracle.pic.casper.mds.object.MdsChecksumType;
import com.oracle.pic.casper.mds.object.MdsChunk;
import com.oracle.pic.casper.mds.object.MdsDigest;
import com.oracle.pic.casper.mds.object.MdsDigestAlgorithm;
import com.oracle.pic.casper.mds.object.MdsEtagType;
import com.oracle.pic.casper.mds.object.MdsObjectStorageTier;
import com.oracle.pic.casper.mds.tenant.MdsBucketMeterFlag;
import com.oracle.pic.casper.mds.tenant.MdsBucketPublicAccessType;
import com.oracle.pic.casper.mds.tenant.MdsBucketStorageTier;
import com.oracle.pic.casper.mds.tenant.MdsBucketsSortBy;
import com.oracle.pic.casper.mds.tenant.MdsObjectLevelAuditMode;
import com.oracle.pic.casper.mds.workrequest.MdsReplicationPolicy;
import com.oracle.pic.casper.mds.workrequest.MdsReplicationStatus;
import com.oracle.pic.casper.mds.workrequest.MdsWorkRequest;
import com.oracle.pic.casper.mds.workrequest.MdsWorkRequestError;
import com.oracle.pic.casper.mds.workrequest.MdsWorkRequestLog;
import com.oracle.pic.casper.mds.workrequest.MdsWorkRequestState;
import com.oracle.pic.casper.mds.workrequest.MdsWorkRequestStatus;
import com.oracle.pic.casper.mds.workrequest.MdsWorkRequestType;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.BucketKey;
import com.oracle.pic.casper.objectmeta.ChecksumType;
import com.oracle.pic.casper.objectmeta.ETagType;
import com.oracle.pic.casper.objectmeta.ObjectDbStats;
import com.oracle.pic.casper.objectmeta.ObjectKey;
import com.oracle.pic.casper.objectmeta.StorageObjectChunk;
import com.oracle.pic.casper.objectmeta.TenantDb;
import com.oracle.pic.casper.objectmeta.input.BucketSortBy;
import com.oracle.pic.casper.objectmeta.input.BucketSortOrder;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;
import com.oracle.pic.casper.webserver.api.model.WorkRequestJson;
import com.oracle.pic.casper.webserver.api.model.WorkRequestJsonHelper;
import com.oracle.pic.casper.webserver.api.model.WsReplicationPolicy;
import com.oracle.pic.casper.workrequest.WorkRequest;
import com.oracle.pic.casper.workrequest.WorkRequestDetail;
import com.oracle.pic.casper.workrequest.WorkRequestError;
import com.oracle.pic.casper.workrequest.WorkRequestLog;
import com.oracle.pic.casper.workrequest.WorkRequestState;
import com.oracle.pic.casper.workrequest.WorkRequestStatus;
import com.oracle.pic.casper.workrequest.WorkRequestType;

import java.math.BigInteger;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class MdsTransformer {


    private MdsTransformer() {
    }

    public static MdsNamespaceApi toMdsNamespaceApi(Api api) {
        switch (api) {
            case V2:
                return MdsNamespaceApi.MDS_NS_API_V2;
            case V1:
                return MdsNamespaceApi.MDS_NS_API_V1;
            default:
                throw new IllegalStateException("Unexpected Api " + api);
        }
    }

    public static Api toApi(MdsNamespaceApi mdsNamespaceApi) {
        switch (mdsNamespaceApi) {
            case MDS_NS_API_V1:
                return Api.V1;
            case MDS_NS_API_V2:
                return Api.V2;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for MDS namespace api: %s", mdsNamespaceApi));
        }
    }

    public static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    public static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

    public static MdsBucketPublicAccessType toPublicAccessType(BucketPublicAccessType bucketPublicAccessType) {
        switch (bucketPublicAccessType) {
            case NoPublicAccess:
                return MdsBucketPublicAccessType.BPA_NO_PUBLIC_ACCESS;
            case ObjectRead:
                return MdsBucketPublicAccessType.BPA_OBJECT_READ;
            case ObjectReadWithoutList:
                return MdsBucketPublicAccessType.BPA_OBJECT_READ_WITHOUT_LIST;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for bucket public access type %s", bucketPublicAccessType));
        }
    }

    public static BucketPublicAccessType toPublicAccessType(MdsBucketPublicAccessType mdsBucketPublicAccessType) {
        switch (mdsBucketPublicAccessType) {
            case BPA_NO_PUBLIC_ACCESS:
                return BucketPublicAccessType.NoPublicAccess;
            case BPA_OBJECT_READ:
                return BucketPublicAccessType.ObjectRead;
            case BPA_OBJECT_READ_WITHOUT_LIST:
                return BucketPublicAccessType.ObjectReadWithoutList;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for MDS bucket public access type: %s", mdsBucketPublicAccessType));
        }
    }

    public static MdsBucketStorageTier toStorageTier(BucketStorageTier bucketStorageTier) {
        switch (bucketStorageTier) {
            case Archive:
                return MdsBucketStorageTier.BST_ARCHIVE;
            case Standard:
                return MdsBucketStorageTier.BST_STANDARD;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for bucket storage tier: %s", bucketStorageTier));
        }
    }

    public static BucketStorageTier toStorageTier(MdsBucketStorageTier mdsBucketStorageTier) {
        switch (mdsBucketStorageTier) {
            case BST_ARCHIVE:
                return BucketStorageTier.Archive;
            case BST_STANDARD:
                return BucketStorageTier.Standard;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for MDS bucket storage tier: %s", mdsBucketStorageTier));
        }
    }

    public static MdsObjectStorageTier toObjectStorageTier(ObjectStorageTier objectStorageTier) {
        switch (objectStorageTier) {
            case ARCHIVED:
                return MdsObjectStorageTier.OST_ARCHIVED;
            case STANDARD:
                return MdsObjectStorageTier.OST_STANDARD;
            case NOT_APPLICABLE:
                return MdsObjectStorageTier.OST_NOT_APPLICABLE;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for MDS object storage tier: %s", objectStorageTier));

        }
    }

    public static ObjectStorageTier toObjectStorageTier(MdsObjectStorageTier mdsObjectStorageTier) {
        switch (mdsObjectStorageTier) {
            case OST_STANDARD:
                return ObjectStorageTier.STANDARD;
            case OST_ARCHIVED:
                return ObjectStorageTier.ARCHIVED;
            case OST_NOT_APPLICABLE:
                return ObjectStorageTier.NOT_APPLICABLE;
            default:
                throw new IllegalStateException(String.format("No mapping for ObjectStorageTier: %s " +
                        "in MDS to ObjectStorageTier", mdsObjectStorageTier.getValueDescriptor().toString()));
        }
    }

    public static MdsObjectLevelAuditMode toAuditMode(ObjectLevelAuditMode objectLevelAuditMode) {
        switch (objectLevelAuditMode) {
            case Disabled:
                return MdsObjectLevelAuditMode.OLAM_DISABLED;
            case ReadWrite:
                return MdsObjectLevelAuditMode.OLAM_READ_WRITE;
            case Write:
                return MdsObjectLevelAuditMode.OLAM_WRITE;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for object level audit mode: %s", objectLevelAuditMode));
        }
    }

    public static ObjectLevelAuditMode toAuditMode(MdsObjectLevelAuditMode mdsObjectLevelAuditMode) {
        switch (mdsObjectLevelAuditMode) {
            case OLAM_DISABLED:
                return ObjectLevelAuditMode.Disabled;
            case OLAM_READ_WRITE:
                return ObjectLevelAuditMode.ReadWrite;
            case OLAM_WRITE:
                return ObjectLevelAuditMode.Write;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for MDS object level audit mode: %s", mdsObjectLevelAuditMode));
        }
    }

    public static MdsBucketsSortBy toMdsSortBy(BucketSortBy bucketSortBy) {
        switch (bucketSortBy) {
            case TIMECREATED:
                return MdsBucketsSortBy.BS_BY_CREATION;
            case NAME:
                return MdsBucketsSortBy.BS_BY_NAME;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for bucket sort by: %s", bucketSortBy));
        }
    }

    public static MdsSortOrder toMdsSortOrder(BucketSortOrder bucketSortOrder) {
        switch (bucketSortOrder) {
            case ASC:
                return MdsSortOrder.MDS_SORT_ASCENDING;
            case DESC:
                return MdsSortOrder.MDS_SORT_DESCENDING;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for bucket sort order: %s", bucketSortOrder));
        }
    }

    private static BucketMeterFlag toBucketMeterFlag(MdsBucketMeterFlag mdsBucketMeterFlag) {
        switch (mdsBucketMeterFlag) {
            case BMF_RMAN:
                return BucketMeterFlag.Rman;
            case BMF_STORAGE_GATEWAY:
                return BucketMeterFlag.StorageGateway;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for MDS bucket meter flag: %s", mdsBucketMeterFlag));
        }
    }

    public static MdsDigest toMdsDigest(Digest digest) {
        return MdsDigest.newBuilder()
                .setValue(ByteString.copyFrom(digest.getValue()))
                .setAlgorithm(toMdsDigestAlgorithm(digest.getAlgorithm()))
                .build();
    }

    private static MdsDigestAlgorithm toMdsDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        switch (digestAlgorithm) {
            case MD5:
                return MdsDigestAlgorithm.DA_MD5;
            case SHA256:
                return MdsDigestAlgorithm.DA_SHA256;
            case NONE:
                return MdsDigestAlgorithm.DA_NONE;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for digest algorithm: %s", digestAlgorithm));
        }
    }

    public static BucketMeterFlagSet toBucketMeterFlagSet(List<MdsBucketMeterFlag> mdsBucketMeterFlagList) {
        final BucketMeterFlagSet bucketMeterFlagSet = new BucketMeterFlagSet();
        for (MdsBucketMeterFlag mdsBucketMeterFlag : mdsBucketMeterFlagList) {
            bucketMeterFlagSet.add(toBucketMeterFlag(mdsBucketMeterFlag));
        }
        return bucketMeterFlagSet;
    }

    private static MdsBucketMeterFlag toBucketMeterFlag(BucketMeterFlag bucketMeterFlag) {
        switch (bucketMeterFlag) {
            case Rman:
                return MdsBucketMeterFlag.BMF_RMAN;
            case StorageGateway:
                return MdsBucketMeterFlag.BMF_STORAGE_GATEWAY;
            default:
                throw new IllegalStateException(
                        String.format("No mapping for bucket meter flag: %s", bucketMeterFlag));
        }
    }

    public static List<MdsBucketMeterFlag> toBucketMeterFlagSet(BucketMeterFlagSet bucketMeterFlagSet) {
        final ImmutableList.Builder<MdsBucketMeterFlag> builder = new ImmutableList.Builder<>();
        Iterator<BucketMeterFlag> iterator = bucketMeterFlagSet.iterator();
        while (iterator.hasNext()) {
            builder.add(toBucketMeterFlag(iterator.next()));
        }
        return builder.build();
    }

    public static ChecksumType toChecksumType(MdsChecksumType mdsChecksumType) {
        switch (mdsChecksumType) {
            case CT_MD5:
                return ChecksumType.MD5;
            case CT_MD5_OF_MD5:
                return  ChecksumType.MD5_OF_MD5;
            default:
                throw new IllegalStateException(String.format("No mapping for MdsChecksumType: %s " +
                        "in MDS to ChecksumType", mdsChecksumType.getValueDescriptor().toString()));
        }
    }

    public static ArchivalState toArchivalState(MdsArchivalState mdsArchivalState) {
        switch (mdsArchivalState) {
            case AS_RESTORING:
                return ArchivalState.Restoring;
            case AS_RESTORED:
                return ArchivalState.Restored;
            case AS_NOT_APPLICABLE:
                return ArchivalState.NotApplicable;
            case AS_AVAILABLE:
                return ArchivalState.Available;
            case AS_ARCHIVED:
                return ArchivalState.Archived;
            default:
                throw new IllegalStateException(String.format("No mapping for ArchivalState: %s " +
                        "in MDS to ArchivalState", mdsArchivalState.getValueDescriptor().toString()));
        }
    }

    private static DigestAlgorithm toDigestAlgorithm(MdsDigestAlgorithm mdsDigestAlgorithm) {
        switch (mdsDigestAlgorithm) {
            case DA_NONE:
                return DigestAlgorithm.NONE;
            case DA_MD5:
                return DigestAlgorithm.MD5;
            case DA_SHA256:
                return DigestAlgorithm.SHA256;
            default:
                throw new IllegalStateException(String.format("No mapping for DigestAlgorithm: %s " +
                        "in MDS to DigestAlgorithm", mdsDigestAlgorithm.getValueDescriptor().toString()));
        }
    }

    public static Digest toDigest(MdsDigest mdsDigest) {
        return new Digest(toDigestAlgorithm(mdsDigest.getAlgorithm()), mdsDigest.getValue().toByteArray());
    }

    public static MdsBucketKey toMdsBucketKey(BucketKey bucketKey) {
        return MdsBucketKey.newBuilder()
                .setBucketName(bucketKey.getName())
                .setNamespaceName(bucketKey.getNamespace())
                .setApi(toMdsNamespaceApi(bucketKey.getApi()))
                .build();
    }

    public static BucketKey toBucketKey(MdsBucketKey mdsBucketKey) {
        return new BucketKey(
                mdsBucketKey.getNamespaceName(),
                mdsBucketKey.getBucketName(),
                toApi(mdsBucketKey.getApi()));
    }

    public static MdsEncryptionKey toMdsEncryptionKey(EncryptionKey encryptionKey) {
        MdsEncryptionKey.Builder builder = MdsEncryptionKey.newBuilder();
        if (encryptionKey.getEncryptedDataEncryptionKey() != null) {
            builder.setKey(ByteString.copyFrom(encryptionKey.getEncryptedDataEncryptionKey()));
        }
        if (encryptionKey.getVersion() != null) {
            builder.setVersion(encryptionKey.getVersion());
        }
        return builder.build();
    }

    public static MdsBucketToken toMdsBucketToken(MdsTenantBucketShardInfo mdsTenantBucketShardInfo) {
        return MdsBucketToken.newBuilder()
                .addRangeMapEntries(MdsTenantBucketShardRangeMapEntry.newBuilder()
                        .setRange(MdsRange.newBuilder()
                                .setLowerBound(MdsBound.newBuilder()
                                        .setBound(0)
                                        .setType(MdsBoundType.MDS_BOUND_TYPE_INCLUSIVE)
                                        .build())
                                .setUpperBound(MdsBound.newBuilder()
                                        .setBound(TenantDb.BUCKET_SHARD_HASH_MOD_VALUE)
                                        .setType(MdsBoundType.MDS_BOUND_TYPE_INCLUSIVE)
                                        .build())
                                .build())
                        .setShardInfo(mdsTenantBucketShardInfo)
                        .build())
                .build();
    }

    public static ObjectKey toObjectKey(MdsObjectKey mdsObjectKey) {
        return new ObjectKey(toBucketKey(mdsObjectKey.getBucketKey()), mdsObjectKey.getObjectName());
    }

    private static Map<Long, StorageObjectChunk> toChunks(Map<Long, MdsChunk> mdsChunkMap) {
        Map<Long, StorageObjectChunk> chunkMap = new HashMap<>();
        for (Map.Entry<Long, MdsChunk> entry : mdsChunkMap.entrySet()) {
            chunkMap.put(entry.getKey(), toChunk(entry.getValue()));
        }
        return chunkMap;
    }

    private static StorageObjectChunk toChunk(MdsChunk mdsChunk) {
        // deal with nullables
        byte[] payload = mdsChunk.getEncryptionKey().getKey().isEmpty() ? null
                : mdsChunk.getEncryptionKey().getKey().toByteArray();
        String encryptKeyVersion = mdsChunk.getEncryptionKey().getVersion().isEmpty() ? null
                : mdsChunk.getEncryptionKey().getVersion();

        return new StorageObjectChunk(
                mdsChunk.getSizeInBytes(),
                toDigest(mdsChunk.getDigest()),
                mdsChunk.getVolumeId(),
                mdsChunk.getVolumeObjectNumber(),
                payload,
                encryptKeyVersion);
    }

    public static WorkRequestState mdsWorkRequestStateToWorkRequestState(MdsWorkRequestState mdsWorkRequestState) {
        switch (mdsWorkRequestState) {
            case WR_STATE_ACCEPTED:
                return WorkRequestState.ACCEPTED;
            case WR_STATE_IN_PROGRESS:
                return WorkRequestState.IN_PROGRESS;
            case WR_STATE_CANCELING:
                return WorkRequestState.CANCELING;
            case WR_STATE_CANCELED:
                return WorkRequestState.CANCELED;
            case WR_STATE_COMPLETED:
                return WorkRequestState.COMPLETED;
            case WR_STATE_FAILED:
                return WorkRequestState.FAILED;
            default:
                throw new IllegalStateException("Unexpected work request state " + mdsWorkRequestState);
        }
    }

    public static MdsWorkRequestType workRequestTypeToMdsWorkRequestType(WorkRequestType workRequestType) {
        switch (workRequestType) {
            case COPY:
                return MdsWorkRequestType.WR_TYPE_COPY;
            case BCKT_MIG:
                return MdsWorkRequestType.WR_TYPE_BUCKET_MIGRATION;
            case BULK_RESTORE:
                return MdsWorkRequestType.WR_TYPE_BULK_RESTORE;
            case REENCRYPT:
                return MdsWorkRequestType.WR_TYPE_BUCKET_REENCRYPTION;
            default:
                throw new IllegalStateException("Unexpected work request type " + workRequestType);
        }
    }

    public static WorkRequestType mdsWorkRequestTypeToWorkRequestType(MdsWorkRequestType mdsWorkRequestType) {
        switch (mdsWorkRequestType) {
            case WR_TYPE_COPY:
                return WorkRequestType.COPY;
            case WR_TYPE_BUCKET_MIGRATION:
                return WorkRequestType.BCKT_MIG;
            case WR_TYPE_BULK_RESTORE:
                return WorkRequestType.BULK_RESTORE;
            case WR_TYPE_BUCKET_REENCRYPTION:
                return WorkRequestType.REENCRYPT;
            default:
                throw new IllegalStateException("Unexpected mds work request type " + mdsWorkRequestType);
        }
    }

    public static WorkRequest mdsWorkRequestToWorkRequest(MdsWorkRequest mdsWorkRequest) {
        final WorkRequestStatus status = mdsWorkRequestStatusToWorkRequestStatus(mdsWorkRequest);

        final WorkRequest workRequest = new WorkRequest(mdsWorkRequest.getWorkRequestId(),
            mdsWorkRequestTypeToWorkRequestType(mdsWorkRequest.getType()),
            mdsWorkRequestStateToWorkRequestState(mdsWorkRequest.getState()),
            mdsWorkRequest.getRequestBlob(),
            mdsWorkRequest.getTenantOcid(),
            mdsWorkRequest.getCompartmentOcid(),
            mdsWorkRequest.getBucketName(),
            Instant.ofEpochSecond(mdsWorkRequest.getTimeAccepted().getSeconds()),
            mdsWorkRequest.hasTimeStarted() ?
                Instant.ofEpochSecond(mdsWorkRequest.getTimeStarted().getSeconds()) : null,
            mdsWorkRequest.hasTimeLastModified() ?
                Instant.ofEpochSecond(mdsWorkRequest.getTimeLastModified().getSeconds()) : null,
            status,
            mdsWorkRequest.getSize(),
            mdsWorkRequest.getFailedCount());
        return workRequest;
    }

    public static WorkRequestStatus mdsWorkRequestStatusToWorkRequestStatus(MdsWorkRequest mdsWorkRequest) {
        WorkRequestStatus status;
        final MdsWorkRequestStatus mdsWorkRequestStatus = mdsWorkRequest.getStatus();
        // The work request MDS in-memory implementation can return a null MdsWorkRequestStatus.
        if (mdsWorkRequestStatus == null) {
            status = new WorkRequestStatus(0, ImmutableList.of(), ImmutableList.of());
        } else {
            ImmutableList.Builder<WorkRequestLog> logBuilder = new ImmutableList.Builder<>();
            for (MdsWorkRequestLog mdsWorkRequestLog : mdsWorkRequestStatus.getLogsList()) {
                logBuilder.add(new WorkRequestLog(toInstant(mdsWorkRequestLog.getTimeStamp()),
                        mdsWorkRequestLog.getMessage()));
            }
            final List<WorkRequestLog> workRequestLogs = logBuilder.build().asList();
            ImmutableList.Builder<WorkRequestError> errorBuilder = new ImmutableList.Builder<>();
            for (MdsWorkRequestError mdsWorkRequestError : mdsWorkRequestStatus.getErrorsList()) {
                errorBuilder.add(new WorkRequestError(toInstant(mdsWorkRequestError.getTimeStamp()),
                        mdsWorkRequestError.getMessage(),
                        mdsWorkRequestError.getCode()));
            }
            final List<WorkRequestError> workRequestErrors = errorBuilder.build().asList();
            status = new WorkRequestStatus(
                    mdsWorkRequestStatus.getPercentComplete(), workRequestLogs, workRequestErrors);
        }
        return status;
    }

    public static WorkRequestJson createWorkRequestJson(MdsWorkRequest mdsWorkRequest,
                                                        WorkRequestDetail detail) {
        Preconditions.checkState(mdsWorkRequest.getType() == MdsWorkRequestType.WR_TYPE_COPY
                || mdsWorkRequest.getType() == MdsWorkRequestType.WR_TYPE_BULK_RESTORE
                || mdsWorkRequest.getType() == MdsWorkRequestType.WR_TYPE_BUCKET_REENCRYPTION);
        final WorkRequest workRequest = mdsWorkRequestToWorkRequest(mdsWorkRequest);
        return new WorkRequestJson(
            workRequest.getType().getName(),
            workRequest.getState().name(),
            workRequest.getRequestId(),
            workRequest.getCompartmentId(),
            WorkRequestJsonHelper.workRequestResourcesFromDetail(detail, workRequest),
            workRequest.getStatus().getPercentComplete(),
            workRequest.getTimeAccepted(),
            workRequest.getTimeStarted(),
            workRequest.getState().isEndState() ? workRequest.getTimeLastModified() : null);
    }

    public static WsReplicationPolicy fromMdsReplicationPolicy(MdsReplicationPolicy mdsReplicationPolicy,
                                                         WSTenantBucketInfo bucket) {
        WsReplicationPolicy result = new WsReplicationPolicy();

        result.setPolicyId(mdsReplicationPolicy.getPolicyId());
        result.setPolicyName(mdsReplicationPolicy.getPolicyName());
        result.setStatus(fromMdsReplicationStatus(mdsReplicationPolicy.getStatus()));
        result.setStatusMessage(mdsReplicationPolicy.getStatusMessage());
        result.setSourceBucketId(bucket.getOcid());
        result.setEnabled(mdsReplicationPolicy.getEnabled());
        result.setDestBucketName(mdsReplicationPolicy.getDestBucketName());
        result.setDestRegion(ConfigRegion.valueOf(mdsReplicationPolicy.getDestRegion()).getFullName());
        result.setSourceNamespace(mdsReplicationPolicy.getSourceNamespace());
        result.setSourceBucketName(mdsReplicationPolicy.getSourceBucketName());
        result.setTimeCreated(Instant.ofEpochSecond(mdsReplicationPolicy.getTimeCreated().getSeconds()));
        if (mdsReplicationPolicy.getTimeLastSync() != null) {
            result.setTimeLastSync(Instant.ofEpochSecond(mdsReplicationPolicy.getTimeLastSync().getSeconds()));
        }
        return result;
    }

    public static final Predicate<MdsReplicationPolicy> IS_POLICY_ACTIVE_OR_CLIENT_ERROR =
            (policy) -> policy.getStatus() == MdsReplicationStatus.RP_STATUS_CLIENT_ERROR ||
                    policy.getStatus() == MdsReplicationStatus.RP_STATUS_ACTIVE;

    public static List<WsReplicationPolicy> fromMdsReplicationPolicies(
            List<MdsReplicationPolicy> policies, WSTenantBucketInfo bucket) {
        return policies.stream()
                .filter(IS_POLICY_ACTIVE_OR_CLIENT_ERROR)
                .map(policy -> fromMdsReplicationPolicy(policy, bucket))
                .collect(Collectors.toList());
    }

    private static ReplicationPolicy.Status fromMdsReplicationStatus(MdsReplicationStatus mdsReplicationStatus) {
        switch (mdsReplicationStatus) {
            case RP_STATUS_ACTIVE:
                return ReplicationPolicy.Status.Active;
            case RP_STATUS_CLIENT_ERROR:
                return ReplicationPolicy.Status.ClientError;
            default:
                return null;
        }
    }


    public static MdsEtagType toEtagType(ETagType eTagType) {
        switch (eTagType) {
            case MD5:
                return MdsEtagType.ET_MD5;
            case ETAG:
                return MdsEtagType.ET_ETAG;
            default:
                throw new IllegalStateException(String.format("No mapping for EtagType: %s " +
                        "to MdsEtagType", eTagType));
        }
    }

    public static ObjectDbStats toObjectDbStats(GetStatForBucketResponse response) {
        return new ObjectDbStats(TimestampUtils.toInstant(response.getStatTime()), response.getNumOfObjects(),
                new BigInteger(response.getObjectStorageSize().toByteArray()),
                new BigInteger(response.getPartSize().toByteArray()),
                 0);
    }
}
