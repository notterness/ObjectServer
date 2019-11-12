package com.oracle.pic.casper.webserver.api.backend;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.protobuf.ByteString;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.model.ScanResults;
import com.oracle.pic.casper.mds.MdsBucketToken;
import com.oracle.pic.casper.mds.common.grpc.TimestampUtils;
import com.oracle.pic.casper.mds.object.ListBasicObjectSummary;
import com.oracle.pic.casper.mds.object.ListBasicResponse;
import com.oracle.pic.casper.mds.object.ListNameOnlyResponse;
import com.oracle.pic.casper.mds.object.ListObjectsResponse;
import com.oracle.pic.casper.mds.object.ListS3ObjectSummary;
import com.oracle.pic.casper.mds.object.ListS3Response;
import com.oracle.pic.casper.mds.object.MdsChunk;
import com.oracle.pic.casper.mds.object.MdsObject;
import com.oracle.pic.casper.mds.object.MdsObjectSummary;
import com.oracle.pic.casper.mds.object.MdsPartInfo;
import com.oracle.pic.casper.mds.object.MdsUploadInfo;
import com.oracle.pic.casper.mds.object.transformers.BucketTokenTransformers;
import com.oracle.pic.casper.mds.tenant.MdsBucket;
import com.oracle.pic.casper.mds.tenant.MdsBucketSummary;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.objectmeta.BucketToken;
import com.oracle.pic.casper.objectmeta.ChecksumType;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.objectmeta.ObjectId;
import com.oracle.pic.casper.objectmeta.ObjectKey;
import com.oracle.pic.casper.objectmeta.PartETag;
import com.oracle.pic.casper.objectmeta.PartKey;
import com.oracle.pic.casper.objectmeta.StorageObject;
import com.oracle.pic.casper.objectmeta.StorageObjectChunk;
import com.oracle.pic.casper.objectmeta.StorageObjectSummary;
import com.oracle.pic.casper.objectmeta.StoragePartInfo;
import com.oracle.pic.casper.objectmeta.model.TenantBucketShardInfo;
import com.oracle.pic.casper.webserver.api.common.Checksum;
import com.oracle.pic.casper.webserver.api.common.MdsTransformer;
import com.oracle.pic.casper.webserver.api.model.Bucket;
import com.oracle.pic.casper.webserver.api.model.BucketSummary;
import com.oracle.pic.casper.webserver.api.model.FinishUploadRequest;
import com.oracle.pic.casper.webserver.api.model.ListResponse;
import com.oracle.pic.casper.webserver.api.model.NamespaceMetadata;
import com.oracle.pic.casper.webserver.api.model.NamespaceSummary;
import com.oracle.pic.casper.webserver.api.model.ObjectBaseSummary;
import com.oracle.pic.casper.webserver.api.model.ObjectBasicSummary;
import com.oracle.pic.casper.webserver.api.model.ObjectFullSummary;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.ObjectNameOnlySummary;
import com.oracle.pic.casper.webserver.api.model.ObjectS3Summary;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.model.PartMetadata;
import com.oracle.pic.casper.webserver.api.model.UploadMetadata;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.api.model.WSStorageObjectChunk;
import com.oracle.pic.casper.webserver.api.model.WSStorageObjectSummary;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketSummary;
import com.oracle.pic.tagging.client.tag.TagSet;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Helper methods for converting from backend database objects to backend API model objects.
 */
public final class BackendConversions {
    private BackendConversions() {

    }

    static WSTenantBucketInfo mdsBucketInfoToWSBucketInfo(MdsBucket mdsBucket) {
        return WSTenantBucketInfo.Builder.builder()
                .ocid(mdsBucket.getOcid())
                .namespaceKey(new NamespaceKey(MdsTransformer.toApi(mdsBucket.getBucketKey().getApi()),
                        mdsBucket.getBucketKey().getNamespaceName()))
                .bucketName(mdsBucket.getBucketKey().getBucketName())
                .compartment(mdsBucket.getCompartmentId())
                .creationUser(mdsBucket.getCreationUser())
                .creationTime(MdsTransformer.toInstant(mdsBucket.getCreationTime()))
                .modificationTime(MdsTransformer.toInstant(mdsBucket.getModificationTime()))
                .etag(mdsBucket.getEtag())
                .encryptedMetadata(mdsBucket.getMetadata())
                .encryptionKeyPayload(mdsBucket.getEncryptionKey().getKey().toByteArray())
                .encryptionKeyVersion(mdsBucket.getEncryptionKey().getVersion())
                .publicAccessType(MdsTransformer.toPublicAccessType(mdsBucket.getAccessType()))
                .storageTier(MdsTransformer.toStorageTier(mdsBucket.getStorageTier()))
                .meterFlagSet(MdsTransformer.toBucketMeterFlagSet(mdsBucket.getMeterFlagSetList()))
                .serializedOptions(mdsBucket.getOptions())
                .objectLevelAuditMode(MdsTransformer.toAuditMode(mdsBucket.getAuditMode()))
                .serializedTags(mdsBucket.getTags())
                .kmsKeyId(mdsBucket.getKmsKey())
                .objectLifecyclePolicyEtag(mdsBucket.getLifecycleEtag())
                .immutableResourceId(mdsBucket.getImmutableResourceId())
                .mdsBucketToken(toMdsBucketToken(mdsBucket.getBucketToken()))
                .isTenancyDeleted(mdsBucket.getNamespaceDeleted())
                .isCacheable(mdsBucket.getCacheable())
                .build();
    }

    private static RangeMap<Integer, TenantBucketShardInfo> dbBucketShardsToWSBucketShards(
            RangeMap<Integer, TenantBucketShardInfo> bucketShards) {
        ImmutableRangeMap.Builder<Integer, TenantBucketShardInfo> builder = new ImmutableRangeMap.Builder<>();

        for (Map.Entry<Range<Integer>, TenantBucketShardInfo> bucketShard : bucketShards.asMapOfRanges().entrySet()) {
            builder.put(Range.closedOpen(bucketShard.getKey().lowerEndpoint(), bucketShard.getKey().upperEndpoint()),
                    dbBucketShardInfoToWSBucketShardInfo(bucketShard.getValue()));
        }

        return builder.build();
    }

    private static TenantBucketShardInfo dbBucketShardInfoToWSBucketShardInfo(
            TenantBucketShardInfo tenantBucketShardInfo) {
        return TenantBucketShardInfo.Builder.builder()
                .bucketId(tenantBucketShardInfo.getBucketId())
                .bucketOcid(tenantBucketShardInfo.getBucketOcid())
                .shard(tenantBucketShardInfo.getShard())
                .bucketName(tenantBucketShardInfo.getBucketName())
                .build();
    }


    static WSTenantBucketSummary mdsBucketSummaryToWSBucketSummary(MdsBucketSummary mdsBucketSummary) {
        return WSTenantBucketSummary.Builder.builder()
                .namespaceKey(new NamespaceKey(MdsTransformer.toApi(mdsBucketSummary.getBucketKey().getApi()),
                        mdsBucketSummary.getBucketKey().getNamespaceName()))
                .bucketName(mdsBucketSummary.getBucketKey().getBucketName())
                .compartment(mdsBucketSummary.getCompartmentId())
                .creationUser(mdsBucketSummary.getCreationUser())
                .creationTime(MdsTransformer.toInstant(mdsBucketSummary.getCreationTime()))
                .etag(mdsBucketSummary.getEtag())
                .objectLifecyclePolicyEtag(mdsBucketSummary.getLifecycleEtag())
                .serializedTags(mdsBucketSummary.getTags())
                .build();
    }

    static Bucket wsBucketToBucket(
            WSTenantBucketInfo bucket, DecidingKeyManagementService kms) {
        final TagSet tags = bucket.getTags();
        String objectLifecyclePolicyEtag = bucket.getObjectLifecyclePolicyEtag().orElse(null);
        if (Strings.isNullOrEmpty(objectLifecyclePolicyEtag)) {
            objectLifecyclePolicyEtag = null;
        }
        String kmsKeyId = bucket.getKmsKeyId().orElse(null);
        if (Strings.isNullOrEmpty(kmsKeyId)) {
            kmsKeyId = null;
        }
        final boolean objectEventsEnabled = (boolean) (bucket.getOptions().getOrDefault("objectEventsEnabled", false));
        return new Bucket.Builder()
                .namespaceName(bucket.getKey().getNamespace())
                .bucketName(bucket.getKey().getName())
                .compartmentId(bucket.getCompartment())
                .createdBy(bucket.getCreationUser())
                .timeCreated(bucket.getCreationTime())
                .metadata(bucket.getMetadata(kms))
                .etag(bucket.getEtag())
                .publicAccessType(bucket.getPublicAccessType())
                .storageTier(bucket.getStorageTier())
                .meterFlagSet(bucket.getMeterFlagSet())
                .serializedOptions(bucket.getSerializedOptions().orElse(null))
                .objectLevelAuditMode(bucket.getObjectLevelAuditMode())
                .freeformTags(tags.getFreeformTags())
                .definedTags(tags.getDefinedTags())
                .objectLifecyclePolicyEtag(objectLifecyclePolicyEtag)
                .kmsKeyId(kmsKeyId)
                .objectEventsEnabled(objectEventsEnabled)
                .replicationEnabled(bucket.isReplicationEnabled())
                .readOnly(bucket.isReadOnly())
                .id(bucket.getOcid())
                .build();
    }

    static BucketSummary wsBucketToBucketSummary(
        WSTenantBucketSummary bucket,
        boolean returnObjectLifecyclePolicyEtag,
        boolean returnTags) {
        final TagSet tagSet = bucket.getTags();
        Map<String, String> freeformTags = tagSet.getFreeformTags();
        Map<String, Map<String, Object>> definedTags = tagSet.getDefinedTags();

        BucketSummary.Builder builder = BucketSummary.Builder.builder()
                .namespaceName(bucket.getKey().getNamespace())
                .bucketName(bucket.getKey().getName())
                .compartmentId(bucket.getCompartment())
                .createdBy(bucket.getCreationUser())
                .timeCreated(bucket.getCreationTime())
                .etag(bucket.getEtag());

        if (returnObjectLifecyclePolicyEtag) {
            builder.objectLifecyclePolicyEtag(bucket.getObjectLifecyclePolicyEtag().orElse(null));
        }

        if (returnTags) {
            builder.freeformTags(freeformTags).definedTags(definedTags);
        }

        return builder.build();
    }

    /**
     * Convert a {@link ScanResults} to a {@link PaginatedList} for a
     * {@link Bucket} result.
     * @param scanResults the scan results.
     * @param returnObjectLifecyclePolicyEtag boolean to return object lifecycle policy etag
     * @return the paginated list.
     */
    static PaginatedList<BucketSummary> bucketScanResultsToPaginatedList(
        ScanResults<WSTenantBucketSummary> scanResults,
        boolean returnObjectLifecyclePolicyEtag) {
        return new PaginatedList<>(
                scanResults.getKeys().stream()
                        .map(bucket -> wsBucketToBucketSummary(bucket, returnObjectLifecyclePolicyEtag, false))
                        .collect(Collectors.toList()), scanResults.isTruncated());
    }

    /**
     * Converts a {@link List} to a {@link PaginatedList} for {@link NamespaceKey}
     * @param namespaceKeys The list results
     * @return the paginated list
     */
    static PaginatedList<NamespaceSummary> namespaceKeyResultsToPaginatedList(List<NamespaceKey> namespaceKeys) {
        return new PaginatedList<>(
                namespaceKeys.stream().map(BackendConversions::namespaceKeyToNamespaceSummary)
                        .collect(Collectors.toList()), !namespaceKeys.isEmpty());
    }

    /**
     * Helper to convert non-null {@link NamespaceKey} to {@link NamespaceSummary}
     * @param nk The namespace key to convert
     * @return The NamespaceSummary result
     */
    static NamespaceSummary namespaceKeyToNamespaceSummary(NamespaceKey nk) {
        return NamespaceSummary.builder().name(nk.getName()).build();
    }

    public static WSStorageObject storageObjectToWSStorageObject(StorageObject storageObject) {
        if (storageObject == null) {
            return null;
        }
        final ImmutableSortedMap.Builder<Long, WSStorageObjectChunk> builder =
                new ImmutableSortedMap.Builder<>(Ordering.natural());
        storageObject.getObjectChunks()
                .forEach((k, v) -> builder.put(k, BackendConversions.storageObjectChunkToWSStorageObjectChunk(v)));
        return new WSStorageObject(
                storageObject.getKey(),
                storageObject.getObjectId(),
                storageObject.getETag(),
                Date.from(storageObject.getCreationTime()),
                Date.from(storageObject.getModificationTime()),
                storageObject.getTotalSizeInBytes(),
                storageObject.getMd5(),
                storageObject.getChecksumType(),
                storageObject.getEncryptedMetadata(),
                storageObject.getEncryptedEncryptionKeyPayLoad(),
                storageObject.getEncryptionKeyVersion(),
                builder.build(),
                storageObject.getPartCount(),
                getNullableDateFromOptionalInstant(storageObject.getMinimumRetentionTime()),
                getNullableDateFromOptionalInstant(storageObject.getArchivedTime()),
                getNullableDateFromOptionalInstant(storageObject.getRestoredTime()),
                storageObject.getArchivalState(),
                storageObject.getStorageTier());
    }

    public static WSStorageObject mdsObjectToWSStorageObject(MdsObject mdsObject) {
        if (mdsObject == null || !mdsObject.isInitialized()) {
            return null;
        }
        final String md5 = mdsObject.getObjectBase().getMd5().isEmpty() ? null : mdsObject.getObjectBase().getMd5();
        final byte[] encryptionKeyPayload = mdsObject.getObjectBase().getEncryptionKey().getKey().isEmpty() ?
                null : mdsObject.getObjectBase().getEncryptionKey().getKey().toByteArray();

        final String encryptionVersion = mdsObject.getObjectBase().getEncryptionKey().getVersion().isEmpty() ?
                null : mdsObject.getObjectBase().getEncryptionKey().getVersion();

        final Date minRetentionTime = mdsObject.getObjectBase().hasMinRetentionTime()
                ? TimestampUtils.toDate(mdsObject.getObjectBase().getMinRetentionTime())
                : null;

        final Date archivedTime = mdsObject.getObjectBase().hasArchivedTime()
                ? TimestampUtils.toDate(mdsObject.getObjectBase().getArchivedTime())
                : null;

        final Date restoredTime = mdsObject.getObjectBase().hasRestoredTime()
                ? TimestampUtils.toDate(mdsObject.getObjectBase().getRestoredTime())
                : null;

        final ImmutableSortedMap.Builder<Long, WSStorageObjectChunk> builder =
                new ImmutableSortedMap.Builder<>(Ordering.natural());
        mdsObject.getChunksMap()
                .forEach((k, v) -> builder.put(k, mdsChunkToWSStorageObjectChunk(v)));
        return new WSStorageObject(
                MdsTransformer.toObjectKey(mdsObject.getObjectKey()),
                new ObjectId(mdsObject.getObjectBase().getObjectId()),
                mdsObject.getObjectBase().getEtag(),
                TimestampUtils.toDate(mdsObject.getObjectBase().getCreationTime()),
                TimestampUtils.toDate(mdsObject.getObjectBase().getModificationTime()),
                mdsObject.getObjectBase().getTotalSizeInBytes(),
                md5,
                MdsTransformer.toChecksumType(mdsObject.getObjectBase().getChecksumType()),
                mdsObject.getObjectBase().getMetadata(),
                encryptionKeyPayload,
                encryptionVersion,
                builder.build(),
                mdsObject.getObjectBase().getPartCount(),
                minRetentionTime,
                archivedTime,
                restoredTime,
                MdsTransformer.toArchivalState(mdsObject.getObjectBase().getArchivalState()),
                MdsTransformer.toObjectStorageTier(mdsObject.getObjectBase().getStorageTier()));
    }

    public static WSStorageObjectSummary mdsObjectSummaryToWSStorageObjectSummary(MdsObjectSummary mdsObjectSummary) {
        if (mdsObjectSummary == null || !mdsObjectSummary.isInitialized()) {
            return null;
        }
        String md5 =
                mdsObjectSummary.getObjectBase().getMd5().isEmpty() ? null
                        : mdsObjectSummary.getObjectBase().getMd5();

        byte[] encryptionKeyPayload = mdsObjectSummary.getObjectBase().getEncryptionKey().getKey().isEmpty() ? null
                : mdsObjectSummary.getObjectBase().getEncryptionKey().getKey().toByteArray();

        String encryptionVersion =
                mdsObjectSummary.getObjectBase().getEncryptionKey().getVersion().isEmpty() ? null
                        : mdsObjectSummary.getObjectBase().getEncryptionKey().getVersion();

        Date minRetentionTime = mdsObjectSummary.getObjectBase().hasMinRetentionTime()
                ? TimestampUtils.toDate(mdsObjectSummary.getObjectBase().getMinRetentionTime())
                : null;

        Date archivedTime = mdsObjectSummary.getObjectBase().hasArchivedTime()
                ? TimestampUtils.toDate(mdsObjectSummary.getObjectBase().getArchivedTime())
                : null;

        Date restoredTime = mdsObjectSummary.getObjectBase().hasRestoredTime()
                ? TimestampUtils.toDate(mdsObjectSummary.getObjectBase().getRestoredTime())
                : null;

        return new WSStorageObjectSummary(
                MdsTransformer.toObjectKey(mdsObjectSummary.getObjectKey()),
                new ObjectId(mdsObjectSummary.getObjectBase().getObjectId()),
                mdsObjectSummary.getObjectBase().getEtag(),
                TimestampUtils.toDate(mdsObjectSummary.getObjectBase().getCreationTime()),
                TimestampUtils.toDate(mdsObjectSummary.getObjectBase().getModificationTime()),
                mdsObjectSummary.getObjectBase().getTotalSizeInBytes(),
                md5,
                MdsTransformer.toChecksumType(mdsObjectSummary.getObjectBase().getChecksumType()),
                mdsObjectSummary.getObjectBase().getMetadata(),
                encryptionKeyPayload,
                encryptionVersion,
                mdsObjectSummary.getObjectBase().getPartCount(),
                minRetentionTime,
                archivedTime,
                restoredTime,
                MdsTransformer.toArchivalState(mdsObjectSummary.getObjectBase().getArchivalState()),
                MdsTransformer.toObjectStorageTier(mdsObjectSummary.getObjectBase().getStorageTier()));
    }

    public static ListResponse<ObjectNameOnlySummary> mdsListResponseToListResponse(ListNameOnlyResponse listResponse) {
        ImmutableList.Builder<ObjectNameOnlySummary> builder = new ImmutableList.Builder<>();
        List<String> objectNames = listResponse.getObjectNamesList();
        for (String objectName : objectNames) {
            builder.add(new ObjectNameOnlySummary(objectName));
        }
        List<ObjectNameOnlySummary> objects = builder.build();

        ObjectNameOnlySummary next = null;
        if (!listResponse.getNextStartObjectName().isEmpty()) {
            next = new ObjectNameOnlySummary(listResponse.getNextStartObjectName());
        }
        return new ListResponse<ObjectNameOnlySummary>(objects, next);
    }

    public static ListResponse<? extends ObjectBaseSummary> mdsListResponseToListResponse(ListS3Response listResponse) {
        ImmutableList.Builder<ObjectS3Summary> builder = new ImmutableList.Builder<>();

        List<ListS3ObjectSummary> mdsObjects = listResponse.getObjectSummariesList();


        for (ListS3ObjectSummary mdsObject : mdsObjects) {
            builder.add(mdsObjectSummaryToObjectS3Summary(mdsObject));
        }
        List<ObjectS3Summary> objects = builder.build();

        ObjectS3Summary next = null;
        if (listResponse.hasNextStartObject()) {
            next = mdsObjectSummaryToObjectS3Summary(listResponse.getNextStartObject());
        }
        return new ListResponse<ObjectS3Summary>(objects, next);
    }

    public static ObjectS3Summary mdsObjectSummaryToObjectS3Summary(ListS3ObjectSummary mdsObjectSummary) {
        if (mdsObjectSummary == null || !mdsObjectSummary.isInitialized()) {
            return null;
        }
        String md5 = mdsObjectSummary.getMd5().isEmpty() ? null : mdsObjectSummary.getMd5();

        return new ObjectS3Summary(
                mdsObjectSummary.getObjectName(),
                TimestampUtils.toInstant(mdsObjectSummary.getModificationTime()),
                mdsObjectSummary.getTotalSizeInBytes(),
                md5,
                MdsTransformer.toChecksumType(mdsObjectSummary.getChecksumType()),
                mdsObjectSummary.getPartCount(),
                MdsTransformer.toArchivalState(mdsObjectSummary.getArchivalState()));
    }

    public static ListResponse<? extends ObjectBaseSummary> mdsListResponseToListResponse(
            ListBasicResponse listResponse) {
        ImmutableList.Builder<ObjectBasicSummary> builder = new ImmutableList.Builder<>();

        List<ListBasicObjectSummary> mdsObjects = listResponse.getObjectSummariesList();


        for (ListBasicObjectSummary mdsObject : mdsObjects) {
            builder.add(mdsObjectSummaryToObjectBasicSummary(mdsObject));
        }
        List<ObjectBasicSummary> objects = builder.build();

        ObjectBasicSummary next = null;
        if (listResponse.hasNextStartObject()) {
            next = mdsObjectSummaryToObjectBasicSummary(listResponse.getNextStartObject());
        }
        return new ListResponse<ObjectBasicSummary>(objects, next);
    }

    public static ObjectBasicSummary mdsObjectSummaryToObjectBasicSummary(ListBasicObjectSummary mdsObjectSummary) {
        if (mdsObjectSummary == null || !mdsObjectSummary.isInitialized()) {
            return null;
        }
        String md5 = mdsObjectSummary.getMd5().isEmpty() ? null : mdsObjectSummary.getMd5();

        return new ObjectBasicSummary(
                mdsObjectSummary.getObjectName(),
                TimestampUtils.toInstant(mdsObjectSummary.getCreationTime()),
                mdsObjectSummary.getTotalSizeInBytes(),
                md5,
                MdsTransformer.toChecksumType(mdsObjectSummary.getChecksumType()),
                mdsObjectSummary.getPartCount(),
                mdsObjectSummary.getEtag());
    }

    public static ListResponse<? extends ObjectBaseSummary> mdsListResponseToListResponse(
            ListObjectsResponse listResponse) {
        ImmutableList.Builder<ObjectFullSummary> builder = new ImmutableList.Builder<>();

        List<MdsObjectSummary> mdsObjects = listResponse.getObjectSummariesList();


        for (MdsObjectSummary mdsObject : mdsObjects) {
            builder.add(mdsObjectSummaryToObjectFullSummary(mdsObject));
        }
        List<ObjectFullSummary> objects = builder.build();

        ObjectFullSummary next = null;
        if (listResponse.hasNextStartObject()) {
            next = mdsObjectSummaryToObjectFullSummary(listResponse.getNextStartObject());
        }
        return new ListResponse<ObjectFullSummary>(objects, next);
    }

    public static ObjectFullSummary mdsObjectSummaryToObjectFullSummary(MdsObjectSummary mdsObjectSummary) {
        if (mdsObjectSummary == null || !mdsObjectSummary.isInitialized()) {
            return null;
        }

        String md5 =
                mdsObjectSummary.getObjectBase().getMd5().isEmpty() ? null
                        : mdsObjectSummary.getObjectBase().getMd5();

        byte[] encryptionKeyPayload = mdsObjectSummary.getObjectBase().getEncryptionKey().getKey().isEmpty() ? null
                : mdsObjectSummary.getObjectBase().getEncryptionKey().getKey().toByteArray();

        String encryptionVersion =
                mdsObjectSummary.getObjectBase().getEncryptionKey().getVersion().isEmpty() ? null
                        : mdsObjectSummary.getObjectBase().getEncryptionKey().getVersion();

        Instant minRetentionTime = mdsObjectSummary.getObjectBase().hasMinRetentionTime()
                ? TimestampUtils.toInstant(mdsObjectSummary.getObjectBase().getMinRetentionTime())
                : null;

        Instant archivedTime = mdsObjectSummary.getObjectBase().hasArchivedTime()
                ? TimestampUtils.toInstant(mdsObjectSummary.getObjectBase().getArchivedTime())
                : null;

        Instant restoredTime = mdsObjectSummary.getObjectBase().hasRestoredTime()
                ? TimestampUtils.toInstant(mdsObjectSummary.getObjectBase().getRestoredTime())
                : null;

        return new ObjectFullSummary(
                mdsObjectSummary.getObjectKey().getObjectName(),
                new ObjectId(mdsObjectSummary.getObjectBase().getObjectId()),
                mdsObjectSummary.getObjectKey().getBucketKey().getNamespaceName(),
                mdsObjectSummary.getObjectKey().getBucketKey().getBucketName(),
                MdsTransformer.toApi(mdsObjectSummary.getObjectKey().getBucketKey().getApi()),
                mdsObjectSummary.getObjectBase().getEtag(),
                TimestampUtils.toInstant(mdsObjectSummary.getObjectBase().getCreationTime()),
                TimestampUtils.toInstant(mdsObjectSummary.getObjectBase().getModificationTime()),
                mdsObjectSummary.getObjectBase().getTotalSizeInBytes(),
                md5,
                MdsTransformer.toChecksumType(mdsObjectSummary.getObjectBase().getChecksumType()),
                mdsObjectSummary.getObjectBase().getPartCount(),
                mdsObjectSummary.getObjectBase().getMetadata(),
                encryptionKeyPayload,
                encryptionVersion,
                minRetentionTime,
                archivedTime,
                restoredTime,
                MdsTransformer.toArchivalState(mdsObjectSummary.getObjectBase().getArchivalState()),
                MdsTransformer.toObjectStorageTier(mdsObjectSummary.getObjectBase().getStorageTier()));
    }

    public static UploadMetadata mdsUploadInfoToUploadMetadata(MdsUploadInfo mdsUploadInfo) {
        if (mdsUploadInfo == null || !mdsUploadInfo.isInitialized()) {
            return null;
        }
        UploadMetadata uploadMetadata = new UploadMetadata(
                mdsUploadInfo.getUploadKey().getObjectKey().getBucketKey().getNamespaceName(),
                mdsUploadInfo.getUploadKey().getObjectKey().getBucketKey().getBucketName(),
                mdsUploadInfo.getUploadKey().getObjectKey().getObjectName(),
                mdsUploadInfo.getUploadKey().getUploadId(),
                TimestampUtils.toDate(mdsUploadInfo.getCreationTime()));
        return uploadMetadata;
    }

    public static PartMetadata mdsPartInfoToPartMetadata(MdsPartInfo mdsPartInfo) {
        if (mdsPartInfo == null || !mdsPartInfo.isInitialized()) {
            return null;
        }
        return new PartMetadata(
                mdsPartInfo.getPartKey().getUploadPartNum(),
                mdsPartInfo.getObjectBase().getEtag(),
                mdsPartInfo.getObjectBase().getMd5(),
                mdsPartInfo.getObjectBase().getTotalSizeInBytes(),
                TimestampUtils.toDate(mdsPartInfo.getObjectBase().getModificationTime()));
    }

    public static StoragePartInfo mdsPartInfoToStoragePartInfo(MdsPartInfo mdsPartInfo) {
        if (mdsPartInfo == null || !mdsPartInfo.isInitialized()) {
            return null;
        }
        return new StoragePartInfo(new PartKey(mdsPartInfo.getPartKey().getUploadId(),
                mdsPartInfo.getPartKey().getUploadPartNum()), new ObjectId(mdsPartInfo.getObjectBase().getObjectId()),
                mdsPartInfo.getObjectBase().getEtag(),
                TimestampUtils.toInstant(mdsPartInfo.getObjectBase().getCreationTime()),
                TimestampUtils.toInstant(mdsPartInfo.getObjectBase().getModificationTime()),
                mdsPartInfo.getObjectBase().getTotalSizeInBytes(),
                mdsPartInfo.getObjectBase().getMd5());
    }

    public static WSStorageObjectSummary storageObjectSummaryToWSStorageObjectSummary(
            StorageObjectSummary<ObjectKey> storageObjectSummary) {
        if (storageObjectSummary == null) {
            return null;
        }
        return new WSStorageObjectSummary(
                storageObjectSummary.getKey(),
                storageObjectSummary.getObjectId(),
                storageObjectSummary.getETag(),
                Date.from(storageObjectSummary.getCreationTime()),
                Date.from(storageObjectSummary.getModificationTime()),
                storageObjectSummary.getTotalSizeInBytes(),
                storageObjectSummary.getMd5(),
                storageObjectSummary.getChecksumType(),
                storageObjectSummary.getEncryptedMetadata(),
                storageObjectSummary.getEncryptedEncryptionKeyPayLoad(),
                storageObjectSummary.getEncryptionKeyVersion(),
                storageObjectSummary.getPartCount(),
                getNullableDateFromOptionalInstant(storageObjectSummary.getMinimumRetentionTime()),
                getNullableDateFromOptionalInstant(storageObjectSummary.getArchivedTime()),
                getNullableDateFromOptionalInstant(storageObjectSummary.getRestoredTime()),
                storageObjectSummary.getArchivalState(),
                storageObjectSummary.getStorageTier());
    }

    public static WSStorageObjectChunk storageObjectChunkToWSStorageObjectChunk(
            StorageObjectChunk storageObjectChunk) {
        if (storageObjectChunk == null) {
            return null;
        }
        return new WSStorageObjectChunk(
                storageObjectChunk.getSizeInBytes(),
                storageObjectChunk.getVolumeId(),
                storageObjectChunk.getVolumeObjectNumber(),
                storageObjectChunk.getEncryptedEncryptionKeyPayload(),
                storageObjectChunk.getEncryptionKeyVersion());
    }

    public static WSStorageObjectChunk mdsChunkToWSStorageObjectChunk(MdsChunk mdsChunk) {
        if (mdsChunk == null) {
            return null;
        }

        byte[] encryptionKeyPayload = mdsChunk.getEncryptionKey().getKey().isEmpty() ?
                null :
                mdsChunk.getEncryptionKey().getKey().toByteArray();

        String encryptionVersion =
                mdsChunk.getEncryptionKey().getVersion().isEmpty() ? null : mdsChunk.getEncryptionKey().getVersion();

        return new WSStorageObjectChunk(
                mdsChunk.getSizeInBytes(),
                mdsChunk.getVolumeId(),
                mdsChunk.getVolumeObjectNumber(),
                encryptionKeyPayload,
                encryptionVersion);
    }

    /**
     * Helper to convert a non-null StorageObject to an ObjectMetadata.
     * @param so the storage object to convert.
     * @return an ObjectMetadata that has the same metadata as the given storage object.
     */
    public static ObjectMetadata storageObjectToObjectMetadata(
            StorageObjectSummary<ObjectKey> so, DecidingKeyManagementService kms) {
        final WSStorageObjectSummary wsStorageObjectSummary = storageObjectSummaryToWSStorageObjectSummary(so);
        return wsStorageObjectSummaryToObjectMetadata(wsStorageObjectSummary, kms);
    }

    public static ObjectMetadata mdsObjectSummaryToObjectMetadata(
            MdsObjectSummary summary, DecidingKeyManagementService kms) {
        final WSStorageObjectSummary wsStorageObjectSummary = mdsObjectSummaryToWSStorageObjectSummary(summary);
        return wsStorageObjectSummaryToObjectMetadata(wsStorageObjectSummary, kms);
    }

    public static ObjectMetadata wsStorageObjectSummaryToObjectMetadata(
            WSStorageObjectSummary wsStorageObjectSummary, DecidingKeyManagementService kms) {
        Checksum checksum = wsStorageObjectSummary.getChecksumType() == ChecksumType.MD5 ?
                Checksum.fromBase64(wsStorageObjectSummary.getMd5()) :
                Checksum.fromMultipartBase64(wsStorageObjectSummary.getMd5(), wsStorageObjectSummary.getPartCount());

        return new ObjectMetadata(
                wsStorageObjectSummary.getKey().getBucket().getNamespace(),
                wsStorageObjectSummary.getKey().getBucket().getName(),
                wsStorageObjectSummary.getKey().getName(),
                wsStorageObjectSummary.getTotalSizeInBytes(),
                checksum,
                wsStorageObjectSummary.getMetadata(kms),
                wsStorageObjectSummary.getCreationTime(),
                wsStorageObjectSummary.getModificationTime(),
                wsStorageObjectSummary.getETag(),
                wsStorageObjectSummary.getArchivedTime().orElse(null),
                wsStorageObjectSummary.getRestoredTime().orElse(null),
                wsStorageObjectSummary.getArchivalState());
    }

    public static ObjectMetadata mdsObjectToObjectMetadata(
            MdsObjectSummary mdsObjectSummary, DecidingKeyManagementService kms) {
        final WSStorageObjectSummary wsStorageObjectSummary =
                mdsObjectSummaryToWSStorageObjectSummary(mdsObjectSummary);
        return wsStorageObjectSummaryToObjectMetadata(wsStorageObjectSummary, kms);
    }

    static PartETag partAndETagToPartETag(FinishUploadRequest.PartAndETag partAndETag) {
        return new PartETag(partAndETag.getUploadPartNum(), partAndETag.getEtag());
    }

    /**
     * Helper to convert a namespaceInfo to a namespaceMetadata
     * @param mdsNamespace the namespaceInfo return from database
     * @return
     */
    static NamespaceMetadata namepsaceInfoToNamespaceMetadata(MdsNamespace mdsNamespace) {
        return new NamespaceMetadata(mdsNamespace.getDefaultS3Compartment(),
                mdsNamespace.getDefaultSwiftCompartment(), mdsNamespace.getNamespaceName());
    }

    /**
     * Helper to convert a opaque bucket token string to MdsBucketToken object.
     * @param bucketToken The byte string representing the opaque bucket token.
     * @return The MdsBucketToken object
     */
    public static MdsBucketToken toMdsBucketToken(ByteString bucketToken) {
        return BucketTokenTransformers.toMdsBucketToken(bucketToken);
    }

    public static BucketToken toBucketToken(ByteString bucketToken) {
        final MdsBucketToken mdsBucketToken = BucketTokenTransformers.toMdsBucketToken(bucketToken);
        return BucketTokenTransformers.toBucketToken(mdsBucketToken);
    }

    private static Date getNullableDateFromOptionalInstant(Optional<Instant> instant) {
        return instant.isPresent() ? Date.from(instant.get()) : null;
    }
}
