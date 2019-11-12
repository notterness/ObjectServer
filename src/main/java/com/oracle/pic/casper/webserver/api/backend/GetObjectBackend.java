package com.oracle.pic.casper.webserver.api.backend;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.encryption.EncryptionKey;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.exceptions.NotRestoredException;
import com.oracle.pic.casper.common.host.HostInfo;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.model.Stripe;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.util.ObjectLifecycleHelper;
import com.oracle.pic.casper.common.util.ParUtil;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.common.vertx.stream.AbortableReadStream;
import com.oracle.pic.casper.common.vertx.stream.MeasuringReadStream;
import com.oracle.pic.casper.common.vertx.stream.RangeReadStream;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.ChecksumType;
import com.oracle.pic.casper.objectmeta.ObjectDb;
import com.oracle.pic.casper.storageclient.SCRequestContext;
import com.oracle.pic.casper.storageclient.VolumeStorageClient;
import com.oracle.pic.casper.storageclient.models.VolumeStorageContext;
import com.oracle.pic.casper.volumemeta.VolumeMetadata;
import com.oracle.pic.casper.volumemeta.VolumeMetadataClientCache;
import com.oracle.pic.casper.volumemeta.util.VolumeMetadataUtils;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authorizer;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.HttpPathHelpers;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.api.model.WSStorageObjectChunk;
import com.oracle.pic.casper.webserver.api.model.WSStorageObjectSummary;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidBucketNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidObjectNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchObjectException;
import com.oracle.pic.casper.webserver.api.model.exceptions.TooManyObjectsException;
import com.oracle.pic.casper.webserver.api.swift.SwiftGetMetricsBundle;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorderReadStream;
import com.oracle.pic.casper.webserver.util.BucketOptionsUtil;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetricsBundle;
import com.oracle.pic.casper.webserver.vertx.AggregatedReadStream;
import com.oracle.pic.casper.webserver.vertx.CipherReadStream;
import com.oracle.pic.casper.webserver.vertx.DecryptingReadStream;
import com.oracle.pic.casper.webserver.vertx.DefaultAbortableBlobReadStream;
import com.oracle.pic.casper.webserver.vertx.ShortCircuitingAbortableReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class GetObjectBackend {

    private final Backend backend;
    private final VolumeStorageClient volumeStorageClient;
    private final VolumeMetadataClientCache volMetaCache;
    private final Authorizer authorizer;

    private final BucketBackend bucketBackend;
    private final DecidingKeyManagementService kms;

    public GetObjectBackend(Backend backend,
                            VolumeStorageClient volumeStorageClient,
                            VolumeMetadataClientCache volMetaCache,
                            Authorizer authorizer,
                            BucketBackend bucketBackend,
                            DecidingKeyManagementService kms
                            ) {
        this.backend = backend;
        this.volumeStorageClient = volumeStorageClient;
        this.volMetaCache = volMetaCache;
        this.authorizer = authorizer;
        this.bucketBackend = bucketBackend;
        this.kms = kms;
    }

    /**
     * An enumeration of read operations and their associated identity operation and permission(s).
     */
    public enum ReadOperation {
        HEAD(CasperOperation.HEAD_OBJECT, CasperPermission.OBJECT_READ, CasperPermission.OBJECT_INSPECT),
        GET(CasperOperation.GET_OBJECT, CasperPermission.OBJECT_READ);

        private final CasperOperation operation;
        private final CasperPermission[] permissions;

        ReadOperation(CasperOperation operation, CasperPermission... permissions) {
            this.operation = operation;
            this.permissions = permissions;
        }

        private CasperOperation getOperation() {
            return operation;
        }

        private CasperPermission[] getPermissions() {
            return permissions.clone();
        }
    }

    /**
     * Get the metadata for an object from the V2 API for either a GET or HEAD object request.
     *
     * @return a CompletableFuture that contains object metadata or an exception.
     * @throws InvalidBucketNameException
     * @throws InvalidObjectNameException
     * @throws NoSuchBucketException
     * @throws NoSuchObjectException
     */
    public CompletableFuture<WSStorageObject> getV2StorageObject(RoutingContext context,
                                                                 AuthenticationInfo authInfo,
                                                                 ReadOperation readOperation,
                                                                 String namespace,
                                                                 String bucketName,
                                                                 String objectName) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);
        Validator.validateObjectName(objectName);

        return getStorageObject(
                context,
                readOperation,
                bucket -> authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        bucket.getNamespaceKey(),
                        bucket.getBucketName(),
                        bucket.getCompartment(),
                        bucket.getPublicAccessType(),
                        readOperation.getOperation(),
                        bucket.getKmsKeyId().orElse(null),
                        false,
                        bucket.isTenancyDeleted(),
                        readOperation.permissions).isPresent(),
                namespace,
                bucketName,
                objectName,
                Api.V2);
    }

    /**
     * Get the metadata for an object from the V1 API for either a GET or HEAD object request.
     *
     * @return a CompletableFuture that contains object metadata or an exception.
     * @throws InvalidBucketNameException
     * @throws InvalidObjectNameException
     * @throws NoSuchBucketException
     * @throws NoSuchObjectException
     */
    public CompletableFuture<WSStorageObject> getV1StorageObject(RoutingContext context,
                                                                 ReadOperation readOperation,
                                                                 String namespace,
                                                                 String bucketName,
                                                                 String objectName) {
        Validator.validateObjectName(objectName);
        Validator.validateBucket(bucketName);

        return getStorageObject(
                context,
                readOperation,
                bucket -> authorizer.authorize(
                        WSRequestContext.get(context),
                        AuthenticationInfo.V1_USER,
                        bucket.getNamespaceKey(),
                        bucket.getBucketName(),
                        bucket.getCompartment(),
                        bucket.getPublicAccessType(),
                        readOperation.getOperation(),
                        null,
                        false,
                        bucket.isTenancyDeleted(),
                        readOperation.permissions).isPresent(),
                namespace,
                bucketName,
                objectName,
                Api.V1);
    }

    /**
     * Get the metadata for an internal PAR object.
     *
     * @return a CompletableFuture that contains object metadata or an exception.
     * @throws InvalidBucketNameException
     * @throws InvalidObjectNameException
     * @throws NoSuchBucketException
     * @throws NoSuchObjectException
     */
    public CompletableFuture<WSStorageObject> getParStorageObject(RoutingContext context,
                                                                  String namespace,
                                                                  String bucketName,
                                                                  String objectName) {
        Validator.validateV2Namespace(namespace);
        Validator.validateParObjectName(objectName);
        Validator.validateInternalBucket(bucketName, ParUtil.PAR_BUCKET_NAME_SUFFIX);

        return getStorageObject(
                context,
                ReadOperation.HEAD,
                bucket -> true,
                namespace,
                bucketName,
                objectName,
                Api.V2);
    }

    /**
     * Get the metadata for an internal object lifecycle policy object.
     *
     * @return a CompletableFuture that contains object metadata or an exception.
     * @throws InvalidBucketNameException
     * @throws InvalidObjectNameException
     * @throws NoSuchBucketException
     * @throws NoSuchObjectException
     */
    public CompletableFuture<WSStorageObject> getLifecycleStorageObject(RoutingContext context,
                                                                        String namespace,
                                                                        String bucketName,
                                                                        String objectName) {
        Validator.validateV2Namespace(namespace);
        Validator.validateObjectName(objectName);
        Validator.validateInternalBucket(bucketName, ObjectLifecycleHelper.LIFECYCLE_NAME_SUFFIX);
        return getStorageObject(
                context,
                ReadOperation.GET,
                bucket -> true,
                namespace,
                bucketName,
                objectName,
                Api.V2);
    }

    CompletableFuture<WSStorageObject> getStorageObject(RoutingContext context,
                                                        ReadOperation readOperation,
                                                        Function<WSTenantBucketInfo, Boolean> authzCB,
                                                        String namespace,
                                                        String bucketName,
                                                        String objectName,
                                                        Api api) {
        final MetricScope rootScope = WSRequestContext.getMetricScope(context);
        final MetricScope childScope = rootScope.child("getObjectBackend:getStorageObject");

        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, objectName);

        return VertxUtil.runAsync(childScope, () -> {
            final WSTenantBucketInfo bucket =
                bucketBackend.getBucketMetadataWithCache(context, childScope, namespace, bucketName, api);

            final WSRequestContext wsRequestContext = WSRequestContext.get(context);
            // Put objectLevelAuditMode in the context to be retrieved by AuditFilterHandler
            if (!BucketBackend.InternalBucketType.isInternalBucket(bucketName)) {
                wsRequestContext.setObjectLevelAuditMode(bucket.getObjectLevelAuditMode());
                wsRequestContext.setBucketOcid(bucket.getOcid());
            }
            wsRequestContext.setBucketLoggingStatus(BucketOptionsUtil
                    .getBucketLoggingStatusFromOptions(bucket.getOptions()));

            Backend.authorizeOperationForPermissionsInternal(
                    childScope,
                    casperPermissions -> authzCB.apply(bucket),
                    bucket,
                    readOperation.getOperation());

            WSStorageObject wsStorageObject;
            if (readOperation == ReadOperation.GET) {
                wsStorageObject = MetricScope.timeScopeF(childScope, "objectMds:get",
                    v -> backend.getObject(context, bucket, objectName)
                        .orElseThrow(() -> new NoSuchObjectException(
                            "The object '" + objectName + "' was not found in the bucket '" + bucketName + "'"
                        )));
            } else {
                WSStorageObjectSummary wsStorageObjectSummary = MetricScope.timeScopeF(childScope, "objectMds:head",
                    v -> backend.headObject(context, bucket, objectName)
                        .orElseThrow(() -> new NoSuchObjectException(
                            "The object '" + objectName + "' was not found in the bucket '" + bucketName + "'"
                        )));

                // This wsStorageObject contains an empty chunk map in it. This empty chunk map will never be used
                // since this is in the Head Object call path.
                wsStorageObject = wsStorageObjectSummary.toWSStorageObjectWithEmptyChunk();
            }

            if (!BucketBackend.InternalBucketType.isInternalBucket(bucketName)) {
                wsRequestContext.setEtag(wsStorageObject.getETag());
            }

            // Trigger the decryption of metadata to ensure it happens on a VertX worker thread.
            wsStorageObject.getMetadata(kms);
            return wsStorageObject;
        });
    }

    public CompletableFuture<List<WSStorageObject>> getStorageObjectsFromPrefix(RoutingContext context,
                                                                                AuthenticationInfo authInfo,
                                                                                ReadOperation operation,
                                                                                String namespace,
                                                                                String bucketName,
                                                                                @Nullable String objectPrefix,
                                                                                int limit) {
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);
        if (objectPrefix != null) {
            Validator.validateObjectName(objectPrefix);
        }

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("getObjectBackend:getStorageObjectsFromPrefix");

        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);

        CompletableFuture<List<WSStorageObject>> result = VertxUtil.runAsync(childScope, () -> {
            final WSTenantBucketInfo bucket =
                bucketBackend.getBucketMetadataWithCache(context, childScope, namespace, bucketName, Api.V2);
            WSRequestContext.get(context).setBucketOcid(bucket.getOcid());
            WSRequestContext.get(context).setBucketCreator(bucket.getCreationUser());

            Backend.authorizeOperationForPermissions(
                    childScope,
                    context,
                    authorizer,
                    authInfo,
                    bucket,
                    operation.getOperation(),
                    operation.getPermissions());

            // Dynamic Large Object (SWIFT feature) specific:
            // Asking for max limit value possible to ensure that we can always get 1000 in case of multi shard bucket
            final List<WSStorageObjectSummary> wsStorageObjectSummaries = MetricScope.timeScopeF(
                    childScope, "objectMds:listObjects",
                    v -> backend.listObjectsFromPrefix(context, bucket, ObjectDb.MAX_LIST_OBJECT_LIMIT, objectPrefix));

            if (wsStorageObjectSummaries.size() > limit) {
                throw new TooManyObjectsException("Too many objects with prefix, maximum " +
                        limit + " objects supported.");
            }

            if (wsStorageObjectSummaries.isEmpty()) {
                throw new NoSuchObjectException("Objects with prefix not found.");
            }

            return MetricScope.timeScopeF(childScope, "objectMds:get",
                    v -> wsStorageObjectSummaries.stream().map(summary -> {
                        if (!summary.isGetPossible()) {
                            throw new NotRestoredException(
                                    "Archived Object " + summary.getKey().getName() + " not available for Get");
                        }
                        return backend.getObject(context, bucket, summary.getKey().getName()).orElse(null);
                    }).filter(so -> so != null).collect(Collectors.toList()));
        });
        ServiceLogsHelper.logServiceEntry(context);
        return result;
    }

    /**
     * <p>
     * Given an {@link WSStorageObject object's metadata}, get the object's content</p>
     * <p>
     * Usage Note:</p>
     * <p>
     * As much as possible, this method should be used only as the last step of composing a response.  Before it is
     * called the request and object metadata should already be fully evaluated.
     * </p>
     *
     * @param so the metadata retrieved by calling getV2StorageObject.
     * @return the object's content as a stream
     * @see <a href="https://tools.ietf.org/html/rfc7233#section-2.1">RFC Doc</a>
     */
    public AbortableBlobReadStream getObjectStream(RoutingContext context,
                                                   WSStorageObject so,
                                                   @Nullable ByteRange byteRange,
                                                   WebServerMetricsBundle bundle) {
        final ArchivalState archivalState = so.getArchivalState();
        if (archivalState == ArchivalState.Archived) {
            throw new NotRestoredException("Archived object is not restored.");
        } else if (archivalState == ArchivalState.Restoring) {
            throw new NotRestoredException("Archived object is being restored.");
        }
        return getObjectStream(WSRequestContext.get(context), so, byteRange, true, bundle);
    }

    AbortableBlobReadStream getObjectStream(WSRequestContext context,
                                            WSStorageObject so,
                                            @Nullable ByteRange byteRange,
                                            boolean decrypt,
                                            WebServerMetricsBundle bundle) {
        final MetricScope scope = context.getCommonRequestContext().getMetricScope();
        final String rid = context.getCommonRequestContext().getOpcRequestId();

        final Map<Long, WSStorageObjectChunk> chunks;
        final long requestedContentLength;
        final String contentMd5;
        if (byteRange == null) {
            chunks = so.getObjectChunks();
            requestedContentLength = so.getTotalSizeInBytes();
            contentMd5 = so.getMd5();
        } else {
            chunks = getFilteredChunks(so.getObjectChunks(), byteRange);
            requestedContentLength = byteRange.getLength();
            contentMd5 = null;
        }

        final Digest expectedDigest;
        final DigestAlgorithm algorithm = DigestAlgorithm.MD5;
        if (so.getChecksumType() == ChecksumType.MD5) {
            expectedDigest = contentMd5 == null ? null : Digest.fromBase64Encoded(algorithm, contentMd5);
        } else if (so.getChecksumType() == ChecksumType.MD5_OF_MD5) {
            expectedDigest = null;
        } else {
            throw new IllegalArgumentException("Unexpected checksum type: " + so.getChecksumType());
        }

        final Iterator<CompletableFuture<AbortableReadStream<Buffer>>> chunkIterator = getChunkIterator(
                chunks.entrySet().stream(), context, byteRange, decrypt);
        final AggregatedReadStream<Buffer> aggregatedReadStream = new AggregatedReadStream<>(chunkIterator);
        final ReadStream<Buffer> trStream = new TrafficRecorderReadStream(aggregatedReadStream, rid);
        final MeasuringReadStream<Buffer> readStream = new MeasuringReadStream<>(
                bundle.getFirstByteLatency(), context.getStartTime(), scope, trStream);
        final ShortCircuitingAbortableReadStream<Buffer> scStream =
                new ShortCircuitingAbortableReadStream<>(aggregatedReadStream, readStream);
        return new DefaultAbortableBlobReadStream(algorithm, Optional.ofNullable(expectedDigest),
                requestedContentLength, scStream);
    }

    /**
     * This code path is only used by Swift interface to deal with DLO where a DLO can have multiple backing objects
     * and each object can have multiple chunks. It works by obtaining manifests for each chunk and then combine them
     * before returning it as {@link AggregatedReadStream}.
     */
    public AbortableBlobReadStream getCombinedObjectStream(WSRequestContext context,
                                                           List<WSStorageObject> parts,
                                                           long totalSizeInBytes,
                                                           @Nullable ByteRange byteRange,
                                                           WebServerMetricsBundle bundle) {
        Preconditions.checkNotNull(parts);
        Preconditions.checkState(!parts.isEmpty());

        final MetricScope scope = context.getCommonRequestContext().getMetricScope();
        final String rid = context.getCommonRequestContext().getOpcRequestId();

        final long contentLength;
        final List<Iterator<CompletableFuture<AbortableReadStream<Buffer>>>> iterators = new ArrayList<>(parts.size());

        if (byteRange == null) {
            //request whole object
            contentLength = totalSizeInBytes;
            for (WSStorageObject part : parts) {
                iterators.add(getChunkIterator(part.getObjectChunks().entrySet().stream(), context, null, true));
            }
        } else {
            //request specific byte range
            contentLength = byteRange.getLength();
            long currentSizeInByte = 0;
            for (WSStorageObject part : parts) {
                final long partSize = part.getTotalSizeInBytes();
                ByteRange partByteRange;

                if (currentSizeInByte + partSize <= byteRange.getStart()) {
                    //requested range is beyond this part
                    currentSizeInByte += partSize;
                    continue;
                } else if (currentSizeInByte > byteRange.getEnd()) {
                    //part is beyond the requested range
                    break;
                } else {
                    //requested range intersects with part
                    long start = Math.max(0, byteRange.getStart() - currentSizeInByte);
                    long end = Math.min(partSize - 1, byteRange.getEnd() - currentSizeInByte);
                    partByteRange = new ByteRange(start, end);
                }

                final Map<Long, WSStorageObjectChunk> chunks = getFilteredChunks(part.getObjectChunks(), partByteRange);
                Iterator<CompletableFuture<AbortableReadStream<Buffer>>> iterator = getChunkIterator(
                        chunks.entrySet().stream(), context, partByteRange, true
                );
                iterators.add(iterator);
                currentSizeInByte += partSize;
            }
        }

        final Iterator<CompletableFuture<AbortableReadStream<Buffer>>> combinedIterator =
                Iterators.concat(iterators.iterator());
        final AggregatedReadStream<Buffer> aggStream = new AggregatedReadStream<>(combinedIterator);

        if (!(bundle instanceof SwiftGetMetricsBundle)) {
            throw new IllegalArgumentException("The bundle must be a SwiftGetMetricsBundle");
        }
        final SwiftGetMetricsBundle swiftBundle = (SwiftGetMetricsBundle) bundle;
        final TrafficRecorderReadStream trStream = new TrafficRecorderReadStream(aggStream, rid);
        final MeasuringReadStream<Buffer> readStream = new MeasuringReadStream<>(
                swiftBundle.getDLOFirstByteLatency(),
                context.getStartTime(),
                scope,
                trStream);
        final ShortCircuitingAbortableReadStream<Buffer> scStream = new ShortCircuitingAbortableReadStream<>(
                aggStream, readStream);
        return new DefaultAbortableBlobReadStream(DigestAlgorithm.MD5, Optional.empty(), contentLength, scStream);
    }

    /**
     * Gets a map of ObjectChunks indexed by the offset that fall in
     * byte range passed.
     *
     * @param objectChunks original full set of objects chunks map
     * @param byteRange    the offset of the object chunks we are interested in
     * @return A map of initial-offset -> chunk for the object's data.
     */
    static NavigableMap<Long, WSStorageObjectChunk> getFilteredChunks(
            NavigableMap<Long, WSStorageObjectChunk> objectChunks,
            ByteRange byteRange) {
        return objectChunks.subMap(objectChunks.floorKey(byteRange.getStart()), true,
                objectChunks.floorKey(byteRange.getEnd()), true);
    }

    /**
     * Helper method to get a read stream of the next chunk's data from the provided chunk metadata iterator.
     *
     * @param chunks    An stream containing the chunk metadata, ordered by offset.
     * @param context   The common context for metrics and logging.
     * @param byteRange The byte range of the chunks we are interested in
     * @return a {@link CompletableFuture} that contains the chunk stream.
     */
    private Iterator<CompletableFuture<AbortableReadStream<Buffer>>> getChunkIterator(
            Stream<Map.Entry<Long, WSStorageObjectChunk>> chunks, WSRequestContext context, final ByteRange byteRange,
            boolean decrypt) {
        return chunks.map(entry -> {
            final Long chunkStartOffset = entry.getKey();
            final WSStorageObjectChunk chunk = entry.getValue();
            final MetricScope rootScope = context.getCommonRequestContext().getMetricScope();
            return VertxUtil.runAsync(() -> {
                if (decrypt) {
                    chunk.getEncryptedEncryptionKey().getEncryptionKey(kms);
                }
            }).thenCompose((ignore) -> {
                MetricScope childScope = rootScope.child("volumeStorageClient:get")
                    .annotate("volumeId", chunk.getVolumeId())
                    .annotate("von", chunk.getVolumeObjectNumber());

                final long contentLength;
                final ByteRange chunkByteRange;
                final ByteRange aesChunkByteRange;
                if (byteRange != null) {
                    //We need to transform the byteRange to a chunk ByteRange
                    chunkByteRange = new ByteRange(
                        Long.max(byteRange.getStart() - chunkStartOffset, 0),
                        Long.min(byteRange.getEnd() - chunkStartOffset, chunk.getSizeInBytes() - 1)
                    );
                    childScope.annotate("range", chunkByteRange.getStart() + "-" + chunkByteRange.getEnd());
                    aesChunkByteRange = CipherReadStream.convertToAesBoundaries(chunkByteRange, chunk.getSizeInBytes());
                    childScope.annotate("aes-range", aesChunkByteRange.getStart() + "-" + aesChunkByteRange.getEnd());
                    contentLength = aesChunkByteRange.getLength();

                } else { //assuming entire chunk length range
                    aesChunkByteRange = null;
                    chunkByteRange = null;
                    contentLength = chunk.getSizeInBytes();
                }

                final VolumeMetadata volMeta = volMetaCache.getVolume(chunk.getVolumeId());
                if (volMeta == null) {
                    throw new IllegalStateException(
                            "The volume metadata could not be found for ID: " + chunk.getVolumeId());
                }

                final Stripe<HostInfo> hostInfoStripe =
                        VolumeMetadataUtils.getStorageServerHostInfoStripe(volMeta, volMetaCache);

                return volumeStorageClient.get(
                    new SCRequestContext(
                            context.getCommonRequestContext().getOpcRequestId(),
                            context.getCommonRequestContext().getOpcClientRequestId().orElse(null),
                            context.getCommonRequestContext().getMetricScope()),
                    new VolumeStorageContext(chunk.getVolumeId(), chunk.getVolumeObjectNumber(), hostInfoStripe),
                    aesChunkByteRange, contentLength
                ).whenComplete((abortableBlobReadStream, throwable) -> {
                    if (throwable != null) {
                        childScope.fail(throwable);
                    } else {
                        childScope.annotate("bytesRead", abortableBlobReadStream.getBytesTransferred()).end();
                    }
                }).thenApply(blobReadStream -> {
                    long offset = Optional.ofNullable(aesChunkByteRange).map(ByteRange::getStart).orElse(0L);
                    return decrypt ?
                        decryptBlobReadStream(
                                offset, blobReadStream, chunk.getEncryptedEncryptionKey().getEncryptionKey(kms)) :
                        blobReadStream;
                }).thenApply(abortableReadStream -> {
                    if (chunkByteRange != null) {
                        long aesStartOffset = CipherReadStream.calculateAesStartOffset(chunkByteRange.getStart());
                        long rangeEnd = chunkByteRange.getLength() + aesStartOffset;
                        RangeReadStream rangeReadStream
                            = new RangeReadStream(abortableReadStream, aesStartOffset, rangeEnd);
                        return new ShortCircuitingAbortableReadStream<>(abortableReadStream, rangeReadStream);
                    } else {
                        return abortableReadStream;
                    }
                });
            });
        }).iterator();

    }

    /**
     * Helper method that is at the moment public so that it can be leveraged by the V1 API.
     * This method will wrap the provided {@link AbortableBlobReadStream} with a {@link DecryptingReadStream}
     * if the chunk attempted to be decrypted has an {@link EncryptionKey}
     *
     * @param offset              The AES block offset at which to start the decryption counter
     * @param abortableReadStream The delegate encrypted read stream to decrypt
     * @param encryptionKey       The encryption key, if it exists will be used to decrypt the read stream.
     * @return A {@link AbortableReadStream} that will decrypt the delegate proided
     */
    public static AbortableReadStream<Buffer> decryptBlobReadStream(long offset,
                                                                    AbortableReadStream<Buffer> abortableReadStream,
                                                                    @Nullable EncryptionKey encryptionKey) {
        if (encryptionKey != null) {
            DecryptingReadStream decryptingReadStream =
                    new DecryptingReadStream(abortableReadStream, encryptionKey, offset);
            return new ShortCircuitingAbortableReadStream<>(abortableReadStream, decryptingReadStream);
        } else {
            return abortableReadStream;
        }
    }

    Authorizer getAuthorizer() {
        return authorizer;
    }
}
