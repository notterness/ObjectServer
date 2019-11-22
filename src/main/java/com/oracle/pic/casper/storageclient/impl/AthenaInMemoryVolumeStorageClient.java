package com.oracle.pic.casper.storageclient.impl;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Singleton;
import com.oracle.oci.casper.jopenssl.CryptoMessageDigest;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.encryption.DigestUtils;
import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.common.exceptions.ObjectNotFoundException;
import com.oracle.pic.casper.common.model.Stripe;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.common.vertx.stream.BlobReadStream;
import com.oracle.pic.casper.common.vertx.stream.ByteBackedReadStream;
import com.oracle.pic.casper.common.vertx.stream.DefaultBlobReadStream;
import com.oracle.pic.casper.common.vertx.stream.RangeReadStream;
import com.oracle.pic.casper.storageclient.AthenaVolumeStorageClient;
import com.oracle.pic.casper.storageclient.models.BlobInfo;
import com.oracle.pic.casper.storageclient.models.DeleteResult;
import com.oracle.pic.casper.common.host.HostInfo;
import com.oracle.pic.casper.storageclient.models.PutResult;
import com.oracle.pic.casper.storageclient.models.ObjectsSummary;
import com.oracle.pic.casper.storageclient.SCRequestContext;
import com.oracle.pic.casper.storageclient.StorageServerClient;
import com.oracle.pic.casper.storageclient.VolumeStorageClient;
import com.oracle.pic.casper.storageclient.models.VolumeStorageContext;
import com.oracle.pic.casper.storageclient.models.WebSocketProgressHandler;
import com.oracle.pic.casper.storageclient.vertx.ForwardingWriteStream;
import com.oracle.pic.casper.storagecommon.models.ObjectStatus;
import com.oracle.pic.casper.webserver.api.v2.Chunk;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * An in memory implementation of the VolumeStorageClient interface.
 * This exists to allow quick integration and unit testing from the webserver.
 * <p>
 * For now, this class does not much error checking or md5 calculation etc.
 * <p>
 * Note: This class is not thread safe. It assumes single threaded access.
 */
@Singleton
public class AthenaInMemoryVolumeStorageClient implements AthenaVolumeStorageClient, StorageServerClient {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryVolumeStorageClient.class);

    private int allowedPutsBeforeFailure = -1;
    private final Multimap<HostInfo, InMemoryExtent> extents = ArrayListMultimap.create();

    @Inject
    public AthenaInMemoryVolumeStorageClient() {
    }

    @Override
    public synchronized CompletableFuture<AbortableBlobReadStream> get(SCRequestContext context,
                                                                       VolumeStorageContext volumeContext,
                                                                       @Nullable ByteRange byteRange,
                                                                       long contentLength) {

        final List<HostInfo> hostInfos = volumeContext.getHostInfos();

        //Shuffle the storage servers
        Collections.shuffle(hostInfos);
        HostInfo randomStorageServerInfo = hostInfos.get(0);

        return getObject(context, randomStorageServerInfo.getHostname(), randomStorageServerInfo.getPort(),
                volumeContext.getVolumeId(), volumeContext.getVon(), byteRange);
    }

    @Override
    public synchronized PutResult put(SCRequestContext context,
                                                         VolumeStorageContext volumeContext,
                                                         Chunk chunk) {
        if (allowedPutsBeforeFailure == 0) {
            throw new RuntimeException("put fail - allowedPutsBeforeFailure is 0");
        } else if (allowedPutsBeforeFailure > 0) {
            allowedPutsBeforeFailure--;
        }
//        CryptoMessageDigest digest = DigestUtils.messageDigestFromAlgorithm(DigestAlgorithm.SHA256);
  //      byte[] data = digest.digest(chunk.getData());
        byte[] data = chunk.getData();
        volumeContext.getHostInfos().forEach(storageServerInfo -> {
            InMemoryExtent extent = getOrCreateExtent(storageServerInfo, volumeContext.getVolumeId());
            extent.putObject(volumeContext.getVon(), data);
        });
        return new PutResult(data.length);
    }

    @Override
    public CompletableFuture<DeleteResult> delete(SCRequestContext context, VolumeStorageContext volumeContext) {
        CompletableFuture<DeleteResult> resultFuture = new CompletableFuture<>();

        boolean exists = true;
        for (HostInfo hostInfo : volumeContext.getHostInfos()) {
            InMemoryExtent extent = getOrCreateExtent(hostInfo, volumeContext.getVolumeId());
            exists &= extent.deleteObject(volumeContext.getVon());
        }
        resultFuture.complete(new DeleteResult(exists));
        return resultFuture;
    }

    @Override
    public CompletableFuture<Void> reconstruct(SCRequestContext context, VolumeStorageContext volumeContext,
                                               long contentLength, boolean[] missingBlocks) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    private synchronized InMemoryExtent getOrCreateExtent(HostInfo hostInfo, long volumeId) {
        InMemoryExtent extent = extents.get(hostInfo).stream()
                .filter(ex -> ex.getVolumeId() == volumeId)
                .findFirst()
                .orElse(InMemoryExtent.create(volumeId));
        extents.put(hostInfo, extent);
        return extent;
    }

    private AbortableBlobReadStream getStream(DigestAlgorithm algorithm,
                                              Optional<Digest> expectedDigest,
                                              long contentLength,
                                              @Nonnull ReadStream<Buffer> stream) {
        return new DoNothingAbortableBlobReadStream(algorithm, expectedDigest, contentLength, stream);
    }

    @Override
    public synchronized CompletableFuture<ObjectNode> getVolumeInfo(SCRequestContext requestContext,
                                                                    String host,
                                                                    int port,
                                                                    long volumeId) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    @Override
    public synchronized CompletableFuture<ArrayNode> getAllVolumeInfos(SCRequestContext requestContext,
                                                                       String host,
                                                                       int port) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    @Override
    public synchronized CompletableFuture<AbortableBlobReadStream> getObject(SCRequestContext requestContext,
                                                                             String host,
                                                                             int port,
                                                                             long volumeId,
                                                                             int vonId,
                                                                             @Nullable ByteRange byteRange) {
        HostInfo info = extents.keys().stream()
                .filter(hostInfo -> hostInfo.getHostname().equals(host) && hostInfo.getPort() == port)
                .findFirst()
                .orElse(new HostInfo(host, port, 0));

        InMemoryExtent extent = getOrCreateExtent(info, volumeId);

        CompletableFuture<AbortableBlobReadStream> streamFuture = new CompletableFuture<>();
        if (!extent.hasObject(vonId)) {
            streamFuture.completeExceptionally(new ObjectNotFoundException());
            return streamFuture;
        }

        byte[] blob = extent.getBlob(vonId);
        ReadStream<Buffer> objectReadStream = new ByteBackedReadStream(blob);

        DigestAlgorithm algorithm = (DigestAlgorithm.SHA256);

        if (byteRange != null) {
            if (byteRange.getStart() >= blob.length) {
                streamFuture.completeExceptionally(new BadRequestException("Incorrect range"));
                return streamFuture;
            }
            long objectLength = blob.length;
            byteRange = new ByteRange(
                    byteRange.getStart(),
                    Math.min(objectLength - 1, byteRange.getEnd())
            );
            RangeReadStream rangeReadStream = new RangeReadStream(objectReadStream, byteRange.getStart(),
                    byteRange.getEnd() + 1);
            long expectedContentLength = byteRange.getLength();
            streamFuture.complete(getStream(algorithm, Optional.empty(), expectedContentLength, rangeReadStream));
        } else {
            streamFuture.complete(getStream(algorithm, Optional.empty(), blob.length, objectReadStream));
        }
        return streamFuture;
    }

    @Override
    public synchronized CompletableFuture<PutResult> putObject(SCRequestContext requestContext, String host,
                                                               int port, long volumeId, int vonId,
                                                               long contentLength,
                                                               CompletableFuture<Void> putContinue,
                                                               ForwardingWriteStream forwardingWriteStream) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    @Override
    public synchronized CompletableFuture<Void> deleteObject(SCRequestContext requestContext,
                                                             String host, int port,
                                                             long volumeId, int vonId) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    @Override
    public CompletableFuture<Void> deleteTombstonedObjects(SCRequestContext requestContext,
                                                           String host, int controlPort,
                                                           long volumeId, List<Integer> vonIds, int volumeGeneration) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    @Override
    public synchronized void syncCopyObject(SCRequestContext requestContext, String sourceHost,
                                            int sourceControlPort, String targetHost, int targetAppPort,
                                            long volumeId, int vonId,
                                            WebSocketProgressHandler webSocketProgressHandler) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    @Override
    public void reconcileVolume(SCRequestContext requestContext, String host, int controlPort, long volumeId,
                                int volumeGeneration, int tombstoneDeletionCount, Stripe<HostInfo> storageServers,
                                WebSocketProgressHandler webSocketProgressHandler) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    @Override
    public synchronized CompletableFuture<Void> createExtent(SCRequestContext requestContext,
                                                             String host, int controlPort,
                                                             long volumeId, long volumeSize, String diskPath) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    @Override
    public synchronized CompletableFuture<Void> restoreExtentInfo(SCRequestContext requestContext,
                                                                  String host, int controlPort,
                                                                  long volumeId, long volumeSize, int volumeGeneration,
                                                                  String diskUuid) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    @Override
    public synchronized CompletableFuture<Void> deleteExtent(SCRequestContext requestContext,
                                                             String host, int controlPort,
                                                             long volumeId, int volumeGeneration) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    @Override
    public synchronized CompletableFuture<Void> setVolumeGeneration(SCRequestContext requestContext,
                                                                    String host, int controlPort,
                                                                    long volumeId, int volumeGeneration) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    @Override
    public synchronized CompletableFuture<BlobInfo> headObject(SCRequestContext requestContext, String host,
                                                               int port, long volumeId, int vonId) {
        HostInfo info = extents.keys().stream()
                .filter(hostInfo -> hostInfo.getHostname().equals(host) && hostInfo.getPort() == port)
                .findFirst()
                .orElse(new HostInfo(host, port, 0));

        InMemoryExtent extent = getOrCreateExtent(info, volumeId);

        byte[] data = extent.getBlob(vonId);
        long contentLength = data.length;
        Digest digest = DigestUtils.fromBytes(DigestAlgorithm.SHA256, data);
        BlobInfo blobInfo = new BlobInfo(contentLength, Optional.of(digest));
        return CompletableFuture.completedFuture(blobInfo);
    }

    @Override
    public synchronized CompletableFuture<ObjectNode> getStatus(SCRequestContext requestContext, String hostName,
                                                                int controlPort) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<ObjectsSummary> getObjectsSummary(SCRequestContext requestContext,
                                                               String hostName, int controlPort,
                                                               long volumeId, ObjectStatus... objectStatuses) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    @Override
    public void reconcileObject(@Nullable SCRequestContext requestContext, String host, int controlPort,
                                long volumeId, int vonId, Stripe<HostInfo> serversStripe,
                                boolean[] serversMissingDataMask, WebSocketProgressHandler progressHandler) {
        throw new UnsupportedOperationException("This has not been implemented for in-memory yet");
    }

    private static class DoNothingAbortableBlobReadStream extends DefaultBlobReadStream
            implements AbortableBlobReadStream {

        DoNothingAbortableBlobReadStream(DigestAlgorithm algorithm, Optional<Digest> expectedDigest,
                                              long contentLength, @Nonnull ReadStream<Buffer> delegate) {
            super(algorithm, expectedDigest, contentLength, delegate);
        }

        @Override
        public void abort() {
            //do nothing
        }
    }

    public Multimap<HostInfo, InMemoryExtent> getExtents() {
        return extents;
    }
}
