package com.oracle.pic.casper.storageclient.impl;

import com.google.inject.Inject;
import com.oracle.pic.casper.common.model.StripeDefinition;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.storageclient.AthenaVolumeStorageClient;
import com.oracle.pic.casper.storageclient.SCRequestContext;
import com.oracle.pic.casper.storageclient.VolumeStorageClient;
import com.oracle.pic.casper.storageclient.core.AthenaPutRequestHandler;
import com.oracle.pic.casper.storageclient.core.AthenaRequestHandlerFactory;
import com.oracle.pic.casper.storageclient.core.DeleteRequestHandler;
import com.oracle.pic.casper.storageclient.core.GetRequestHandler;
import com.oracle.pic.casper.storageclient.core.PutRequestHandler;
import com.oracle.pic.casper.storageclient.core.RequestHandlerFactory;
import com.oracle.pic.casper.storageclient.core.replicated.ReplicatedReconstructRequestHandler;
import com.oracle.pic.casper.storageclient.erasure.codec.Codec;
import com.oracle.pic.casper.storageclient.models.DeleteResult;
import com.oracle.pic.casper.storageclient.models.PutResult;
import com.oracle.pic.casper.storageclient.models.VolumeStorageContext;
import com.oracle.pic.casper.webserver.api.v2.Chunk;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The storage client that talks to storage servers to get/put/delete blobs. At this point, the caller should
 * already have the information which volume id and von id it needs to interact with. This implementation will
 * be responsible for fetching the volume metadata to find out:
 * - What type of the volume it so that it can use the right implementation of the request handler
 * - What storage servers are the members of this volume so that it can talk to the right servers.
 * <p>
 * Then, it will use {@code RequestHandlerFactory} to construct the proper {@code RequestHandler} that will
 * handle the request and then invoke it.
 */
public class AthenaVolumeStorageClientImpl implements AthenaVolumeStorageClient {
    private static final Logger logger = LoggerFactory.getLogger(VolumeStorageClientImpl.class);

    private final AthenaRequestHandlerFactory requestHandlerFactory;
    private final Map<StripeDefinition, Codec> codecs = new ConcurrentHashMap<>();

    /**
     * Instantiates a new {@code VolumeStorageClientImpl}.
     *
     * @param requestHandlerFactory the request handler factory
     */
    @Inject
    public AthenaVolumeStorageClientImpl(AthenaRequestHandlerFactory requestHandlerFactory) {
        this.requestHandlerFactory = requestHandlerFactory;
    }

    private Codec getCodec(VolumeStorageContext volumeContext) {
        return codecs.computeIfAbsent(volumeContext.getStripeDefinition(), Codec::create);
    }

    @Override
    public CompletableFuture<AbortableBlobReadStream> get(SCRequestContext context,
                                                          VolumeStorageContext volumeContext,
                                                          @Nullable ByteRange byteRange,
                                                          long contentLength) {
        logger.trace("Getting {}/{}", volumeContext.getVolumeId(), volumeContext.getVon());
        GetRequestHandler getRequestHandler = requestHandlerFactory.newGetRequestHandler();
        return getRequestHandler.get(context, volumeContext, getCodec(volumeContext), byteRange, contentLength, false);
    }

    @Override
    public PutResult put(SCRequestContext context,
                                            VolumeStorageContext volumeContext,
                                            Chunk chunk) {
        logger.trace("Putting {}/{}", volumeContext.getVolumeId(), volumeContext.getVon());
        AthenaPutRequestHandler putRequestHandler = requestHandlerFactory.newPutRequestHandlerAthena();
        return putRequestHandler.put(context, volumeContext, getCodec(volumeContext), chunk);
    }

    @Override
    public CompletableFuture<DeleteResult> delete(SCRequestContext context, VolumeStorageContext volumeContext) {
        logger.trace("Deleting {}/{}", volumeContext.getVolumeId(), volumeContext.getVon());
        DeleteRequestHandler deleteRequestHandler = requestHandlerFactory.newDeleteRequestHandler();
        return deleteRequestHandler.delete(context, volumeContext);
    }

    @Override
    public CompletableFuture<Void> reconstruct(SCRequestContext context, VolumeStorageContext volumeContext,
                                               long contentLength,
                                               boolean[] missingBlocks) {
        logger.trace("Reconstructing {}/{}", volumeContext.getVolumeId(), volumeContext.getVon());
        ReplicatedReconstructRequestHandler handler = new ReplicatedReconstructRequestHandler(
                requestHandlerFactory.newGetRequestHandler(),
                requestHandlerFactory.newPutRequestHandler());
        return handler.reconstruct(context, volumeContext, getCodec(volumeContext), contentLength, missingBlocks);
    }
}
