package com.oracle.pic.casper.storageclient;

import com.oracle.pic.casper.common.exceptions.InternalServerErrorException;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.storageclient.models.DeleteResult;
import com.oracle.pic.casper.storageclient.models.PutResult;
import com.oracle.pic.casper.storageclient.models.VolumeStorageContext;
import com.oracle.pic.casper.webserver.api.v2.Chunk;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Interface to put / get / delete objects from volume servers.
 * A particular volume lives on many storage servers.
 * This interface abstracts that detail from the clients and exposes a single logical view of a
 * Volume.
 */
public interface AthenaVolumeStorageClient {

    /**
     * Get an object from a volume.
     *
     * @param context SCRequestContext for this request
     * @param volumeContext the volume context for this get
     * @param byteRange The byte range to request
     * @param contentLength expected contentLength
     * @return Future that holds a reference to a BlobReadStream from which the object can be read.
     * @throws InternalServerErrorException if it fails to get the blob from the server
     */
    CompletableFuture<AbortableBlobReadStream> get(SCRequestContext context,
                                                   VolumeStorageContext volumeContext,
                                                   @Nullable ByteRange byteRange,
                                                   long contentLength);

    /**
     * Put an object into a volume. Internally, the object will be put onto all the relevant
     * storage servers.
     *
     * @param context SCRequestContext for this request
     * @param volumeContext the volume context for this put
     * @param chunk Stream to read the object bytes from.
     * @return Future that holds a reference to the PutResult.
     * @throws InternalServerErrorException if it fails to put the blob to enough number of servers
     */
    PutResult put(SCRequestContext context,
                                     VolumeStorageContext volumeContext,
                                     Chunk chunk);

    /**
     * Delete an object from the given volume.
     *
     * @param context SCRequestContext for this request
     * @param volumeContext the volume context for this delete
     * @return Future that holds a reference to the DeleteResult.
     * @throws InternalServerErrorException if it fails to delete the blob from enough number of servers
     */
    CompletableFuture<DeleteResult> delete(SCRequestContext context, VolumeStorageContext volumeContext);

    /**
     * Reconstructs blocks that are marked as missing.
     *
     * @param context SCRequestContext for this request
     * @param volumeContext the volume context
     * @param contentLength the length of content
     * @param missingBlocks boolean array that defines missing blocks
     * @return a Future
     */
    CompletableFuture<Void> reconstruct(SCRequestContext context,
                                        VolumeStorageContext volumeContext,
                                        long contentLength,
                                        boolean[] missingBlocks);
}
