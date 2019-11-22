package com.oracle.pic.casper.storageclient.core;

import com.oracle.pic.casper.storageclient.SCRequestContext;
import com.oracle.pic.casper.storageclient.erasure.codec.Codec;
import com.oracle.pic.casper.storageclient.models.PutResult;
import com.oracle.pic.casper.storageclient.models.VolumeStorageContext;
import com.oracle.pic.casper.webserver.api.v2.Chunk;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import java.util.concurrent.CompletableFuture;

/**
 * Request handler to handle PUT operation.
 */
public interface AthenaPutRequestHandler {
    /**
     * Puts an object to the given volumeId/vonId.
     *
     * @param scRequestContext Storage client request context
     * @param volumeContext context for the volume to which the data will be written
     * @param codec a Codec appropriate to the volume type
     * @param chunk the chunk
     * @param contentLength the content length of data to write
     * @return a completable future
     */
    PutResult put(SCRequestContext scRequestContext,
                                     VolumeStorageContext volumeContext,
                                     Codec codec,
                                     Chunk chunk);

    /**
     * A put operations that allows to override minimum successful block writes count.
     * Only for reconciliation path!
     */
    PutResult put(SCRequestContext scRequestContext,
                                     VolumeStorageContext volumeContext,
                                     Codec codec,
                                     Chunk chunk,
                                     Integer minimumWritesOverride);
}

