package com.oracle.pic.casper.webserver.vertx;

import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.common.vertx.stream.AbortableReadStream;
import com.oracle.pic.casper.common.vertx.stream.BlobReadStream;
import com.oracle.pic.casper.common.vertx.stream.DefaultBlobReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * A {@link BlobReadStream} that can also abort another {@link ReadStream} down the delegation chain.
 */
public class DefaultAbortableBlobReadStream extends DefaultBlobReadStream implements AbortableBlobReadStream {

    private final AbortableReadStream<Buffer> abortableReadStream;

    public DefaultAbortableBlobReadStream(DigestAlgorithm algorithm, Optional<Digest> expectedDigest,
                                          long contentLength, @Nonnull AbortableReadStream<Buffer> delegate) {
        super(algorithm, expectedDigest, contentLength, delegate);
        this.abortableReadStream = delegate;
    }

    public DefaultAbortableBlobReadStream(DigestAlgorithm algorithm, Optional<Digest> expectedDigest,
                                          long contentLength, @Nonnull AbortableReadStream<Buffer> abortableReadStream,
                                   @Nonnull ReadStream<Buffer> delegate) {
        super(algorithm, expectedDigest, contentLength, delegate);
        this.abortableReadStream = abortableReadStream;
    }

    @Override
    public void abort() {
        abortableReadStream.abort();
    }
}
