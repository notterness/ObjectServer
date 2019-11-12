package com.oracle.pic.casper.webserver.vertx;

import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.vertx.HttpServerUtil;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.common.vertx.stream.DefaultBlobReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.streams.ReadStream;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * A wrapper around {@link HttpServerRequest} to provide checksum functionality and expose itself as
 * {@link AbortableBlobReadStream}.
 *
 * Note: this class disables the automatic MD5 and size validations done by the DefaultBlobReadStream, so clients of
 * this class must do their own validation.
 */
public class HttpServerRequestReadStream extends DefaultBlobReadStream implements AbortableBlobReadStream {

    private final HttpServerRequest request;

    public HttpServerRequestReadStream(DigestAlgorithm algorithm,
                                       Optional<Digest> expectedDigest,
                                       long contentLength,
                                       @Nonnull HttpServerRequest request,
                                       @Nonnull ReadStream<Buffer> delegate) {
        super(algorithm, expectedDigest, contentLength, delegate, false);
        this.request = request;
    }

    @Override
    public void abort() {
        HttpServerUtil.abort(request);
    }
}
