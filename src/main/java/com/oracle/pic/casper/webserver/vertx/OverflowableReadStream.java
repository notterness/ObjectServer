package com.oracle.pic.casper.webserver.vertx;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import java.util.Optional;

/**
 * {@code OverflowableReadStream} is a ReadStream that can "overflow". Interface invariants:
 * - if hasEnded() is false, getOverflowBuffer() always returns empty,
 * - if hasEnded() is true, getOverflowBuffer will return non-empty if there is an overflow and empty otherwise.
 */
public interface OverflowableReadStream extends ReadStream<Buffer> {
    /**
     * @return The overflow buffer if the delegate stream was larger than the capacity.
     */
    Optional<Buffer> getOverflowBuffer();

    /**
     * @return true if this stream has ended and false otherwise.
     */
    boolean hasEnded();
}
