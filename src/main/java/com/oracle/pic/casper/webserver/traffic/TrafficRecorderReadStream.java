package com.oracle.pic.casper.webserver.traffic;

import com.oracle.pic.casper.common.vertx.stream.ComposingReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import javax.annotation.Nonnull;

/**
 * TrafficRecorderReadStream is a Vert.x ReadStream that updates a per-context TrafficRecorder when a Buffer arrives.
 */
public final class TrafficRecorderReadStream extends ComposingReadStream<Buffer> {
    private final String rid;

    public TrafficRecorderReadStream(@Nonnull ReadStream<Buffer> delegate, String rid) {
        super(delegate);
        this.rid = rid;
    }

    @Override
    protected Buffer before(Buffer buf) {
        TrafficRecorder.getVertxRecorder().bufferArrived(rid, buf.length());
        return buf;
    }
}
