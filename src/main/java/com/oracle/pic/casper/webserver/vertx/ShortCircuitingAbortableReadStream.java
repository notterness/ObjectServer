package com.oracle.pic.casper.webserver.vertx;

import com.oracle.pic.casper.common.vertx.stream.AbortableReadStream;
import com.oracle.pic.casper.common.vertx.stream.DelegateReadStream;
import io.vertx.core.streams.ReadStream;

import javax.annotation.Nonnull;

/**
 * A {@link ReadStream} delegation chain may not all be {@link AbortableReadStream} however we'd like to
 * support short-circuiting the delegation for abort.
 * This is a simple class where you can either give it a delegate that is abortable or a delegate
 * and the other abortable {@link ReadStream} -- which may be further down the delegation chain.
 */
public class ShortCircuitingAbortableReadStream<T> extends DelegateReadStream<T> implements AbortableReadStream<T> {

    private final AbortableReadStream<T> abortableReadStream;

    public ShortCircuitingAbortableReadStream(@Nonnull AbortableReadStream<T> delegate) {
        super(delegate);
        this.abortableReadStream = delegate;
    }

    public ShortCircuitingAbortableReadStream(@Nonnull AbortableReadStream<T> abortableReadStream,
                                              @Nonnull ReadStream<T> delegate) {
        super(delegate);
        this.abortableReadStream = abortableReadStream;
    }

    @Override
    public void abort() {
        abortableReadStream.abort();
    }
}
