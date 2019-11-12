package com.oracle.pic.casper.webserver.api.common;

import com.google.common.annotations.VisibleForTesting;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

/**
 * A Vert.x handler that executes actions asynchronously after reading in the body of the request.
 *
 * You should make your handler extend from this class if it needs to both read a small request body entirely into
 * memory and it also needs to do work on the event loop, e.g. pumping data from one stream to another.
 *
 * This class is named "AsyncBodyHandler" because classes extending from it are free to make asynchronous, non-blocking,
 * API calls that go off of the event loop.  Except for reading the request body, the business logic is synchronous
 * from the perspective of the event loop; {@link #handleCompletably(RoutingContext, Buffer)} is called _on_ the event
 * loop.
 */
public abstract class AsyncBodyHandler implements Handler<RoutingContext>, CountingHandler.FailureHandler {

    private final CountingHandler.MaximumContentLength contentLengthLimiter;

    public AsyncBodyHandler(CountingHandler.MaximumContentLength contentLengthLimiter) {
        this.contentLengthLimiter = contentLengthLimiter;
    }

    /**
     * Handle the request.  This might fail a request because its
     * stated Content-length exceeds the current maximum or because it
     * eventually sends more than the maximum.  See validateContentLength.
     *
     * @param context The routing context containing the request to consider.
     */
    public final void handle(RoutingContext context) {
        validateHeaders(context);

        final CompletableFuture<Buffer> bodyReady = new CompletableFuture<>();
        final CountingHandler bufferHandler = new CountingHandler(this, context, contentLengthLimiter);
        context.request().handler(bufferHandler);
        context.request().endHandler(x -> {
            if (!context.failed()) {
                Buffer received = bufferHandler.getReceived();
                bodyReady.complete(received);
            }
        });

        bodyReady
                .thenComposeAsync(buffer -> handleCompletably(context, buffer), VertxExecutor.eventLoop())
                .exceptionally(throwable -> {
                    context.fail(throwable);
                    return null;
                });
    }


    /**
     * Validate the headers of the request before the bytes of the body are read.
     * <p>
     * Any exceptions thrown here are handled via the Vert.x router chain.
     *
     * @param context the Vert.x request context
     */
    protected abstract void validateHeaders(RoutingContext context);

    /**
     * Compose further actions on the provided buffer. Any exceptions thrown here are handled via the Vert.x router
     * chain.
     *
     * @param context the Vert.x request context.
     * @param buffer  byte buffer.
     */
    protected abstract CompletableFuture<Void> handleCompletably(RoutingContext context,
                                                                 Buffer buffer);

    @VisibleForTesting
    public CountingHandler.MaximumContentLength getContentLengthLimiter() {
        return contentLengthLimiter;
    }
}
