package com.oracle.pic.casper.webserver.api.common;

import com.google.common.annotations.VisibleForTesting;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

/**
 * A Vert.x handler that runs entirely on a single worker thread after reading in the body of the request.
 *
 * This class is meant for requests that include an HTTP request body that is small enough to be read entirely into
 * memory.
 *
 * Sub-classes must implement the validateHeaders method to check headers before reading the body, but this class will
 * assert that the received data does not exceed the provided maximum content length.  Sub-classes must implement
 * failureRuntimeException to represent an over-long body to their context's failure handler.
 *
 * This class is named "SyncBodyHandler" because classes extending it are free to make synchronous, blocking, API calls.
 * It is asynchronous from the perspective of the event loop; the {@link VertxUtil#runAsync(Runnable)} method called
 * below returns immediately without blocking the event loop.
 */
public abstract class SyncBodyHandler implements Handler<RoutingContext>, CountingHandler.FailureHandler {

    private final CountingHandler.MaximumContentLength contentLengthLimiter;

    public SyncBodyHandler(CountingHandler.MaximumContentLength contentLengthLimiter) {
        this.contentLengthLimiter = contentLengthLimiter;
    }

    /**
     * Handle the request.  This might fail a request because its
     * stated Content-length exceeds the current maximum or because it
     * eventually sends more than the maximum.  See validateContentLength.
     *
     * @param context The routing context containing the request to consider.
     */
    @Override
    public void handle(RoutingContext context) {
        validateHeaders(context);

        final CompletableFuture<Buffer> bodyReady = new CompletableFuture<>();
        bodyReady
                .thenCompose(buffer ->
                        VertxUtil.runAsync(() -> handleBody(context, buffer.getBytes()))
                )
                .exceptionally(throwable -> {
                    context.fail(throwable);
                    return null;
                });

        final CountingHandler bufferHandler = new CountingHandler(this, context, contentLengthLimiter);
        context.request().handler(bufferHandler);
        context.request().endHandler(x -> {
            if (!context.failed()) {
                Buffer received = bufferHandler.getReceived();
                bodyReady.complete(received);
            }
        });
    }


    /**
     * Validate the headers of the request before the bytes of the body are read.
     *
     * Any exceptions thrown here are handled via the Vert.x router chain.
     *
     * @param context the Vert.x request context
     */
    protected abstract void validateHeaders(RoutingContext context);

    /**
     * Handle the bytes of the body.
     *
     * Any exceptions thrown here are handled via the Vert.x router chain.
     *
     * @param context the Vert.x request context
     * @param bytes   the bytes of the HTTP request body
     */
    public abstract void handleBody(RoutingContext context, byte[] bytes);

    @VisibleForTesting
    public CountingHandler.MaximumContentLength getContentLengthLimiter() {
        return contentLengthLimiter;
    }
}
