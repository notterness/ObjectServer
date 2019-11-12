package com.oracle.pic.casper.webserver.api.common;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An adaptor for converting a Vert.x body handler into a CompletableFuture.
 *
 * This is meant to be used with the Vert.x {@link HttpServerRequest#bodyHandler(Handler)} method to provide a
 * CompletableFuture that is completed when the body handler has been executed. The CompletableFuture will contain the
 * result of the body handler, or an exception if one was thrown.
 *
 * Here is an example of using the static helper method to convert a body handler to a CompletableFuture:
 *
 * <code>
 *     HttpServerRequest request = ...;
 *     CompletableBodyHandler.bodyHandler(request, buffer -> {
 *         // ... do some stuff ...
 *         return "my value here";
 *     });
 * </code>
 *
 * Unlike {@link CompletableHandler}, this class is _not_ meant to be used as a base class from which other handlers
 * will extend.  It's a {@link Handler<Buffer>}, not a {@link Handler<io.vertx.ext.web.RoutingContext>}.
 *
 * @param <R> the return type for the body handler.
 */
public class CompletableBodyHandler<R> implements Handler<Buffer> {
    private final Function<Buffer, R> handler;
    private final CompletableFuture<R> completableFuture;

    public static <R> CompletableFuture<R> bodyHandler(HttpServerRequest request, Function<Buffer, R> handler) {
        CompletableFuture<R> completableFuture = new CompletableFuture<>();
        request.bodyHandler(new CompletableBodyHandler<>(handler, completableFuture));
        return completableFuture;
    }

    public CompletableBodyHandler(Function<Buffer, R> handler, CompletableFuture<R> completableFuture) {
        this.handler = handler;
        this.completableFuture = completableFuture;
    }

    @Override
    public void handle(Buffer buffer) {
        try {
            completableFuture.complete(handler.apply(buffer));
        } catch (Throwable t) {
            completableFuture.completeExceptionally(t);
        }
    }
}
