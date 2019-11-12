package com.oracle.pic.casper.webserver.api.common;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

/**
 * A Vert.x {@link Handler} that returns a {@link CompletableFuture}.
 *
 * CompletableFuture is a convenience for Vert.x Handler's that internally use CompletableFuture, and it correctly
 * handles exceptions passed through the completable futures by routing them to a failure handler.
 *
 * To use this class, simply extend from it and implement the {@link #handleCompletably(RoutingContext)} method in the
 * subclass, returning a CompletableFuture.  You should use this class when your business logic needs to do work on the
 * event loop, e.g. pumping data from one stream to another.
 */
public abstract class CompletableHandler implements Handler<RoutingContext> {
    @Override
    public final void handle(RoutingContext context) {
        handleCompletably(context)
                .exceptionally(throwable -> {
                    context.fail(throwable);
                    return null;
                });
    }

    public abstract CompletableFuture<Void> handleCompletably(RoutingContext context);
}
