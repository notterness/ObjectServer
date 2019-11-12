package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.vertx.VertxUtil;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

/**
 * A Vert.x handler that runs entirely on a single worker thread.
 *
 * This handler is meant for requests that have no HTTP body, and require processing that would otherwise block the
 * event loop. Use SyncBodyHandler for requests that include an HTTP body that is small enough to be read entirely into
 * memory.
 *
 * This class is named "SyncHandler" because classes extending it are free to make synchronous, blocking, API calls.
 * It is asynchronous from the perspective of the event loop; the {@link VertxUtil#runAsync(Runnable)} method called
 * below returns immediately without blocking the event loop.
 */
public abstract class SyncHandler implements Handler<RoutingContext> {
    @Override
    public void handle(RoutingContext context) {
        VertxUtil.runAsync(() -> handleSync(context))
                .exceptionally(throwable -> {
                    context.fail(throwable);
                    return null;
                });
    }

    public abstract void handleSync(RoutingContext context);
}
