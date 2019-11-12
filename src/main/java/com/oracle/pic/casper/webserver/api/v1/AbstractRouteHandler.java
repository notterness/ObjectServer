package com.oracle.pic.casper.webserver.api.v1;

import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

/**
 * Base class for webserver route handlers.  Route handlers all extend from this class, though there's some other
 * abstraction present for similar routes.
 * <p/>
 * Subclasses must implement {@link #subclassHandle(RoutingContext)}, which will be called when basic request filtering
 * is complete.  Further filtering may be done by implementations of subclassHandle.
 */
abstract class AbstractRouteHandler implements Handler<RoutingContext> {

    /**
     * All arguments to this constructor and subclass constructors are expected to be stateless objects.
     * If they aren't stateless, we'll have problems.
     */
    AbstractRouteHandler() {
    }

    /**
     * No-op wrapper for the Vert.x failure mechanism to make it work as a CompletableFuture target
     * <p/>
     * Note:  This method should not do any work!  An exception raised in this method will cause the client to never
     * get a response, not even an error response, and this can be very hard to debug.
     *
     * @param routingContext Vert.x context to be failed
     * @param t              Throwable that caused the failure
     * @return null, so that this method can be used in a CompletableFuture.exceptionally() parameter
     */
    protected static Void fail(RoutingContext routingContext, Throwable t) {
        routingContext.fail(t);
        return null;
    }

    /**
     * Implement Vert.x {@link Handler} interface.
     * <p/>
     * This method only does basic, universal request filtering; it forwards requests on to subclasses that handle
     * business logic.
     */
    @Override
    public void handle(RoutingContext routingContext) {
        WSRequestContext.setOperationName(Api.V1.getVersion(), getClass(), routingContext, null);
        subclassHandle(routingContext);
    }

    /**
     * Subclasses MUST override this method to perform their business logic.
     */
    protected abstract void subclassHandle(RoutingContext routingContext);
}
