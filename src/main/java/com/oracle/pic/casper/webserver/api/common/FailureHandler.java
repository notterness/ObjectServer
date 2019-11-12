package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.ratelimit.Embargo;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * A failure handler for a Vert.x {@link io.vertx.ext.web.Router}.
 * <p>
 * A Vert.x failure handler is installed by calling:
 * <code><pre>
 *     Router router = ...
 *     router.route().failureHandler(new FailureHandler(...));
 * </pre></code>
 * The failure handler's "handle()" method is called whenever a request encounters an exception while being processed.
 * This handler does the following:
 * <ul>
 * <li>Unwraps any CompletionExceptions to get the root cause of the failure.</li>
 * <li>Logs an error with the exception.</li>
 * <li>Annotates the parent metric scope with a "fail": true and with the "throwable" message.</li>
 * <li>Checks if the request has ended; if not, it attempts to: a) send a "Connection: close" header and b) consumes
 * any data for up to the timeout so that the client will receive an HTTP error code instead of a transport error.</li>
 * <li>Calls the given WSExceptionTranslator methods with the exception to build a response.</li>
 * </ul>
 * <p>
 * This handler will write out a response and then call ".end()" on the response object.  There is no code path that
 * does not call ".end()", so responses will never hang.
 */
public class FailureHandler implements Handler<RoutingContext> {
    private static final Logger LOG = LoggerFactory.getLogger(FailureHandler.class);

    private final WSExceptionTranslator wsExceptionTranslator;
    private final Embargo embargo;
    private final Duration closeTimeout;

    public FailureHandler(WSExceptionTranslator wsExceptionTranslator, Embargo embargo, Duration closeTimeout) {
        this.wsExceptionTranslator = wsExceptionTranslator;
        this.embargo = embargo;
        this.closeTimeout = closeTimeout;
    }

    @Override
    public void handle(RoutingContext routingContext) {
        // if it is not running on the event loop thread, let's schedule them to run later to avoid the deadlock
        if (!Context.isOnEventLoopThread()) {
            routingContext.vertx().runOnContext(nothing -> handle(routingContext));
            // stop the execution now as the rest of the execution will be done in the event loop thread
            return;
        }

        final HttpServerRequest request = routingContext.request();
        final HttpServerResponse response = routingContext.response();
        // we are trying to send a response before we've read the entire request
        if (!request.isEnded()) {
            final Vertx vertx = routingContext.vertx();
            final long timeoutID = vertx.setTimer(
                    closeTimeout.toMillis(),
                    ignored -> {
                        LOG.warn("Ignored request bytes because the request failed prematurely.");
                        if (!response.ended()) {
                            // Add a Connection: close header in case the client reads the response before we can
                            // close the connection.  This ensures a well-behaving client that's slow enough to
                            // hit this timeout but fast enough to read the response before we close the connection
                            // receives the HTTP status code instead of a transport error.
                            if (!response.headWritten()) {
                                response.putHeader(HttpHeaders.CONNECTION, "close");
                            }
                            terminate(routingContext, request, response);
                        }
                        response.close();
                    });

            // Deregister the data handler, which can't do anything useful with a closed request,
            // to free any references it holds.
            request.handler(null);
            // Register an end handler that cancels the timeout and terminates the response if it's still active.
            request.endHandler(v -> {
                vertx.cancelTimer(timeoutID);
                // The end handler will be called when the timer above runs.
                if (!response.ended()) {
                    terminate(routingContext, request, response);
                }
            });
            request.resume();
        } else {
            terminate(routingContext, request, response);
        }
    }

    /**
     * Actually terminate the request.
     */
    private void terminate(RoutingContext routingContext, HttpServerRequest request, HttpServerResponse response) {
        final WSRequestContext wsRequestContext = WSRequestContext.safeGetWsRequestContext(routingContext);
        final Throwable throwable = routingContext.failure();
        final Throwable rootCause = wsExceptionTranslator.rewriteException(routingContext, throwable);

        if (throwable == null) {
            LOG.trace("RoutingContext's failure was null with status code {}. rootCause will be a generic internal " +
                    "server error.", routingContext.statusCode());
        } else {
            LOG.trace(ExceptionUtils.getStackTrace(throwable));
        }

        if (wsRequestContext != null) {
            MetricScope metricScope = wsRequestContext.getCommonRequestContext().getMetricScope();
            metricScope.fail(rootCause);
            wsRequestContext.getVisa().ifPresent(embargo::exit);
        }

        if (response.headWritten()) {
            LOG.debug("The response has already been written while trying to write an error out", throwable);
            // Someone else is writing to this response.  They're responsible for calling .end()
            return;
        }

        final int statusCode = wsExceptionTranslator.getHttpResponseCode(rootCause);
        response.setStatusCode(statusCode);

        wsExceptionTranslator.writeResponseErrorHeaders(routingContext, statusCode);

        if (request.method() != HttpMethod.HEAD) {
            final String responseErrorContentType = wsExceptionTranslator.getResponseErrorContentType();
            response
                .putHeader(HttpHeaders.CONTENT_TYPE, responseErrorContentType)
                .headers().remove(HttpHeaders.CONTENT_LENGTH);
            response.end(wsExceptionTranslator.getResponseErrorMessage(rootCause));
        } else {
            response.end();
        }

        if (HttpResponseStatus.isServerError(statusCode)) {
            LOG.error("Returned a server error", rootCause);
        } else if (HttpResponseStatus.isClientError(statusCode)) {
            LOG.debug("Returning error response", rootCause);
        } else {
            LOG.error("Returned an error response with non-error status code:  " + statusCode, throwable);
        }
    }
}
