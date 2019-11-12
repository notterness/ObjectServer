package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;

import javax.measure.Measure;
import javax.measure.quantity.DataAmount;
import javax.measure.unit.NonSI;

/**
 * A Vert.x handler that only accepts new data if the total amount of received data is less than the maximum content
 * length.
 */
public class CountingHandler implements Handler<Buffer> {

    private final MaximumContentLength contentLengthLimiter;
    private final Buffer received = Buffer.buffer();
    private final FailureHandler handler;
    private final RoutingContext context;

    CountingHandler(FailureHandler handler, RoutingContext context, MaximumContentLength contentLengthLimiter) {
        this.handler = handler;
        this.context = context;
        this.contentLengthLimiter = contentLengthLimiter;
        this.validateContentLength(context);
    }

    @Override
    public void handle(Buffer arrived) {
        if ((long) received.length() + (long) arrived.length() > contentLengthLimiter.maximum) {
            try {
                throw handler.failureRuntimeException(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, context);
            } catch (Throwable thrown) {
                context.fail(thrown);
            }
        } else if (!context.failed()) {
            received.appendBuffer(arrived);
        }
    }

    /**
     * Attempt to parse the Content-length header, if any, into a
     * long.  If the header is not present, SyncBodyHandler assumes
     * the request will use chunked transfer encoding and relies on
     * CountingHandler to fail requests that have exceeded the maximum
     * Content-length.
     *
     * @param context The routing context containing the request to consider.
     * @throws Throwable
     */
    private void validateContentLength(RoutingContext context) {
        String rawContentLength = context.request().getHeader(HttpHeaders.CONTENT_LENGTH);
        if (rawContentLength == null) {
            return;
        }

        long contentLength;
        try {
            contentLength = Long.parseLong(rawContentLength);
        } catch (NumberFormatException e) {
            throw handler.failureRuntimeException(HttpResponseStatus.BAD_REQUEST, context);
        }

        if (contentLength > contentLengthLimiter.maximum) {
            throw handler.failureRuntimeException(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, context);
        }
    }

    public Buffer getReceived() {
        return received;
    }

    /**
     * A class that shares the maximum content between Handler instances and any configuration updaters.
     */
    public static class MaximumContentLength {
        private volatile long maximum;

        public MaximumContentLength(Measure<DataAmount> maximum) {
            setMaximum(maximum);
        }

        public void setMaximum(Measure<DataAmount> maximum) {
            this.maximum = maximum.longValue(NonSI.BYTE);
        }

        public Measure<DataAmount> getMaximum() {
            return Measure.valueOf(maximum, NonSI.BYTE);
        }
    }

    public interface FailureHandler {
        /**
         * Create a RuntimeException that will represent the provided status code to an installed failure handler.
         *
         * @param statusCode The status code to represent to the failure handler.
         * @param context
         * @return A RuntimeException that will fail the request context.
         */

        RuntimeException failureRuntimeException(int statusCode, RoutingContext context);
    }
}
