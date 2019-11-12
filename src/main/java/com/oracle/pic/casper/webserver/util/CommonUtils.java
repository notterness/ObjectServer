package com.oracle.pic.casper.webserver.util;

import com.google.common.base.Ticker;
import com.google.common.testing.FakeTicker;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import io.vertx.ext.web.RoutingContext;

import java.util.UUID;

/**
 * A class to share common code for various APIs.
 */
public final class CommonUtils {

    public static final long DEFAULT_TIMER_PERIOD_IN_MS = 15000;

    public static final String DEFAULT_KEEP_ALIVE_TEXT = "\r\n";

    private static final Ticker FAKE_TICKER = new FakeTicker();
    private static final Ticker SYSTEM_TICKER = Ticker.systemTicker();


    private CommonUtils() {
    }

    /**
     * Creates a vert.x timer that keeps connection alive by periodically writing to the context response.
     * Also sets the end handler of the response to cancel the timer. Uses default timer period and keep alive text.
     *
     * @param context       the context whose response will be periodically updated
     * @param timerCallback the function to be called every time timer handler is invoked.
     */
    public static void startKeepConnectionAliveTimer(final RoutingContext context,
                                                     final TimerCallback timerCallback) {
        startKeepConnectionAliveTimer(context, DEFAULT_KEEP_ALIVE_TEXT, DEFAULT_TIMER_PERIOD_IN_MS, timerCallback);
    }

    /**
     * Creates a vert.x timer that keeps connection alive by periodically writing to the context response.
     * Also sets the end handler of the response to cancel the timer.
     *
     * @param context            the context whose response will be periodically updated
     * @param text               the text that will be periodically written to context response
     * @param timePeriodInMillis periodic time in milliseconds to write to context response
     */
    public static void startKeepConnectionAliveTimer(final RoutingContext context,
                                                     final String text,
                                                     final long timePeriodInMillis) {
        startKeepConnectionAliveTimer(context, text, timePeriodInMillis, null);
    }

    /**
     * Creates a vert.x timer that keeps connection alive by periodically writing to the context response.
     * Also sets the end handler of the response to cancel the timer.
     *
     * @param context            the context whose response will be periodically updated
     * @param text               the text that will be periodically written to context response
     * @param timePeriodInMillis periodic time in milliseconds to write to context response
     * @param timerCallback      the function to be called every time timer handler is invoked.
     */
    public static void startKeepConnectionAliveTimer(final RoutingContext context,
                                                     final String text,
                                                     final long timePeriodInMillis,
                                                     final TimerCallback timerCallback) {
        long timerId = context.vertx().setPeriodic(timePeriodInMillis, id -> {
            if (timerCallback != null) {
                timerCallback.execute();
            }
            try {
                context.response().write(text);
            } catch (Exception e) {
                /* Ignore the exception. May occur if the response is closed. */
            }
        });
        WSRequestContext.get(context).pushEndHandler(connectionClosed -> context.vertx().cancelTimer(timerId));
    }

    public interface TimerCallback {
        void execute();
    }

    /**
     * Generates a new etag for a new bucket, new object, updated object, etc.
     * @return the new ETag
     */
    public static String generateETag() {
        return UUID.randomUUID().toString();
    }

    // It is used to facilitate the bucket cache tests
    public static Ticker getTicker(WebServerFlavor flavor) {
        if (flavor == WebServerFlavor.STANDARD) {
            return SYSTEM_TICKER;
        } else {
            return FAKE_TICKER;
        }
    }
}
