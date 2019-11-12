package com.oracle.pic.casper.webserver.api.common;

import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.oracle.pic.casper.common.healthcheck.HealthCheckStatus;
import com.oracle.pic.casper.common.healthcheck.HealthChecker;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

/**
 * A simple web handler that will set HealthCheck to false, and, optionally, shutdown JVM after a specified delay.
 * <p>
 * It should be hooked up to Router using this code:
 * <p>
 * <pre>
 * <code>
 *     router.get("/shutdown").handle(shutdownHandler);
 * </code>
 * </pre>
 * Disabled for now. This was disabled as a part of CASPER-5088. This handler opens us to a possiblity of DDOS,
 * we should add auth before enabling it again.
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "DM_EXIT")
@Singleton
public class ShutdownHandler extends SyncHandler {

    public static final String DELAY_PARAMETER = "after";
    public static final Duration MAX_DELAY = Duration.ofHours(24);

    private static final Logger logger = LoggerFactory.getLogger(ShutdownHandler.class);
    private final HealthChecker healthChecker;

    private final TimerTask systemExitTask = new TimerTask() {
        public void run() {
            System.exit(0);
        }
    };

    @Inject
    public ShutdownHandler(HealthChecker healthChecker) {
        this.healthChecker = healthChecker;
    }

    @Override
    public void handleSync(RoutingContext context) {
        logger.warn("Shutdown request");

        // If "after" parameter was provided, will call System.exit(0) after the specified delay.
        String when = context.request().getParam(DELAY_PARAMETER);
        if (when != null) {
            Integer secondsToShutdown = Ints.tryParse(when);
            if (secondsToShutdown == null) {
                context.response().setStatusCode(400).end("Bad request.");
                return;
            }

            Duration shutdownDelay = Duration.ofSeconds(secondsToShutdown);

            if (shutdownDelay.isNegative() || shutdownDelay.isZero() || shutdownDelay.compareTo(MAX_DELAY) > 0) {
                context.response().setStatusCode(400).end("Bad request.  Invalid shutdown delay.");
                return;
            }

            Date shutdownDate = new Date(Instant.now().plus(shutdownDelay).toEpochMilli());
            String message = "Shutting down at " + SimpleDateFormat.getDateTimeInstance().format(shutdownDate);

            healthChecker.declareStatus(new HealthCheckStatus(HealthCheckStatus.NOT_OK_CODE, message));

            logger.warn(message);

            Timer shutdownTimer = new Timer();
            shutdownTimer.schedule(systemExitTask, shutdownDelay.toMillis());

            context.response().setStatusCode(200).end(message);
        } else {
            logger.warn("Healthcheck set to false");
            healthChecker.declareStatus(new HealthCheckStatus(HealthCheckStatus.NOT_OK_CODE, "Healthcheck disabled."));
            context.response().setStatusCode(200).end("Healthcheck disabled.");
        }
    }
}
