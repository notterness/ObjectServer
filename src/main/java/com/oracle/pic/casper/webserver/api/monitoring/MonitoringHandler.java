package com.oracle.pic.casper.webserver.api.monitoring;

import com.oracle.pic.casper.common.config.v2.PublicTelemetryConfiguration;
import com.oracle.pic.casper.webserver.api.common.FailureHandler;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoringHandler implements Handler<RoutingContext> {
    private static Logger log = LoggerFactory.getLogger(MonitoringHandler.class);

    private final MonitoringHelper monitoringHelper;
    private final PublicTelemetryConfiguration config;

    public MonitoringHandler(MonitoringHelper monitoringHelper, PublicTelemetryConfiguration configuration) {
        this.monitoringHelper = monitoringHelper;
        this.config = configuration;
    }

    /**
     * The handler that records public metric information.
     *
     * Similar to Metrics handler, this <b>must</b> come before any other handler in the
     * {@link io.vertx.ext.web.Router} in order
     * to handle any exceptions that are thrown before the {@link FailureHandler} gets it.
     * Metrics are collected in the addBodyEndHandler to get the total latency.
     * @param context
     */
    @Override
    public void handle(RoutingContext context) {

        if (config.isWsMetricsEnabled()) {
            final WSRequestContext wsRequestContext = WSRequestContext.get(context);
            wsRequestContext.pushEndHandler(c -> {
                if (!WSRequestContext.get(context).getWebServerMonitoringMetric().isPresent()) {
                    String operation = WSRequestContext.get(context).getOperation().isPresent() ?
                            WSRequestContext.get(context).getOperation().get().getSummary() : "Unknown Operation";
                    log.debug("Request does not contain a public metric to be logged for operation {}",
                            operation);
                    return;
                }
                monitoringHelper.logMonitoringMetric(context);
            });
        }
        context.next();
    }
}
