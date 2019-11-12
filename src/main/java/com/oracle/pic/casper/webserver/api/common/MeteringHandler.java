package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.metering.MeteringHelper;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

import java.util.Optional;

/**
 * A common metering handler that is in charge of recording requests and bandwidth to a metering log.
 */
public class MeteringHandler implements Handler<RoutingContext> {

    private final MeteringHelper meteringHelper;

    public MeteringHandler(MeteringHelper meteringHelper) {
        this.meteringHelper = meteringHelper;
    }

    /**
     * The handler that records metering information <b>after</b> passing the context.
     * This handler <b>must</b> come before any other handler in the {@link io.vertx.ext.web.Router} in order
     * to handle any exceptions that are thrown before the {@link FailureHandler} gets it.
     * @param context
     */
    @Override
    public void handle(RoutingContext context) {
        //Both metering must be registered on the body end handler for cases such as
        //PUT object that may throw during body handling.
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.pushEndHandler(c -> {
            final Optional<CasperOperation> optOperation = wsRequestContext.getOperation();
            final CasperOperation operation = optOperation.orElse(null);
            if (MeteringHelper.isBillableHttpMethod(context.request()) &&
                    MeteringHelper.isBillableException(context.failure()) &&
                    MeteringHelper.isBillableOperation(operation)) {
                meteringHelper.logMeteredBandwidth(context);
                meteringHelper.logMeteredRequest(context);
            }
        });
        context.next();
    }
}
