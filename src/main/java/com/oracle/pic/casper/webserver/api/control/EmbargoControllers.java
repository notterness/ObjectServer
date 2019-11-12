package com.oracle.pic.casper.webserver.api.control;

import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.CachingEmbargoRuleCollectorImpl;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoRuleCollector;
import io.vertx.ext.web.RoutingContext;

/**
 * Collection of controllers for Embargo related activities
 */
public class EmbargoControllers {

    public class RefreshRulesController extends SyncHandler {

        @Override
        public void handleSync(RoutingContext context) {
            if (ruleCollector instanceof CachingEmbargoRuleCollectorImpl) {
                CachingEmbargoRuleCollectorImpl cachingCollector = (CachingEmbargoRuleCollectorImpl) ruleCollector;
                cachingCollector.refresh();
                context.response().setStatusCode(HttpResponseStatus.OK).end("Refreshed.");
            } else {
                context.response().setStatusCode(HttpResponseStatus.NOT_MODIFIED).end();
            }
        }
    }

    private final EmbargoRuleCollector ruleCollector;

    public EmbargoControllers(EmbargoRuleCollector ruleCollector) {
        this.ruleCollector = ruleCollector;
    }

}
