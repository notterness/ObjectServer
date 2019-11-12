package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.metrics.Annotation;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

/**
 * A generic annotation handler for a Vert.x {@link io.vertx.ext.web.Router}
 * that is used in conjunction with code that supports {@link com.oracle.pic.casper.common.metrics.MetricScope}
 *
 * This handler will inject into the <b>root</b> {@link MetricScope} a user supplied
 * {@link com.oracle.pic.casper.common.metrics.Annotation}.
 * <b>NOTE:</b> This handler must be placed after the root MetricScope hsa been setup.
 *
 * <code>
 *     Router router = ...
 *     router.route().handler(new MetricAnnotationHandler("foo", "bar"));
 * </code>
 *
 */
public class MetricAnnotationHandler implements Handler<RoutingContext> {

    private final Annotation annotation;

    public MetricAnnotationHandler(String name, Object value) {
        this(new Annotation(name, value));
    }

    public MetricAnnotationHandler(Annotation annotation) {
        this.annotation = annotation;
    }

    @Override
    public void handle(RoutingContext context) {
        MetricScope metricScope = WSRequestContext.getMetricScope(context);
        metricScope.annotate(annotation);
        context.next();
    }

}
