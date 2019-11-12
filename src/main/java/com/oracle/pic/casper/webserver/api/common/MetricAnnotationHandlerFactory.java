package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.metrics.Annotation;

/**
 * A simple interface to help Guice create a {@link MetricAnnotationHandler}
 * with some of the constructor arguments provided by the user
 */
public interface MetricAnnotationHandlerFactory {

    MetricAnnotationHandler create(Annotation annotation);

    MetricAnnotationHandler create(String key, Object value);
}
