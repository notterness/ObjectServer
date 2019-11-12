package com.oracle.pic.casper.webserver.api.v1;

import com.oracle.pic.casper.common.vertx.handler.HealthCheckHandler;
import com.oracle.pic.casper.common.metrics.Annotation;
import com.oracle.pic.casper.common.metrics.Annotations;
import com.oracle.pic.casper.webserver.api.Api;
import com.oracle.pic.casper.webserver.api.common.CasperAPI;
import com.oracle.pic.casper.webserver.api.common.CasperLoggerHandler;
import com.oracle.pic.casper.webserver.api.common.CommonHandler;
import com.oracle.pic.casper.webserver.api.common.CommonHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.FailureHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.MeteringHandler;
import com.oracle.pic.casper.webserver.api.common.MetricAnnotationHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;

public class CasperApiV1 implements Api {

    public static final String HEALTHCHECK_ROUTE = "/healthcheck";
    public static final String V1_HEALTHCHECK_ROUTE = "/v1[:]healthcheck";
    private static final String SCOPE_ROUTE = "/v1/([^/]+)/?";
    private static final String NS_ROUTE = "/v1/([^/]+)/([^/]+)/?";
    private static final String OBJECT_ROUTE = "/v1/([^/]+)/([^/]+)/(.+)";

    static final String SCOPE_PARAM = "param0";
    static final String NS_PARAM = "param1";
    static final String OBJECT_PARAM = "param2";
    private final ScanNamespacesHandler scanNamespacesHandler;
    private final ScanObjectsHandler scanObjectsHandler;
    private final PutNamespaceHandler putNamespaceHandler;
    private final HeadObjectHandler headObjectHandler;
    private final GetObjectHandler getObjectHandler;
    private final PutObjectHandler putObjectHandler;
    private final DeleteObjectHandler deleteObjectHandler;

    private final CommonHandler commonHandler;
    private final MetricsHandler metricsHandler;
    private final V1ExceptionTranslator v1ExceptionTranslator;
    private final MetricAnnotationHandlerFactory metricAnnotationHandlerFactory;
    private final FailureHandlerFactory failureHandlerFactory;
    //todo remove once fully migrated
    private final HealthCheckHandler healthCheckHandler;
    private final HealthCheckHandler v1HealthCheckHandler;
    private final MeteringHandler meteringHandler;
    private final CasperLoggerHandler loggerHandler;

    public CasperApiV1(MetricsHandler metricsHandler,
                       ScanNamespacesHandler scanNamespacesHandler,
                       ScanObjectsHandler scanObjectsHandler,
                       PutNamespaceHandler putNamespaceHandler,
                       HeadObjectHandler headObjectHandler,
                       GetObjectHandler getObjectHandler,
                       PutObjectHandler putObjectHandler,
                       DeleteObjectHandler deleteObjectHandler,
                       CommonHandlerFactory commonHandlerFactory,
                       V1ExceptionTranslator v1ExceptionTranslator,
                       MetricAnnotationHandlerFactory metricAnnotationHandlerFactory,
                       FailureHandlerFactory failureHandlerFactory,
                       HealthCheckHandler healthCheckHandler,
                       HealthCheckHandler v1HealthCheckHandler,
                       MeteringHandler meteringHandler,
                       CasperLoggerHandler loggerHandler) {
        this.metricsHandler = metricsHandler;
        this.scanNamespacesHandler = scanNamespacesHandler;
        this.scanObjectsHandler = scanObjectsHandler;
        this.putNamespaceHandler = putNamespaceHandler;
        this.headObjectHandler = headObjectHandler;
        this.getObjectHandler = getObjectHandler;
        this.putObjectHandler = putObjectHandler;
        this.deleteObjectHandler = deleteObjectHandler;
        this.commonHandler = commonHandlerFactory.create(false, CasperAPI.V1);
        this.v1ExceptionTranslator = v1ExceptionTranslator;
        this.metricAnnotationHandlerFactory = metricAnnotationHandlerFactory;
        this.failureHandlerFactory = failureHandlerFactory;
        this.healthCheckHandler = healthCheckHandler;
        this.v1HealthCheckHandler = v1HealthCheckHandler;
        this.meteringHandler = meteringHandler;
        this.loggerHandler = loggerHandler;
    }

    @Override
    public Router createRouter(Vertx vertx) {
        Router router = Router.router(vertx);

        router.get(HEALTHCHECK_ROUTE).useNormalisedPath(false).handler(healthCheckHandler);
        router.get(V1_HEALTHCHECK_ROUTE).useNormalisedPath(false).handler(v1HealthCheckHandler);

        router.route().useNormalisedPath(false).handler(commonHandler);
        router.route().useNormalisedPath(false).handler(loggerHandler);

        //Record metered requests/bandwidth for everything other than delete
        router.route().useNormalisedPath(false).handler(meteringHandler);

        router.route().useNormalisedPath(false).handler(metricsHandler);
        Annotation application = Annotations.application(CasperApiV1.class);
        router.route().useNormalisedPath(false).handler(metricAnnotationHandlerFactory.create(application));

        router.route().useNormalisedPath(false).failureHandler(failureHandlerFactory.create(v1ExceptionTranslator));

        router.getWithRegex(SCOPE_ROUTE).useNormalisedPath(false).handler(scanNamespacesHandler);
        router.getWithRegex(NS_ROUTE).useNormalisedPath(false).handler(scanObjectsHandler);
        router.putWithRegex(NS_ROUTE).useNormalisedPath(false).handler(putNamespaceHandler);
        router.headWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(headObjectHandler);
        router.getWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(getObjectHandler);
        router.putWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(putObjectHandler);
        router.deleteWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(deleteObjectHandler);

        return router;
    }
}
