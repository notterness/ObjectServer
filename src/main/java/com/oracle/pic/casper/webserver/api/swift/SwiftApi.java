package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.metrics.Annotation;
import com.oracle.pic.casper.common.metrics.Annotations;
import com.oracle.pic.casper.common.vertx.handler.HealthCheckHandler;
import com.oracle.pic.casper.webserver.api.Api;
import com.oracle.pic.casper.webserver.api.auditing.AuditFilterHandler;
import com.oracle.pic.casper.webserver.api.common.CasperLoggerHandler;
import com.oracle.pic.casper.webserver.api.common.CommonHandler;
import com.oracle.pic.casper.webserver.api.common.FailureHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.HeaderCorsHandler;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MeteringHandler;
import com.oracle.pic.casper.webserver.api.common.MetricAnnotationHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.NotFoundHandler;
import com.oracle.pic.casper.webserver.api.common.OptionCorsHandler;
import com.oracle.pic.casper.webserver.api.common.URIValidationHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHandler;
import com.oracle.pic.casper.webserver.api.monitoring.MonitoringHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoHandler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class SwiftApi implements Api {

    private static final String INFO_ROUTE = "/info/?";
    private static final String ENDPOINTS_ROUTE = "/v1/endpoints/?";
    // TODO(jfriedly):  Figure out allowable characters for account, container, and object names.
    // Make this accept only the subset of characters that are allowable both in Swift and in Casper.
    // Their docs say that containers names can't have slashes in them, but I couldn't find any other docs.
    private static final String ACCOUNT_ROUTE = "/v1/([^/]+)/?";
    private static final String CONTAINER_ROUTE = "/v1/([^/]+)/([^/]+)/?";
    private static final String OBJECT_ROUTE = "/v1/([^/]+)/([^/]+)/(.+)";
    private static final String HEALTHCHECK_ROUTE = "/healthcheck";
    private static final String SWIFT_HEALTHCHECK_ROUTE = "/swift[:]healthcheck";

    public static final String ACCOUNT_PARAM = "param0";
    public static final String CONTAINER_PARAM = "param1";
    public static final String OBJECT_PARAM = "param2";
    public static final String PREFIX_PARAM = "prefix";


    private final ListContainersHandler listContainersHandler;
    private final PostContainerHandler postContainerHandler;
    private final HeadContainerHandler headContainerHandler;
    private final PutContainerHandler putContainerHandler;
    private final DeleteContainerHandler deleteContainerHandler;
    private final ListObjectsHandler listObjectsHandler;
    private final PutObjectHandler putObjectHandler;
    private final GetObjectHandler getObjectHandler;
    private final HeadObjectHandler headObjectHandler;
    private final DeleteObjectHandler deleteObjectHandler;
    private final HeadAccountHandler headAccountHandler;
    private final BulkDeleteHandler bulkDeleteHandler;

    private final CommonHandler commonHandler;
    private final MetricsHandler metricsHandler;
    private final SwiftExceptionTranslator swiftExceptionTranslator;
    private final MetricAnnotationHandlerFactory metricAnnotationHandlerFactory;
    private final MeteringHandler meteringHandler;
    private final FailureHandlerFactory failureHandlerFactory;
    //todo: remove once fully migrated
    private final HealthCheckHandler healthCheckHandler;
    private final HealthCheckHandler swiftHealthCheckHandler;
    private final OptionCorsHandler optionsCorsHandler;
    private final HeaderCorsHandler headerCorsHandler;
    private final NotFoundHandler notFoundHandler;
    private final CasperLoggerHandler loggerHandler;
    private final EmbargoHandler embargoHandler;
    private final AuditFilterHandler auditFilterHandler;
    private final URIValidationHandler uriValidationHandler;
    private final MonitoringHandler monitoringHandler;
    private final ServiceLogsHandler serviceLogsHandler;

    public SwiftApi(ListContainersHandler listContainersHandler,
                    PostContainerHandler postContainerHandler,
                    HeadContainerHandler headContainerHandler,
                    PutContainerHandler putContainerHandler,
                    DeleteContainerHandler deleteContainerHandler,
                    ListObjectsHandler listObjectsHandler,
                    PutObjectHandler putObjectHandler,
                    GetObjectHandler getObjectHandler,
                    HeadObjectHandler headObjectHandler,
                    DeleteObjectHandler deleteObjectHandler,
                    HeadAccountHandler headAccountHandler,
                    BulkDeleteHandler bulkDeleteHandler,
                    CommonHandler commonHandler,
                    MetricsHandler metricsHandler,
                    SwiftExceptionTranslator swiftExceptionTranslator,
                    MetricAnnotationHandlerFactory metricAnnotationHandlerFactory,
                    MeteringHandler meteringHandler,
                    FailureHandlerFactory failureHandlerFactory,
                    HealthCheckHandler healthCheckHandler,
                    HealthCheckHandler swiftHealthCheckHandler,
                    OptionCorsHandler optionsCorsHandler,
                    HeaderCorsHandler headerCorsHandler,
                    NotFoundHandler notFoundHandler,
                    CasperLoggerHandler loggerHandler,
                    EmbargoHandler embargoHandler,
                    AuditFilterHandler auditFilterHandler,
                    URIValidationHandler uriValidationHandler,
                    MonitoringHandler monitoringHandler,
                    ServiceLogsHandler serviceLogsHandler) {
        this.listContainersHandler = listContainersHandler;
        this.postContainerHandler = postContainerHandler;
        this.headContainerHandler = headContainerHandler;
        this.putContainerHandler = putContainerHandler;
        this.deleteContainerHandler = deleteContainerHandler;
        this.listObjectsHandler = listObjectsHandler;
        this.putObjectHandler = putObjectHandler;
        this.getObjectHandler = getObjectHandler;
        this.headObjectHandler = headObjectHandler;
        this.deleteObjectHandler = deleteObjectHandler;
        this.headAccountHandler = headAccountHandler;
        this.bulkDeleteHandler = bulkDeleteHandler;
        this.commonHandler = commonHandler;
        this.metricsHandler = metricsHandler;
        this.swiftExceptionTranslator = swiftExceptionTranslator;
        this.metricAnnotationHandlerFactory = metricAnnotationHandlerFactory;
        this.meteringHandler = meteringHandler;
        this.failureHandlerFactory = failureHandlerFactory;
        this.healthCheckHandler = healthCheckHandler;
        this.swiftHealthCheckHandler = swiftHealthCheckHandler;
        this.optionsCorsHandler = optionsCorsHandler;
        this.headerCorsHandler = headerCorsHandler;
        this.notFoundHandler = notFoundHandler;
        this.loggerHandler = loggerHandler;
        this.embargoHandler = embargoHandler;
        this.auditFilterHandler = auditFilterHandler;
        this.uriValidationHandler = uriValidationHandler;
        this.monitoringHandler = monitoringHandler;
        this.serviceLogsHandler = serviceLogsHandler;
    }

    @Override
    public Router createRouter(Vertx vertx) {
        Router router = Router.router(vertx);

        router.get(HEALTHCHECK_ROUTE).useNormalisedPath(false).handler(healthCheckHandler);
        router.get(SWIFT_HEALTHCHECK_ROUTE).useNormalisedPath(false).handler(swiftHealthCheckHandler);

        // the execution order for headerEndsHandler has been reversed in Vert.x 3.3.
        router.options().useNormalisedPath(false).handler(optionsCorsHandler);
        router.route().useNormalisedPath(false).handler(headerCorsHandler);

        router.route().useNormalisedPath(false).handler(commonHandler);
        router.route().useNormalisedPath(false).handler(loggerHandler);

        router.route().useNormalisedPath(false).handler(meteringHandler);

        router.route().useNormalisedPath(false).failureHandler(failureHandlerFactory.create(swiftExceptionTranslator));
        router.route().useNormalisedPath(false).handler(metricsHandler);
        Annotation application = Annotations.application(SwiftApi.class);
        router.route().useNormalisedPath(false).handler(metricAnnotationHandlerFactory.create(application));

        //refuse requests that match embargo rules
        router.route().useNormalisedPath(false).handler(embargoHandler);
        router.route().useNormalisedPath(false).handler(auditFilterHandler);

        // validate URI before Vert.x parses it and matches against regex
        router.route().useNormalisedPath(false).handler(uriValidationHandler);

        //Monitoring - public metrics
        router.route().useNormalisedPath(false).handler(monitoringHandler);

        //Service logs
        router.route().useNormalisedPath(false).handler(serviceLogsHandler);

        router.getWithRegex(INFO_ROUTE).useNormalisedPath(false).handler(this::throwNotImplemented);
        router.getWithRegex(ENDPOINTS_ROUTE).useNormalisedPath(false).handler(this::throwNotImplemented);

        router.headWithRegex(ACCOUNT_ROUTE).useNormalisedPath(false).handler(headAccountHandler);
        router.postWithRegex(ACCOUNT_ROUTE).useNormalisedPath(false).handler(bulkDeleteHandler);
        router.deleteWithRegex(ACCOUNT_ROUTE).useNormalisedPath(false).handler(bulkDeleteHandler);
        router.headWithRegex(ACCOUNT_ROUTE).useNormalisedPath(false).handler(this::throwNotImplemented);
        router.getWithRegex(ACCOUNT_ROUTE).useNormalisedPath(false).handler(listContainersHandler);

        router.putWithRegex(CONTAINER_ROUTE).useNormalisedPath(false).handler(putContainerHandler);
        router.postWithRegex(CONTAINER_ROUTE).useNormalisedPath(false).handler(postContainerHandler);
        router.headWithRegex(CONTAINER_ROUTE).useNormalisedPath(false).handler(headContainerHandler);
        router.deleteWithRegex(CONTAINER_ROUTE).useNormalisedPath(false).handler(deleteContainerHandler);
        router.getWithRegex(CONTAINER_ROUTE).useNormalisedPath(false).handler(listObjectsHandler);

        router.putWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(putObjectHandler);
        router.postWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(this::throwNotImplemented);
        // TODO(jfriedly):  Make Vert.x accept HTTP method COPY and throwNotImplemented.
        router.headWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(headObjectHandler);
        router.getWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(getObjectHandler);
        router.deleteWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(deleteObjectHandler);

        // This catches requests that don't have a configured handler, so it must be the last handler in the chain.
        router.route().handler(notFoundHandler);

        return router;
    }

    private void throwNotImplemented(RoutingContext context) {
        throw new HttpException(V2ErrorCode.NOT_IMPLEMENTED,
            "This Swift implementation does not support this operation.", context.request().path());
    }
}
