package com.oracle.pic.casper.webserver.api.control;

import com.oracle.pic.casper.common.metrics.Annotation;
import com.oracle.pic.casper.common.metrics.Annotations;
import com.oracle.pic.casper.common.vertx.handler.HealthCheckHandler;
import com.oracle.pic.casper.common.vertx.handler.JmxHandler;
import com.oracle.pic.casper.webserver.api.Api;
import com.oracle.pic.casper.webserver.api.common.CasperAPI;
import com.oracle.pic.casper.webserver.api.common.CommonHandler;
import com.oracle.pic.casper.webserver.api.common.CommonHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.FailureHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.MetricAnnotationHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.NotFoundHandler;
import com.oracle.pic.casper.webserver.api.monitoring.MonitoringHandler;
import com.oracle.pic.casper.webserver.api.v2.V2ExceptionTranslator;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.LoggerHandler;

import static com.oracle.pic.casper.webserver.api.control.ControlConstants.HEALTHCHECK_ROUTE;

public class CasperApiControl implements Api {

    private final CommonHandler commonHandler;
    private final V2ExceptionTranslator v2ExceptionTranslator;
    private final MetricAnnotationHandlerFactory metricAnnotationHandlerFactory;
    private final FailureHandlerFactory failureHandlerFactory;
    private final HealthCheckHandler healthCheckHandler;
    private final RefreshVolumeMetadataControlHandler refreshVolumeMetadataControlHandler;
    private final GetVmdChangeSeqHandler getVmdChangeSeqHandler;
    private final EmbargoControllers embargoControllers;
    private final JmxHandler jmxHandler;
    private final ListBucketsControlHandler listBucketsHandler;
    private final ListObjectsControlHandler listObjectsHandler;
    private final ListNamespacesControlHandler listNamespacesHandler;
    private final ArchiveObjectControlHandler archiveObjectControlHandler;
    private final MetricsHandler metricsHandler;
    private final NotFoundHandler notFoundHandler;
    private final CheckObjectHandler checkObjectHandler;
    private final MonitoringHandler monitoringHandler;

    public CasperApiControl(CommonHandlerFactory commonHandlerFactory,
                            V2ExceptionTranslator v2ExceptionTranslator,
                            MetricAnnotationHandlerFactory metricAnnotationHandlerFactory,
                            FailureHandlerFactory failureHandlerFactory,
                            HealthCheckHandler healthCheckHandler,
                            RefreshVolumeMetadataControlHandler refreshVolumeMetadataControlHandler,
                            GetVmdChangeSeqHandler getVmdChangeSeqHandler,
                            EmbargoControllers embargoControllers,
                            JmxHandler jmxHandler,
                            ListBucketsControlHandler listBucketsHandler,
                            ListObjectsControlHandler listObjectsHandler,
                            ListNamespacesControlHandler listNamespacesHandler,
                            ArchiveObjectControlHandler archiveObjectControlHandler,
                            MetricsHandler metricsHandler,
                            NotFoundHandler notFoundHandler,
                            CheckObjectHandler checkObjectHandler,
                            MonitoringHandler monitoringHandler) {
        this.commonHandler = commonHandlerFactory.create(false, CasperAPI.CONTROL);
        this.v2ExceptionTranslator = v2ExceptionTranslator;
        this.metricAnnotationHandlerFactory = metricAnnotationHandlerFactory;
        this.failureHandlerFactory = failureHandlerFactory;
        this.healthCheckHandler = healthCheckHandler;
        this.refreshVolumeMetadataControlHandler = refreshVolumeMetadataControlHandler;
        this.getVmdChangeSeqHandler = getVmdChangeSeqHandler;
        this.embargoControllers = embargoControllers;
        this.jmxHandler = jmxHandler;
        this.listBucketsHandler = listBucketsHandler;
        this.listObjectsHandler = listObjectsHandler;
        this.listNamespacesHandler = listNamespacesHandler;
        this.archiveObjectControlHandler = archiveObjectControlHandler;
        this.metricsHandler = metricsHandler;
        this.notFoundHandler = notFoundHandler;
        this.checkObjectHandler = checkObjectHandler;
        this.monitoringHandler = monitoringHandler;
    }

    @Override
    public Router createRouter(Vertx vertx) {
        Router router = Router.router(vertx);

        router.route().useNormalisedPath(false).handler(LoggerHandler.create());
        router.get(HEALTHCHECK_ROUTE).useNormalisedPath(false).handler(healthCheckHandler);

        router.route().handler(CorsHandler.create("*")
                .allowedMethod(HttpMethod.GET)
                .allowedMethod(HttpMethod.POST)
                .allowedHeader("*"));

        router.route().useNormalisedPath(false).handler(commonHandler);

        router.route().useNormalisedPath(false).handler(metricsHandler);
        Annotation application = Annotations.application(CasperApiControl.class);
        router.route().useNormalisedPath(false).handler(metricAnnotationHandlerFactory.create(application));

        router.route().useNormalisedPath(false).failureHandler(failureHandlerFactory.create(v2ExceptionTranslator));

        router.route().useNormalisedPath(false).handler(monitoringHandler);

        router.post(ControlConstants.REFRESH_VOLUME_METADATA).handler(refreshVolumeMetadataControlHandler);
        router.get(ControlConstants.GET_VMD_CHANGE_SEQ).handler(getVmdChangeSeqHandler);
        router.get(ControlConstants.EMBARGO_URI + "/refresh").handler(embargoControllers.new RefreshRulesController());
        router.route(ControlConstants.JMX_URI + "/*").handler(jmxHandler);
        //The unrestricted list APIs are useful to be able to walk the whole object list in a region
        router.getWithRegex(ControlConstants.UNRESTRICTED_NAMESPACE_COLLECTION_URI).useNormalisedPath(false)
                .handler(listNamespacesHandler);
        router.getWithRegex(ControlConstants.UNRESTRICTED_BUCKET_COLLECTION_URI).useNormalisedPath(false)
                .handler(listBucketsHandler);
        router.getWithRegex(ControlConstants.UNRESTRICTED_OBJECT_COLLECTION_URI).useNormalisedPath(false)
                .handler(listObjectsHandler);
        router.postWithRegex(ControlConstants.UNRESTRICTED_ARCHIVE_OBJECT_URI).useNormalisedPath(false)
                .handler(archiveObjectControlHandler);

        // Check object hashes
        router.getWithRegex(ControlConstants.CHECK_OBJECT_ROUTE).useNormalisedPath(false)
                .handler(checkObjectHandler);


        // This catches requests that don't have a configured handler, so it must be the last handler in the chain.
        router.route().handler(notFoundHandler);

        return router;
    }
}
