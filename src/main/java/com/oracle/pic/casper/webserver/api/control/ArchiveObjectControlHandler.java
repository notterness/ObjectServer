package com.oracle.pic.casper.webserver.api.control;

import com.oracle.pic.casper.common.exceptions.AlreadyArchivedObjectException;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * Move an object to archive-tier storage.  Will handle many objects at a time in the future.
 */
public class ArchiveObjectControlHandler extends SyncHandler {

    private final Backend backend;

    public ArchiveObjectControlHandler(Backend backend) {
        this.backend = backend;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_ARCHIVE_OBJECTS_BUNDLE);
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.ARCHIVEOBJECT_REQUEST_COUNT);

        final String namespaceName = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);
        final String ifMatch = HttpMatchHelpers.getIfMatchHeader(request);

        try {
            backend.archiveObject(context, namespaceName, bucketName, objectName, ifMatch);
        } catch (AlreadyArchivedObjectException ex) {
            // we ignore AlreadyArchivedObjectException, just return 202 on multiple archive request
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
        // Returns 202 on archive request
        response.setStatusCode(HttpResponseStatus.ACCEPTED).end();
    }
}
