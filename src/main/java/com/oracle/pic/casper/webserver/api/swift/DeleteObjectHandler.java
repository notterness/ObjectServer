package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

/**
 * Deletes an object.
 *
 * Swift implementation notes:
 *
 * Swift never returns any object-specific headers on DELETE object calls.
 * Swift ignores If-Match and If-None-Match headers; sending them does *not* result in 400.  We return 400.
 * Swift does not return the X-Timestamp header on DELETE object calls, but it's easier for us to include it, so we do.
 *
 * http://developer.openstack.org/api-ref-objectstorage-v1.html#deleteObject
 */
public class DeleteObjectHandler extends SyncHandler {
    private static final String NOT_FOUND_MSG = "<html><h1>Not Found</h1>" +
            "<p>The resource could not be found.</p></html>";

    private final Authenticator authenticator;
    private final Backend backend;
    private final TenantBackend tenantBackend;
    private final SwiftResponseWriter swiftResponseWriter;
    private final EmbargoV3 embargoV3;

    public DeleteObjectHandler(
            Authenticator authenticator,
            Backend backend,
            TenantBackend tenantBackend,
            SwiftResponseWriter swiftResponseWriter,
            EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.tenantBackend = tenantBackend;
        this.swiftResponseWriter = swiftResponseWriter;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.SWIFT_DELETE_OBJECT_BUNDLE);
        WSRequestContext.setOperationName("swift", getClass(), context, CasperOperation.DELETE_OBJECT);

        final HttpServerRequest request = context.request();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.DELETEOBJECT_REQUEST_COUNT);

        final String namespace = SwiftHttpPathHelpers.getNamespace(request, wsRequestContext);
        final String containerName = SwiftHttpPathHelpers.getContainerName(request, wsRequestContext);
        final String objectName = SwiftHttpPathHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.Swift)
            .setOperation(CasperOperation.DELETE_OBJECT)
            .setNamespace(namespace)
            .setBucket(containerName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoOperation);

        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.NO,
                HttpMatchHelpers.IfNoneMatchAllowed.NO);
        try {
            final String tenantOcid = tenantBackend.getNamespace(context, scope, new NamespaceKey(Api.V2, namespace))
                    .map(MdsNamespace::getTenantOcid)
                    .orElse(null);
            final AuthenticationInfo authInfo =
                    authenticator.authenticateSwiftOrCavage(context, null, namespace, tenantOcid);
            backend.deleteV2Object(
                    context, authInfo, namespace, containerName, objectName, null)
                    .orElseThrow(() -> new HttpException(V2ErrorCode.OBJECT_NOT_FOUND, NOT_FOUND_MSG, request.path()));
            swiftResponseWriter.writeEmptyResponse(context, ContentType.TEXT_HTML_UTF8);
        } catch (Exception ex) {

            throw SwiftHttpExceptionHelpers.rewrite(context, ex);
        }
    }
}
