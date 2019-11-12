package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.NotAuthenticatedException;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * (Kind of) show account metadata
 *
 * We do not completely adhere to the Swift specification. The official documentation requires that this API return
 * metadata for the account including: the number of containers (buckets), the number of objects, and the total number
 * of bytes stored in the account. We do not return any of these values.
 *
 * Instead, we simply authenticate the user and return. This is necessary to support users (cough, cough, PSM, cough)
 * who rely on using this API to verify credentials.
 *
 * https://developer.openstack.org/api-ref/object-storage/?expanded=show-account-metadata-detail#show-account-metadata
 */
public class HeadAccountHandler extends SyncHandler {

    private final Authenticator authenticator;
    private final TenantBackend tenantBackend;
    private final SwiftResponseWriter responseWriter;
    private final EmbargoV3 embargoV3;

    public HeadAccountHandler(
            Authenticator authenticator,
            TenantBackend tenantBackend,
            SwiftResponseWriter responseWriter,
            EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.tenantBackend = tenantBackend;
        this.responseWriter = responseWriter;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.SWIFT_HEAD_ACCOUNT_BUNDLE);
        WSRequestContext.setOperationName("swift", getClass(), context, CasperOperation.GET_NAMESPACE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        scope.annotate("operation", CasperOperation.GET_NAMESPACE.getSummary());

        final String namespace = SwiftHttpPathHelpers.getNamespace(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.Swift)
            .setOperation(CasperOperation.GET_NAMESPACE)
            .setNamespace(namespace)
            .build();
        embargoV3.enter(embargoOperation);

        // Although HEAD responses have no body, the header should include CONTENT_TYPE as if body is returned
        final SwiftResponseFormat format = SwiftHttpContentHelpers.readResponseFormat(request);
        final String formatStr = format == SwiftResponseFormat.JSON ?
                ContentType.APPLICATION_JSON_UTF8 : ContentType.APPLICATION_XML_UTF8;

        // Inform clients that we don't support range requests.
        response.putHeader(HttpHeaders.ACCEPT_RANGES, "none");

        try {
            final String tenantOcid = tenantBackend.getNamespace(context, scope, new NamespaceKey(Api.V2, namespace))
                    .map(MdsNamespace::getTenantOcid)
                    .orElse(null);
            AuthenticationInfo authenticationInfo =
                    authenticator.authenticateSwiftOrCavage(context, null, namespace, tenantOcid);
            if (authenticationInfo.equals(AuthenticationInfo.ANONYMOUS_USER)) {
                throw new NotAuthenticatedException(null);
            }
            wsRequestContext.setCompartmentID(tenantOcid);
            responseWriter.writeEmptyResponse(context, formatStr);
        } catch (Exception ex) {
            throw SwiftHttpExceptionHelpers.rewrite(context, ex);
        }
    }
}
