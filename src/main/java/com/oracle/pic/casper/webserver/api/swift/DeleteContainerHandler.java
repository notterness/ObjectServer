package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.NotAuthenticatedException;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

/**
 * Creates a container.
 *
 * A PUT operation is idempotent, it either creates a container or updates an existing one as appropriate.
 *
 */
public class DeleteContainerHandler extends SyncHandler {

    private final Authenticator authenticator;
    private final BucketBackend bucketBackend;
    private final TenantBackend tenantBackend;
    private final SwiftResponseWriter responseWriter;
    private final EmbargoV3 embargoV3;

    public DeleteContainerHandler(Authenticator authenticator,
                               BucketBackend bucketBackend,
                               TenantBackend tenantBackend,
                               SwiftResponseWriter responseWriter,
                               EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.bucketBackend = bucketBackend;
        this.tenantBackend = tenantBackend;
        this.responseWriter = responseWriter;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName("swift", getClass(), context, CasperOperation.DELETE_BUCKET);
        MetricsHandler.addMetrics(context, WebServerMetrics.SWIFT_DELETE_CONTAINER_BUNDLE);

        final HttpServerRequest request = context.request();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = SwiftHttpPathHelpers.getNamespace(request, wsRequestContext);
        final String containerName = SwiftHttpPathHelpers.getContainerName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.Swift)
            .setOperation(CasperOperation.DELETE_BUCKET)
            .setNamespace(namespace)
            .setBucket(containerName)
            .build();
        embargoV3.enter(embargoOperation);

        try {
            final String tenantOcid = tenantBackend.getNamespace(context, scope, new NamespaceKey(Api.V2, namespace))
                    .map(MdsNamespace::getTenantOcid)
                    .orElse(null);

            // TODO: figure out the wording for this error
            if (tenantOcid == null) {
                throw new NotAuthenticatedException("UnAuthorized Access");
            }
            final AuthenticationInfo authInfo =
                    authenticator.authenticateSwiftOrCavage(context, null, namespace, tenantOcid);
            bucketBackend.deleteBucket(context, authInfo, namespace, containerName, null);

            responseWriter.writeCreatedResponse(context);

        } catch (Exception ex) {
            throw SwiftHttpExceptionHelpers.rewrite(context, ex);
        }
    }
}
