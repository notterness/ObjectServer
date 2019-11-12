package com.oracle.pic.casper.webserver.api.swift;

import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.BucketUpdate;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;

/**
 * Updates custom user metadata on a container.
 *
 * http://developer.openstack.org/api-ref-objectstorage-v1.html#updateContainerMeta
 */
public class PostContainerHandler extends SyncBodyHandler {

    private final Authenticator authenticator;
    private final BucketBackend backend;
    private final TenantBackend tenantBackend;
    private final SwiftResponseWriter responseWriter;
    private final EmbargoV3 embargoV3;

    public PostContainerHandler(Authenticator authenticator,
                                BucketBackend backend,
                                TenantBackend tenantBackend,
                                SwiftResponseWriter responseWriter,
                                CountingHandler.MaximumContentLength maximumContentLength,
                                EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.backend = backend;
        this.tenantBackend = tenantBackend;
        this.responseWriter = responseWriter;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.SWIFT_POST_CONTAINER_BUNDLE);
        WSRequestContext.setOperationName("swift", getClass(), context, CasperOperation.UPDATE_BUCKET);

        final HttpServerRequest request = context.request();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = SwiftHttpPathHelpers.getNamespace(request, wsRequestContext);
        final String containerName = SwiftHttpPathHelpers.getContainerName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.Swift)
            .setOperation(CasperOperation.UPDATE_BUCKET)
            .setNamespace(namespace)
            .setBucket(containerName)
            .build();
        embargoV3.enter(embargoOperation);

        // POST responses should always contain "text/html; charset=UTF-8" as CONTENT_TYPE to conform to reference impl
        final String formatStr = ContentType.TEXT_HTML_UTF8;

        final BucketUpdate bucketUpdate = BucketUpdate.builder()
                .namespace(namespace)
                .bucketName(containerName)
                .metadata(SwiftHttpHeaderHelpers.getUserContainerMetadataHeaders(request))
                .build();

        try {
            final String tenantOcid = tenantBackend.getNamespace(context, scope, new NamespaceKey(Api.V2, namespace))
                    .map(MdsNamespace::getTenantOcid)
                    .orElse(null);

            final String bodySha256 = Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes());
            final AuthenticationInfo authInfo =
                    authenticator.authenticateSwiftOrCavage(context, bodySha256, namespace, tenantOcid);
            backend.updateBucketPartially(context, authInfo, bucketUpdate, null);
            responseWriter.writeEmptyResponse(context, formatStr);
        } catch (Exception ex) {
            throw SwiftHttpExceptionHelpers.rewrite(context, ex);
        }
    }
}
