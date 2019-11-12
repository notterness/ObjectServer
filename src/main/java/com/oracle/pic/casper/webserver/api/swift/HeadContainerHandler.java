package com.oracle.pic.casper.webserver.api.swift;

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
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.model.Bucket;
import com.oracle.pic.casper.webserver.api.model.swift.SwiftContainer;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * Reads custom user metadata on a container.
 *
 * Real Swift supports a number of specified metadata headers, such as the number of objects in the container and the
 * total space used by all of them, but we don't have the metadata internally to support that efficiently, so we don't.
 *
 * http://developer.openstack.org/api-ref-objectstorage-v1.html#showContainerMeta
 */
public class HeadContainerHandler extends SyncHandler {

    private final Authenticator authenticator;
    private final BucketBackend backend;
    private final TenantBackend tenantBackend;
    private final SwiftResponseWriter responseWriter;
    private final EmbargoV3 embargoV3;

    public HeadContainerHandler(Authenticator authenticator,
                                BucketBackend backend,
                                TenantBackend tenantBackend,
                                SwiftResponseWriter responseWriter,
                                EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.tenantBackend = tenantBackend;
        this.responseWriter = responseWriter;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.SWIFT_HEAD_CONTAINER_BUNDLE);
        WSRequestContext.setOperationName("swift", getClass(), context, CasperOperation.HEAD_BUCKET);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = SwiftHttpPathHelpers.getNamespace(request, wsRequestContext);
        final String containerName = SwiftHttpPathHelpers.getContainerName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.Swift)
            .setOperation(CasperOperation.HEAD_BUCKET)
            .setNamespace(namespace)
            .setBucket(containerName)
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
            final AuthenticationInfo authInfo =
                    authenticator.authenticateSwiftOrCavage(context, null, namespace, tenantOcid);
            final Bucket bucket = backend.headBucket(context, authInfo, namespace, containerName)
                    .orElseThrow(() -> new HttpException(V2ErrorCode.BUCKET_NOT_FOUND, "", request.path()));

            final SwiftContainer container = SwiftContainer.fromBucket(bucket.getBucketName(), bucket.getMetadata());

            SwiftHttpHeaderHelpers.addUserContainerMetadataToHttpHeaders(response, container.getMetadata());

            responseWriter.writeEmptyResponse(context, formatStr);

        } catch (Exception ex) {
            throw SwiftHttpExceptionHelpers.rewrite(context, ex);
        }
    }
}
