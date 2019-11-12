package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.PostObjectMetadataResponse;
import com.oracle.pic.casper.webserver.api.model.UpdateObjectMetadataDetails;
import com.oracle.pic.casper.webserver.api.model.exceptions.UnauthorizedHeaderException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;
import java.util.List;

/**
 * Vert.x HTTP handler for overwriting object's metadata
 */
public class PostObjectMetadataReplaceHandler extends SyncBodyHandler {
    private final Authenticator authenticator;
    private final Backend backend;
    private final ObjectMapper mapper;
    private final List<String> servicePrincipals;
    private final EmbargoV3 embargoV3;

    public PostObjectMetadataReplaceHandler(Authenticator authenticator,
                                            Backend backend,
                                            ObjectMapper mapper,
                                            CountingHandler.MaximumContentLength maximumContentLength,
                                            List<String> servicePrincipals, EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.servicePrincipals = servicePrincipals;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {
        HttpMatchHelpers.validateConditionalHeaders(
                context.request(),
                HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.NO);
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_POST_OBJECT_UPDATE_BUNDLE);
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.REPLACE_OBJECT_METADATA);


        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.POSTOBJECT_REQUEST_COUNT);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.REPLACE_OBJECT_METADATA)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        final String ifMatch = HttpMatchHelpers.getIfMatchHeader(request);
        final String etagOverride = request.getHeader(CasperApiV2.ETAG_OVERRIDE_HEADER);
        // annotate secret header values for logging
        final MetricScope metricScope = WSRequestContext.getMetricScope(context);
        metricScope.annotate(CasperApiV2.ETAG_OVERRIDE_HEADER, etagOverride);

        final UpdateObjectMetadataDetails details = HttpContentHelpers
                .getUpdateObjectMetadataDetails(request, mapper, bytes, false);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(
                    context, Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));
            if (etagOverride != null &&
                    !(authInfo.getServicePrincipal().isPresent() &&
                            servicePrincipals.contains(authInfo.getServicePrincipal().get().getTenantId()))) {
                throw new UnauthorizedHeaderException();
            }
            PostObjectMetadataResponse result = backend.replaceObjectMetadata(context, authInfo, namespace, bucketName,
                    objectName, details.getMetadata(), ifMatch, etagOverride);
            HttpContentHelpers.writeJsonResponse(request, response, result, mapper);
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
