package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
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
import com.oracle.pic.casper.webserver.api.model.ReencryptObjectDetails;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;

public class PostObjectReencryptHandler extends SyncBodyHandler {

    private final Authenticator authenticator;
    private final Backend backend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    public PostObjectReencryptHandler(Authenticator authenticator, Backend backend, ObjectMapper mapper,
                                      CountingHandler.MaximumContentLength maximumContentLength,
                                      EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handle(RoutingContext context) {
        super.handle(context);
    }

    @Override
    public void validateHeaders(RoutingContext context) {
        HttpMatchHelpers.validateConditionalHeaders(
                context.request(),
                HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.NO);
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_POST_OBJECT_UPDATE_BUNDLE);
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.REENCRYPT_OBJECT);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.POSTOBJECT_REQUEST_COUNT);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.REENCRYPT_OBJECT)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        try {
            final ReencryptObjectDetails reencryptObjectDetails =
                    HttpContentHelpers.getReencryptObjectDetails(request, bytes, mapper);
            final AuthenticationInfo authInfo = authenticator.authenticate(context,
                    Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));
            backend.reencryptObject(context, authInfo, namespace, bucketName, objectName, reencryptObjectDetails);
            HttpContentHelpers.writeJsonResponse(request, response, null, mapper);
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
