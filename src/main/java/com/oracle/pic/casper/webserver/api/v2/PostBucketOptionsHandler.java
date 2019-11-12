package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.mds.common.validator.InputValidator;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.UpdateBucketOptionsRequestJson;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;
import java.util.Map;

public class PostBucketOptionsHandler extends SyncBodyHandler {

    private final Authenticator authenticator;
    private final BucketBackend bucketBackend;
    private final ObjectMapper objectMapper;
    private final EmbargoV3 embargoV3;

    public PostBucketOptionsHandler(Authenticator authenticator,
                                    BucketBackend bucketBackend, ObjectMapper objectMapper,
                                    CountingHandler.MaximumContentLength maximumContentLength,
                                    EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.bucketBackend = bucketBackend;
        this.objectMapper = objectMapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {
        HttpContentHelpers.negotiateApplicationJsonContent(
                context.request(), InputValidator.BUCKET_OPTIONS_SIZE_LIMIT);
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
                CasperOperation.UPDATE_BUCKET_OPTIONS);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_POST_BUCKET_OPTIONS_BUNDLE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.UPDATE_BUCKET_OPTIONS)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.NO);
        final UpdateBucketOptionsRequestJson updateBucketOptionsRequestJson =
                HttpContentHelpers.readUpdateBucketOptionsRequestJson(request, objectMapper, bytes);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(
                    context, Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));
            final Map<String, Object> newOptions = updateBucketOptionsRequestJson.getOptions().orElse(null);
            bucketBackend.updateBucketOptions(context, authInfo, namespace, bucketName, newOptions);
            response.setStatusCode(HttpResponseStatus.NO_CONTENT).end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }

}
