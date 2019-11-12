package com.oracle.pic.casper.webserver.api.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.UpdateBucketOptionsRequestJson;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Map;
import java.util.function.Supplier;

public class PostReplicationOptionsHandler extends SyncBodyHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PostReplicationOptionsHandler.class);

    private final Authenticator authenticator;
    private final BucketBackend bucketBackend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    public PostReplicationOptionsHandler(Authenticator authenticator,
                                         BucketBackend bucketBackend,
                                         ObjectMapper mapper,
                                         CountingHandler.MaximumContentLength maximumContentLength,
                                         EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.bucketBackend = bucketBackend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    protected void validateHeaders(RoutingContext context) {
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

        final Supplier<Map<String, Object>> optionsSupplier = () -> {
            final UpdateBucketOptionsRequestJson updateBucketOptionsRequestJson =
                    HttpContentHelpers.readUpdateBucketOptionsRequestJson(request, mapper, bytes);
            return updateBucketOptionsRequestJson.getOptions().orElse(null);
        };

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(
                    context, Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));
            bucketBackend.updateBucketReplicationOptions(context, authInfo, namespace, bucketName, optionsSupplier);
            response.setStatusCode(HttpResponseStatus.NO_CONTENT).end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return null;
    }
}
