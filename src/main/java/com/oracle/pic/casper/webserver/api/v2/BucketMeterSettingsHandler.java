package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.model.BucketMeterFlag;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;
import java.util.Optional;

public class BucketMeterSettingsHandler extends SyncBodyHandler {

    private final Authenticator authenticator;
    private final BucketBackend bucketBackend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    public BucketMeterSettingsHandler(Authenticator authenticator,
                                      BucketBackend bucketBackend, ObjectMapper mapper,
                                      CountingHandler.MaximumContentLength maximumContentLength,
                                      EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.bucketBackend = bucketBackend;
        this.mapper = mapper;
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
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_BUCKET_METER_SETTINGS_BUNDLE);
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.ADD_BUCKET_METER_SETTING);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.ADD_BUCKET_METER_SETTING)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        Optional<BucketMeterFlag> meterSetting = HttpPathQueryHelpers.getMeterSetting(request);
        if (!meterSetting.isPresent()) {
            // If the setting isn't specified, default to RMAN.
            meterSetting = Optional.of(BucketMeterFlag.Rman);
        }

        HttpMatchHelpers.validateConditionalHeaders(request,
                                                    HttpMatchHelpers.IfMatchAllowed.NO,
                                                    HttpMatchHelpers.IfNoneMatchAllowed.NO);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(
                    context,
                    Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));

            bucketBackend.addBucketMeterSetting(context, authInfo, namespace, bucketName, meterSetting.get(), response);
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }

}
