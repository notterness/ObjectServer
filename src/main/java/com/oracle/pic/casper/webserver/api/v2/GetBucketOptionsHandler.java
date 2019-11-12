package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.oracle.bmc.objectstorage.model.BucketOptionsDetails;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * Vert.x HTTP handler for fetching options (internal use only) associated with a bucket.
 */
public class GetBucketOptionsHandler extends SyncHandler {
    private final Authenticator authenticator;
    private final ObjectMapper mapper;
    private final BucketBackend bucketBackend;
    private final EmbargoV3 embargoV3;

    public GetBucketOptionsHandler(Authenticator authenticator, BucketBackend bucketBackend, ObjectMapper mapper,
                                   EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.mapper = mapper;
        this.bucketBackend = bucketBackend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.GET_BUCKET_OPTIONS);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_GET_BUCKET_OPTIONS_BUNDLE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.YES);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.GET_BUCKET_OPTIONS)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        try {
            final FilterProvider filter = null;
            final AuthenticationInfo authInfo = authenticator.authenticate(context);
            BucketOptionsDetails bucketOptions =
                    bucketBackend.getBucketOptions(context, authInfo, namespace, bucketName);
            HttpContentHelpers.writeJsonResponse(request, response, bucketOptions, mapper, filter);
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
