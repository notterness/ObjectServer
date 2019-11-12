package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.bmc.objectstorage.model.UsageSummary;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.GetUsageBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler.MaximumContentLength;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;
import java.util.List;
import java.util.Optional;

public class GetUsageHandler extends SyncBodyHandler {
    private final Authenticator authenticator;
    private final GetUsageBackend getUsageBackend;
    private final ObjectMapper mapper;

    public GetUsageHandler(
        Authenticator authenticator,
        GetUsageBackend getUsageBackend,
        MaximumContentLength maximumContentLength,
        ObjectMapper mapper) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.getUsageBackend = getUsageBackend;
        this.mapper = mapper;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_GET_USAGE_BUNDLE);
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.GET_USAGE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        // Authenticate an HTTP request with a body.
        final AuthenticationInfo authInfo = authenticator.authenticate(context,
            Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));

        // This tenantId should be tenant ocid (e.g. root compartment id)
        Optional<String> tenantId = HttpPathQueryHelpers.getTenantId(request);
        Optional<String> limitName = HttpPathQueryHelpers.getLimitParam(request);
        Optional<String> ad = HttpPathQueryHelpers.getAd(request);
        if (!tenantId.isPresent()) {
            throw new HttpException(V2ErrorCode.BAD_REQUEST, "Missing tenant id", "");
        }

        if (ad.isPresent()) {
            throw new HttpException(V2ErrorCode.BAD_REQUEST,
                "Ad must NOT be provided since object storage is regional service",
                "");
        }

        if (!limitName.isPresent()) {
            throw new HttpException(V2ErrorCode.BAD_REQUEST, "Missing limit name", "");
        }

        try {
            if (limitName.get().equals("storage-limit-bytes")) {
                List<UsageSummary> list = getUsageBackend.getStorageUsage(context, authInfo, tenantId.get());
                HttpContentHelpers.writeJsonResponse(request, response, list, mapper);
            } else {
                // limit name is some other names other than "storage-limit-bytes", return 400 invalid parameter
                throw new HttpException(V2ErrorCode.BAD_REQUEST, "Unknown limit name", "");
            }
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
