package com.oracle.pic.casper.webserver.api.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.UpdateLoggingRequestJson;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;
import java.util.Map;

/**
 * This handler is responsible for enabling logging for a bucket for a particular category
 *
 * POST /logging
 * request body : {
 *     logId:
 *     category:
 *     resource:
 *     tenancyId:
 * }
 */
public class PostLoggingHandler extends SyncBodyHandler {

    private final BucketBackend bucketBackend;
    private final ObjectMapper objectMapper;
    private final ResourceControlledMetadataClient metadataClient;
    private final Authenticator authenticator;

    public PostLoggingHandler(Authenticator authenticator,
                              CountingHandler.MaximumContentLength contentLengthLimiter,
                              ObjectMapper objectMapper,
                              BucketBackend bucketBackend,
                              ResourceControlledMetadataClient metadataClient) {
        super(contentLengthLimiter);
        this.objectMapper = objectMapper;
        this.bucketBackend = bucketBackend;
        this.metadataClient = metadataClient;
        this.authenticator = authenticator;
    }


    @Override
    protected void validateHeaders(RoutingContext context) {
        //No validation added for headers
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.POST_LOGGING);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        final UpdateLoggingRequestJson updateLoggingRequestJson =
                readUpdateLoggingRequestJson(request, objectMapper, bytes);

        String bucketName = updateLoggingRequestJson.getResource();
        String tenantId = updateLoggingRequestJson.getTenantId();

        final Tenant tenant = metadataClient.getTenantByCompartmentId(tenantId, ResourceLimiter.UNKNOWN_NAMESPACE)
                .orElseThrow(() -> new HttpException(V2ErrorCode.INVALID_TENANT_ID,
                "Could not find a valid namespace for the tenant", request.path()));
        try {
            String category = PublicLoggingHelper.getBucketOptionFromCategory(updateLoggingRequestJson.getCategory());
            Map<String, Object> bucketOption = ImmutableMap.of(category, updateLoggingRequestJson.getLogId());

            final AuthenticationInfo authInfo = authenticator.authenticate(
                    context, Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));
            bucketBackend.updateBucketOptions(context, authInfo, tenant.getNamespace(), bucketName, bucketOption);
            response.setStatusCode(HttpResponseStatus.OK).end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }

    }


    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    public static UpdateLoggingRequestJson readUpdateLoggingRequestJson(HttpServerRequest request,
                                                                        ObjectMapper mapper,
                                                                        byte[] bytes) {
        ChecksumHelper.checkContentMD5(request, bytes);
        final UpdateLoggingRequestJson updateLoggingRequestJson;
        try {
            updateLoggingRequestJson = mapper.readValue(bytes, UpdateLoggingRequestJson.class);
        } catch (Exception e) {
            throw new HttpException(V2ErrorCode.INVALID_JSON,
                    "Could not parse body as a valid update bucket options request.",
                    request.path(), e);
        }
        return updateLoggingRequestJson;
    }
}
