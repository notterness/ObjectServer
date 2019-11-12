package com.oracle.pic.casper.webserver.api.logging;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.vertx.HttpServerUtil;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.HashMap;
import java.util.Map;

/**
 * This handler is responsible for disabling logging for a bucket.
 *
 * API spec- https://bitbucket.oci.oraclecorp.com/projects/LUM/repos/hydra-pika-controlplane/browse/hydra-pika-controlplane-api-spec/specs/s2s.cond.yaml?at=public-api#131
 *
 * DELETE /logging/{logId}?resource=<>&category=<>&tenancyId=<>
 *
 **/
public class DeleteLoggingHandler extends SyncHandler {

    private final BucketBackend bucketBackend;
    private final ResourceControlledMetadataClient metadataClient;
    private final Authenticator authenticator;

    public DeleteLoggingHandler(Authenticator authenticator,
                                BucketBackend bucketBackend,
                                ResourceControlledMetadataClient metadataClient) {
        this.bucketBackend = bucketBackend;
        this.metadataClient = metadataClient;
        this.authenticator = authenticator;
    }
    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.DELETE_LOGGING);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        Map<String, String> queryParameters = HttpServerUtil.getQueryParams(request);
        String bucketName = queryParameters.getOrDefault("resource", null);
        String category = queryParameters.getOrDefault("category", null);
        String tenancyId = queryParameters.getOrDefault("tenancyId", null);

        if (tenancyId == null || category == null || bucketName == null) {
            throw new HttpException(V2ErrorCode.BAD_REQUEST, "One or more of the required parameters not available",
                    request.absoluteURI());
        }

        final Tenant tenant = metadataClient.getTenantByCompartmentId(tenancyId, ResourceLimiter.UNKNOWN_NAMESPACE)
                .orElseThrow(() -> new HttpException(V2ErrorCode.INVALID_TENANT_ID,
                        "Could not find a valid namespace for the tenant", request.path()));

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();

        try {
            String logCategory = PublicLoggingHelper.getBucketOptionFromCategory(category);
            Map<String, Object> bucketOption = new HashMap<String, Object>() {{
                put(logCategory, null);
            }};
            final AuthenticationInfo authInfo = authenticator.authenticate(context);
            try {
                bucketBackend.getBucketMetadata(context, scope, tenant.getNamespace(), bucketName);
            } catch (NoSuchBucketException ex) {
                /*
                 * We did not find the bucket -- it has been deleted (or never existed).
                 * When this happens we still want this call to succeed because this
                 * method's contract is "Return successfully if logging is no longer
                 * enabled on the bucket." If the bucket does not exist logging is
                 * definitely NOT enabled, so we ignore this exception and return
                 * successfully.
                 *
                 * DO NOT CHANGE THIS BEHAVIOR (i.e. do not have this method start to
                 * return a 404 error if the bucket does not exist). The Public Logging
                 * log deletion workflow relies on this idempotent behavior.
                 */
                response.setStatusCode(HttpResponseStatus.NO_CONTENT).end();
                return;
            }
            bucketBackend.updateBucketOptions(context, authInfo, tenant.getNamespace(), bucketName, bucketOption);
            response.setStatusCode(HttpResponseStatus.NO_CONTENT).end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);

        }

    }
}
