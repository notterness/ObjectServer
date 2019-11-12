package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.config.v2.AuditConfiguration;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.common.V2BucketCreationChecks;
import com.oracle.pic.casper.webserver.api.model.Bucket;
import com.oracle.pic.casper.webserver.api.model.BucketUpdate;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.NotImplementedException;

import java.util.Base64;

/**
 * Vert.x HTTP handler for partial updates to existing buckets.
 */
public class PostBucketHandler extends SyncBodyHandler {
    private final Authenticator authenticator;
    private final BucketBackend backend;
    private final ObjectMapper mapper;
    private final ResourceControlledMetadataClient metadataClient;
    private final AuditConfiguration auditConfiguration;
    private final EmbargoV3 embargoV3;

    public PostBucketHandler(Authenticator authenticator,
                             BucketBackend backend,
                             ObjectMapper mapper,
                             ResourceControlledMetadataClient metadataClient,
                             AuditConfiguration auditConfiguration,
                             CountingHandler.MaximumContentLength maximumContentLength,
                             EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.metadataClient = metadataClient;
        this.auditConfiguration = auditConfiguration;
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
                context.request(), PostBucketCollectionHandler.MAX_BUCKET_CONTENT_BYTES);
        HttpMatchHelpers.validateConditionalHeaders(
                context.request(),
                HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.NO);
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_POST_BUCKET_BUNDLE);
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.UPDATE_BUCKET);

        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        Validator.validateV2Namespace(namespace);

        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.UPDATE_BUCKET)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        final String ifMatch = HttpMatchHelpers.getIfMatchHeader(request);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(
                    context, Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));

            final BucketUpdate bucketUpdate = HttpContentHelpers.readBucketUpdateContent(
                    request, mapper, namespace, bucketName, bytes);
            final String tenantId = authInfo.getMainPrincipal().getTenantId();

            // usage of the ObjectLevelAuditMode is only allowed if the tenant is in the whitelist
            // OR the whitelist is to be overridden (ie anyone is allowed to use this feature)
            // The whitelist override is a per-region flag, specified via CasperConfig
            final boolean objectLevelAccessModeSettingAllowed = auditConfiguration
                    .getObjectLevelTenantWhitelist().contains(tenantId) ||
                    auditConfiguration.getObjectLevelTenantWhitelistOverride();

            // if objectLevelAuditMode set and setting it is not allowed, reject the request
            if (bucketUpdate.getObjectLevelAuditMode().isPresent() && !objectLevelAccessModeSettingAllowed) {
                throw new NotImplementedException("objectLevelAuditMode is not implemented");
            }

            // CASPER-5579: For a bucket move operation, we need to check whether the target compartment belongs to the
            // bucket's current namespace. Note that we should not check against the caller's compartment as it will
            // disallow the cross tenant call to move the bucket.
            if (bucketUpdate.getCompartmentId().isPresent()) {
                final String tenantOcidOwnsCompartment = V2BucketCreationChecks.checkNamespace(metadataClient, authInfo,
                                bucketUpdate.getCompartmentId().get(), namespace).getId();
                WSRequestContext.get(context).setResourceTenantOcid(tenantOcidOwnsCompartment);
            }

            final Bucket bucket = backend.updateBucketPartially(context, authInfo, bucketUpdate, ifMatch);
            HttpContentHelpers.writeJsonResponse(
                    request, context.response(), bucket, mapper, HttpHeaderHelpers.etagHeader(bucket.getETag()));
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
