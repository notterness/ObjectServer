package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.config.v2.AuditConfiguration;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.common.V2BucketCreationChecks;
import com.oracle.pic.casper.webserver.api.model.Bucket;
import com.oracle.pic.casper.webserver.api.model.BucketCreate;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.identity.authentication.Principal;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

/**
 * Vert.x HTTP handler for creating new buckets.
 */
public class PostBucketCollectionHandler extends SyncBodyHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PostBucketCollectionHandler.class);
    static final long MAX_BUCKET_CONTENT_BYTES = 10 * 1024;

    private final Authenticator authenticator;
    private final ObjectMapper mapper;
    private final BucketBackend backend;
    private final TenantBackend tenantBackend;
    private final ResourceControlledMetadataClient metadataClient;
    private final AuditConfiguration auditConfiguration;
    private final EmbargoV3 embargoV3;

    public PostBucketCollectionHandler(
        Authenticator authenticator,
        ObjectMapper mapper,
        BucketBackend backend,
        TenantBackend tenantBackend,
        ResourceControlledMetadataClient metadataClient,
        AuditConfiguration auditConfiguration,
        CountingHandler.MaximumContentLength maximumContentLength,
        EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.mapper = mapper;
        this.backend = backend;
        this.tenantBackend = tenantBackend;
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
        HttpContentHelpers.negotiateApplicationJsonContent(context.request(), MAX_BUCKET_CONTENT_BYTES);
        HttpMatchHelpers.validateConditionalHeaders(
                context.request(),
                HttpMatchHelpers.IfMatchAllowed.NO,
                HttpMatchHelpers.IfNoneMatchAllowed.NO);
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.CREATE_BUCKET);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_POST_BUCKET_COLL_BUNDLE);
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final HttpServerRequest request = context.request();
        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);

        Validator.validateV2Namespace(namespace);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(
                    context, Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));

            final Principal principal = authInfo.getMainPrincipal();
            final BucketCreate bucketCreate = HttpContentHelpers.readBucketCreateContent(
                    request, mapper, namespace, principal.getSubjectId(), bytes);

            final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
                    .setApi(EmbargoV3Operation.Api.V2)
                    .setOperation(CasperOperation.CREATE_BUCKET)
                    .setNamespace(namespace)
                    .setBucket(bucketCreate.getBucketName())
                    .build();
            embargoV3.enter(embargoV3Operation);

            // usage of the ObjectLevelAuditMode is only allowed if the tenant is in the whitelist
            // OR the whitelist is to be overridden (ie anyone is allowed to use this feature)
            // The whitelist override is a per-region flag, specified via CasperConfig
            final boolean objectLevelAccessModeSettingAllowed = auditConfiguration
                    .getObjectLevelTenantWhitelist().contains(principal.getTenantId()) ||
                    auditConfiguration.getObjectLevelTenantWhitelistOverride();

            // if objectLevelAuditMode set and setting it is not allowed, reject the request
            if (bucketCreate.getObjectLevelAuditMode().isPresent() && !objectLevelAccessModeSettingAllowed) {
                throw new NotImplementedException("objectLevelAuditMode is not implemented");
            }

            final String compartmentId = bucketCreate.getCompartmentId();
            Validator.validateCompartmentId(compartmentId);

            Tenant tenant = V2BucketCreationChecks.checkNamespace(metadataClient,
                                                                  authInfo,
                                                                  compartmentId,
                                                                  namespace);
            WSRequestContext.get(context).setResourceTenantOcid(tenant.getId());
            // Create the namespace with the tenant ocid associated with the namespace.
            tenantBackend.getOrCreateNamespace(context, scope, new NamespaceKey(Api.V2, namespace), tenant.getId());

            wsRequestContext.setBucketName(bucketCreate.getBucketName());

            LOG.debug("\n\nPOST creating bucket: {} with authInfo {}", bucketCreate, authInfo);
            final Bucket bucket = backend.createV2Bucket(context, authInfo, bucketCreate);
            wsRequestContext.setNewkmsKeyId(bucketCreate.getKmsKeyId());
            FilterProvider filter = null;
            HttpContentHelpers.writeJsonResponse(
                    request,
                    context.response(),
                    bucket,
                    mapper,
                    filter,
                    HttpHeaderHelpers.locationHeader(request, bucket.getBucketName()),
                    HttpHeaderHelpers.etagHeader(bucket.getETag()));
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
