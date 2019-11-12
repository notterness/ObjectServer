package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.NotAuthenticatedException;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.NamespaceMetadata;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchCompartmentIdException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchNamespaceException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Optional;


public class PutNamespaceMetadataHandler extends SyncBodyHandler {
    private static final long MAX_NAMESPACE_METADATA_BYTES = 10 * 1024;
    private final Authenticator authenticator;
    private final BucketBackend backend;
    private final ObjectMapper mapper;
    private final ResourceControlledMetadataClient metadataClient;
    private final EmbargoV3 embargoV3;

    public PutNamespaceMetadataHandler(Authenticator authenticator,
                                       BucketBackend backend,
                                       ObjectMapper mapper,
                                       ResourceControlledMetadataClient metadataClient,
                                       CountingHandler.MaximumContentLength maximumContentLength,
                                       EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.metadataClient = metadataClient;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {
        HttpContentHelpers.negotiateApplicationJsonContent(context.request(), MAX_NAMESPACE_METADATA_BYTES);
    }

    public void validateCompartmentID(String compartmentId, String tenantId, String namespace) {
        if (compartmentId == null) {
            return;
        }
        Optional<Tenant> tenantFromCompartmentId = metadataClient.getTenantByCompartmentId(compartmentId, namespace);
        if (!tenantFromCompartmentId.isPresent()) {
            throw new NoSuchCompartmentIdException("Either the compartment with id '" + compartmentId +
                    "' does not exist  or you are not authorized to access it");
        }
        if (!tenantFromCompartmentId.get().getId().equals(tenantId)) {
            throw new NoSuchBucketException(
                    "Either the bucket not exist in namespace " + tenantFromCompartmentId.get().getNamespace() +
                    " or you are not authorized to update it.");
        }
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.UPDATE_NAMESPACE_METADATA);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_POST_NAMESPACE_METADATA_BUNDLE);
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final MetricScope scope = commonContext.getMetricScope();
        final HttpServerRequest request = context.request();
                final HttpServerResponse response = context.response();
        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.UPDATE_NAMESPACE_METADATA)
            .setNamespace(namespace)
            .build();
        embargoV3.enter(embargoV3Operation);

        Validator.validateV2Namespace(namespace);

        try {
            // For Casper PUT operation, avoid content_sha check is allowed. Thus use authenticatePutObject() instead
            // of authenticate() here.
            final AuthenticationInfo authInfo = authenticator.authenticatePutObject(context);
            if (authInfo == AuthenticationInfo.ANONYMOUS_USER) {
                throw new NotAuthenticatedException("Update namespace metadata is limited to authenticated users");
            }
            final String tenantId = authInfo.getMainPrincipal().getTenantId();
            Optional<Tenant> tenantFromCompartmentId = metadataClient.getTenantByCompartmentId(tenantId, namespace);
            if (!tenantFromCompartmentId.isPresent() ||
                    !tenantFromCompartmentId.get().getNamespace().equals(namespace)) {
                throw new NoSuchNamespaceException("Either the namespace " + namespace +
                        " does not exist or you are not authorized to view it");
            }

            WSRequestContext.get(context).setCompartmentID(tenantId);

            final NamespaceMetadata updateMetadata = HttpContentHelpers.getNamespaceMetadata(request, bytes, mapper);
            validateCompartmentID(updateMetadata.getDefaultS3CompartmentId(), tenantId, namespace);
            validateCompartmentID(updateMetadata.getDefaultSwiftCompartmentId(), tenantId, namespace);
            backend.updateNamespaceMetadata(scope, context, authInfo, namespace, tenantId, updateMetadata);
            HttpContentHelpers.writeJsonResponse(request, response, updateMetadata, mapper);
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
