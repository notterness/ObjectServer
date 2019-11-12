package com.oracle.pic.casper.webserver.api.v2;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.NotAuthenticatedException;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.NamespaceMetadata;
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

public class GetNamespaceMetadataHandler extends SyncHandler {
    private final Authenticator authenticator;
    private final BucketBackend backend;
    private final ObjectMapper mapper;
    private final ResourceControlledMetadataClient metadataClient;
    private final EmbargoV3 embargoV3;

    public GetNamespaceMetadataHandler(Authenticator authenticator,
                                       BucketBackend backend,
                                       ObjectMapper mapper,
                                       ResourceControlledMetadataClient metadataClient,
                                       EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.metadataClient = metadataClient;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.GET_NAMESPACE_METADATA);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_GET_NAMESPACE_METADATA_BUNDLE);
        final HttpServerResponse response = context.response();
        final MetricScope scope = WSRequestContext.getMetricScope(context);
        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.GET_NAMESPACE_METADATA)
            .setNamespace(namespace)
            .build();
        embargoV3.enter(embargoV3Operation);

        Validator.validateV2Namespace(namespace);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(context);
            if (authInfo == AuthenticationInfo.ANONYMOUS_USER) {
                throw new NotAuthenticatedException("Get compartmentId is limited to authenticated users");
            }
            final String tenantId = authInfo.getMainPrincipal().getTenantId();
            WSRequestContext.get(context).setCompartmentID(tenantId);
            Optional<Tenant> tenantFromCompartmentId = metadataClient.getTenantByCompartmentId(tenantId, namespace);
            if (!tenantFromCompartmentId.isPresent() ||
                    !tenantFromCompartmentId.get().getNamespace().equals(namespace)) {
                throw new NoSuchNamespaceException("Either the namespace " + namespace +
                        " does not exist or you are not authorized to view it");
            }
            NamespaceMetadata namespaceMetadata = backend.
                    getNamespaceMetadata(scope, context, authInfo, namespace, tenantId, Api.V2);

            // return both s3 & swift default compartment id
            HttpContentHelpers.writeJsonResponse(request, response, namespaceMetadata, mapper);
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}



