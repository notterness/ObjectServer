
package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Optional;

public class GetNamespaceCollectionHandler extends SyncHandler {
    private final ObjectMapper mapper;
    private final Backend backend;
    private final Authenticator authenticator;
    private final EmbargoV3 embargoV3;

    public GetNamespaceCollectionHandler(
            Authenticator authenticator, Backend backend,  ObjectMapper mapper, EmbargoV3 embargoV3) {
        this.mapper = mapper;
        this.backend = backend;
        this.authenticator = authenticator;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.GET_NAMESPACE);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_GET_NAMESPACE_COLL_BUNDLE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        final MetricScope scope = WSRequestContext.getMetricScope(context);
        scope.annotate("operation", CasperOperation.GET_NAMESPACE.getSummary());

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.GET_NAMESPACE)
            .build();
        embargoV3.enter(embargoV3Operation);

        Optional<String> tenantId = HttpPathQueryHelpers.getCompartmentId(request);
        final AuthenticationInfo authInfo = authenticator.authenticate(context);
        try {
            String tenantName = backend.getTenantName(context, authInfo, tenantId);
            HttpContentHelpers.writeJsonResponse(request, response, tenantName, mapper);
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}

