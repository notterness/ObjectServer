package com.oracle.pic.casper.webserver.api.pars;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.GetPreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Vert.x HTTP handler for retrieving a PAR.
 */
public class GetParHandler extends CompletableHandler {

    private final AsyncAuthenticator authenticator;
    private final PreAuthenticatedRequestBackend parStore;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    private static final Logger LOG = LoggerFactory.getLogger(GetParHandler.class);

    public GetParHandler(AsyncAuthenticator authenticator,
                         PreAuthenticatedRequestBackend parStore,
                         ObjectMapper mapper, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.parStore = parStore;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.GET_PAR);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_GET_PAR_BUNDLE);

        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String customerParId = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);
        final BackendParId parId = new CustomerParId(customerParId).toBackendParId();

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.GET_PAR)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        return authenticator.authenticate(context)
                .thenCompose(authInfo -> {
                    GetPreAuthenticatedRequestRequest getRequest =
                            new GetPreAuthenticatedRequestRequest(context, authInfo, parId);
                    LOG.debug("Sending get PAR request to PAR backend: " + getRequest);
                    return parStore.getPreAuthenticatedRequest(getRequest);
                })
                .handle((val, ex) -> HttpException.handle(request, val, ex))
                .thenAccept(parMd -> {
                    ServiceLogsHelper.logServiceEntry(context);
                    HttpContentHelpers.writeJsonResponse(
                            request, response, ParConversionUtils.backendToCustomerParMd(parMd), mapper);
                });
    }
}
