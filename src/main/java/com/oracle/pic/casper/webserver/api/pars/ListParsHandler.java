package com.oracle.pic.casper.webserver.api.pars;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.ListPreAuthenticatedRequestsRequest;
import com.oracle.pic.casper.webserver.api.model.PreAuthenticatedRequestMetadata;
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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Vert.x HTTP handler for listing PARs.
 */
public class ListParsHandler extends CompletableHandler {

    static final int DEFAULT_LIMIT = 100;

    private final AsyncAuthenticator authenticator;
    private final PreAuthenticatedRequestBackend parStore;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    private static final Logger LOG = LoggerFactory.getLogger(ListParsHandler.class);

    public ListParsHandler(AsyncAuthenticator authenticator,
                           PreAuthenticatedRequestBackend parStore,
                           ObjectMapper mapper,
                           EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.parStore = parStore;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.LIST_PARS);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_LIST_PAR_BUNDLE);

        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = request.response();

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final int limit = HttpPathQueryHelpers.getLimit(request).orElse(DEFAULT_LIMIT);
        final String prefix = HttpPathQueryHelpers.getObjectNamePrefix(request).orElse(null);
        final String startWith = HttpPathQueryHelpers.getPage(request).orElse(null);
        final BackendParId parId = HttpPathQueryHelpers.getParIdFromParUrl(request).orElse(null);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.LIST_PARS)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        return authenticator.authenticate(context)
                .thenCompose(authInfo -> {
                    ListPreAuthenticatedRequestsRequest listRequest = ListPreAuthenticatedRequestsRequest.builder()
                            .withContext(context)
                            .withAuthInfo(authInfo)
                            .withBucket(bucketName)
                            .withPageLimit(limit)
                            .withObjectNamePrefix(prefix)
                            .withNextPageToken(startWith)
                            .withParId(parId)
                            .build();
                    LOG.debug("Sending list PAR request to PAR backend: " + listRequest);
                    return parStore.listPreAuthenticatedRequests(listRequest);
                })
                .handle((val, ex) -> HttpException.handle(request, val, ex))
                .thenAccept(parPaginatedList -> {
                    List<PreAuthenticatedRequestMetadata> parMds = parPaginatedList.getItems().stream()
                            .map(ParConversionUtils::backendToCustomerParMd)
                            .collect(Collectors.toList());
                    HttpContentHelpers.writeJsonResponse(request, response, parMds, mapper,
                            HttpHeaderHelpers.nextPageHeader(parPaginatedList.getNextPageToken()));
                    ServiceLogsHelper.logServiceEntry(context);
                });
    }
}
