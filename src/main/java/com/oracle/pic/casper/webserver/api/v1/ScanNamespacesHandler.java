package com.oracle.pic.casper.webserver.api.v1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.rest.CommonQueryParams;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.NamespaceCaseWhiteList;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * responds to the client with a page of namespaces. If a cursor is sent in,
 * then the page corresponding to that cursor value will be returned otherwise
 * the first page will be returned.
 */
public class ScanNamespacesHandler extends AbstractScanHandler {

    private final BucketBackend bucketBackend;
    private final EmbargoV3 embargoV3;

    public ScanNamespacesHandler(BucketBackend bucketBackend,
                                 ObjectMapper objectMapper,
                                 EmbargoV3 embargoV3) {
        super(objectMapper);
        this.bucketBackend = bucketBackend;
        this.embargoV3 = embargoV3;
    }

    @Override
    protected CompletableFuture<Void> businessLogicCF(RoutingContext routingContext) {
        final HttpServerRequest request = routingContext.request();
        final WSRequestContext wsRequestContext = routingContext.get(WSRequestContext.REQUEST_CONTEXT_KEY);
        wsRequestContext.setNamespaceName(RequestHelpers.computeNamespaceKey(request).getName());
        MetricsHandler.addMetrics(routingContext, WebServerMetrics.V1_SCAN_NS_BUNDLE);
        final MetricScope parentScope = wsRequestContext.getCommonRequestContext().getMetricScope();
        final int pageSize = RequestHelpers.getPageSize(request);

        // Confusingly, in the V1 API namespaces are called "scopes", while
        // buckets are actually called "namespaces". This means that this
        // handler is actually listing buckets.
        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V1)
            .setNamespace(request.getParam(CasperApiV1.SCOPE_PARAM))
            .setOperation(CasperOperation.LIST_BUCKETS)
            .build();
        embargoV3.enter(embargoV3Operation);

        return VertxUtil.runAsync(parentScope.child("namespaceDb:scanBuckets"),
                () -> this.mdsScanNamespaces(routingContext, pageSize)
        )
                .thenAccept(namespaces -> this.writeResponse(routingContext.response(), namespaces, pageSize))
                .exceptionally(throwable -> fail(routingContext, throwable));
    }

    private PaginatedList<String> mdsScanNamespaces(RoutingContext context, int pageSize) {
        final HttpServerRequest request = context.request();
        final String cursor = request.getParam(CommonQueryParams.PAGE_PARAM);
        final String scope =
                NamespaceCaseWhiteList.lowercaseNamespace(Api.V1, request.getParam(CasperApiV1.SCOPE_PARAM));
        return bucketBackend.listBucketsInV1(context, scope, pageSize, cursor);
    }

    private void writeResponse(
            HttpServerResponse response,
            PaginatedList<String> namespacePage,
            int requestPageSize) {
        final List<String> namespaceList = new ArrayList<>(namespacePage.getItems());
        final String cursor;
        if (namespacePage.isTruncated()) {
            cursor = namespacePage.getItems().get(namespacePage.getItems().size() - 1);
        } else {
            cursor = null;
        }
        writePageResponse(response, namespaceList, cursor, requestPageSize);
    }
}
