package com.oracle.pic.casper.webserver.api.v1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.exceptions.NotFoundException;
import com.oracle.pic.casper.common.rest.CommonQueryParams;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.objectmeta.BucketKey;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

/**
 * Returns an unordered list of objects (keys only) for a specific namespace. If a cursor is passed in, the page
 * corresponding to that cursor is returned otherwise the first page is returned to the client.
 */
public class ScanObjectsHandler extends AbstractScanHandler {

    private final Backend backend;
    private final EmbargoV3 embargoV3;

    public ScanObjectsHandler(Backend backend, ObjectMapper objectMapper, EmbargoV3 embargoV3) {
        super(objectMapper);
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    protected CompletableFuture<Void> businessLogicCF(RoutingContext routingContext) {
        final WSRequestContext wsRequestContext = WSRequestContext.get(routingContext);
        final HttpServerRequest request = routingContext.request();
        wsRequestContext.setNamespaceName(RequestHelpers.computeNamespaceKey(request).getName());
        return this.mdsScanObjects(routingContext)
                .thenAcceptAsync(objectKeyPage -> this.writeResponse(routingContext, objectKeyPage),
                        VertxExecutor.eventLoop())
                .exceptionally(throwable -> fail(routingContext, throwable));
    }

    private CompletableFuture<PaginatedList<String>> mdsScanObjects(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.V1_SCAN_OBJECTS_BUNDLE);
        final HttpServerRequest request = context.request();
        final BucketKey bucketKey = RequestHelpers.computeBucketKey(request);
        final int pageSize = RequestHelpers.getPageSize(request);
        final String cursor = request.getParam(CommonQueryParams.PAGE_PARAM);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V1)
            .setNamespace(bucketKey.getNamespace())
            .setBucket(bucketKey.getName())
            .setOperation(CasperOperation.LIST_OBJECTS)
            .build();
        embargoV3.enter(embargoV3Operation);

        return VertxUtil.runAsync(() -> {
            try {
                return backend.listObjectsV1(context, bucketKey.getNamespace(), bucketKey.getName(), pageSize, cursor);
            } catch (NoSuchBucketException e) {
                throw new NotFoundException("Bucket " + bucketKey.getName() +
                        " does not exist within scope " + bucketKey.getNamespace());
            }
        });
    }

    private void writeResponse(
            RoutingContext context,
            PaginatedList<String> objectPage) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final int pageSize = RequestHelpers.getPageSize(request);

        final String cursor;
        if (objectPage.isTruncated()) {
            cursor = objectPage.getItems().get(objectPage.getItems().size() - 1);
        } else {
            cursor = null;
        }
        writePageResponse(response, objectPage.getItems(), cursor, pageSize);
    }

}
