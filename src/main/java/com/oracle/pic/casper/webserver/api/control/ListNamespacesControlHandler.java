package com.oracle.pic.casper.webserver.api.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.model.NamespaceSummary;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * This is the <b>CONTROL</b> handler to list all namespaces in Casper.
 * This is useful if you ever want to go through the steps of walking through every object.
 * <br/>
 * <i>List namespace -> List buckets -> List objects</i>
 */
public class ListNamespacesControlHandler extends SyncHandler {

    static final int DEFAULT_LIMIT = 100;

    private final BucketBackend bucketBackend;
    private final ObjectMapper mapper;

    public ListNamespacesControlHandler(BucketBackend bucketBackend, ObjectMapper mapper) {
        this.bucketBackend = bucketBackend;
        this.mapper = mapper;
    }

    @Override
    public void handleSync(RoutingContext context) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        final int limit = HttpPathQueryHelpers.getLimit((request)).orElse(DEFAULT_LIMIT);
        final String startWith = HttpPathQueryHelpers.getStartWith(request).orElse(null);

        try {
            PaginatedList<NamespaceSummary> summary =
                    bucketBackend.listAllNamespacesUnauthenticated(context, Api.V2, limit,
                            startWith != null ? new NamespaceKey(Api.V2, startWith) : null);
            HttpContentHelpers.writeJsonResponse(
                    request,
                    response,
                    summary.getItems(),
                    mapper);
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
