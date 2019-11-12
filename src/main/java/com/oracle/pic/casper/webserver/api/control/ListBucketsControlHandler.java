package com.oracle.pic.casper.webserver.api.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.model.BucketProperties;
import com.oracle.pic.casper.webserver.api.model.BucketSummary;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Set;

/**
 * This is the <b>CONTROL</b> handler to list all buckets in the Casper namespace.
 * This is useful if you ever want to go through the steps of walking through every object.
 * This will list all buckets in a namespace regardless of compartment mapping.
 * <br/>
 * <i>List namespace -> List buckets -> List objects</i>
 */
public class ListBucketsControlHandler extends SyncHandler {

    static final int DEFAULT_LIMIT = 100;
    static final String NS_PARAM = "param0";


    private final BucketBackend bucketBackend;
    private final ObjectMapper mapper;

    public ListBucketsControlHandler(BucketBackend bucketBackend, ObjectMapper mapper) {
        this.bucketBackend = bucketBackend;
        this.mapper = mapper;
    }

    @Override
    public void handleSync(RoutingContext context) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        final int limit = HttpPathQueryHelpers.getLimit((request)).orElse(DEFAULT_LIMIT);
        final String startWith = HttpPathQueryHelpers.getStartWith(request).orElse(null);
        final String namespace = request.getParam(NS_PARAM);
        final Set<BucketProperties> bucketProperties =
                HttpPathQueryHelpers.getRequestedBucketParametersForList(request, BucketProperties.TAGS,
                        BucketProperties.OBJECT_LIFECYCLE_POLICY_ETAG);

        try {
            PaginatedList<BucketSummary> summary =
                    bucketBackend.listAllBucketsInNamespaceUnauthenticated(context, namespace, limit, startWith,
                            bucketProperties);
            // filter out the PAR buckets
            final PaginatedList<BucketSummary> filteredSummary = BucketBackend.filterInternalBucket(summary);

            HttpContentHelpers.writeJsonResponse(
                    request,
                    response,
                    filteredSummary.getItems(),
                    mapper);
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }

}
