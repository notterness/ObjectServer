package com.oracle.pic.casper.webserver.api.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.model.ObjectProperties;
import com.oracle.pic.casper.webserver.api.model.ObjectSummaryCollection;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * This is the <b>CONTROL</b> handler to list all buckets in the Casper namespace.
 * This is useful if you ever want to go through the steps of walking through every object.
 * This will list all buckets in a namespace regardless of compartment mapping.
 * <br/>
 * <i>List namespace -> List buckets -> List objects</i>
 */
public class ListObjectsControlHandler extends SyncHandler {

    static final int DEFAULT_LIMIT = 1000;

    private final Backend backend;
    private final ObjectMapper mapper;

    public ListObjectsControlHandler(Backend backend, ObjectMapper mapper) {
        this.backend = backend;
        this.mapper = mapper;
    }

    @Override
    public void handleSync(RoutingContext context) {

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = request.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final int limit = HttpPathQueryHelpers.getLimit((request)).orElse(DEFAULT_LIMIT);
        final Character delimiter = HttpPathQueryHelpers.getDelimiter(request);
        final ImmutableList<ObjectProperties> requestedProperties =
                HttpPathQueryHelpers.getRequestedObjectParametersForList(request);
        final String startWith = HttpPathQueryHelpers.getStartWith(request).orElse(null);
        final String endBefore = HttpPathQueryHelpers.getEndBefore(request).orElse(null);
        final String prefix = HttpPathQueryHelpers.getPrefix(request).orElse(null);

        try {
            final ObjectSummaryCollection summary = backend.listObjects(
                    context,
                    AuthenticationInfo.SUPER_USER,
                    requestedProperties,
                    limit,
                    namespace,
                    bucketName,
                    delimiter,
                    prefix,
                    startWith,
                    endBefore);
            HttpContentHelpers.writeJsonResponse(request, response, summary, mapper);
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
