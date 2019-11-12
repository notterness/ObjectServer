package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectProperties;
import com.oracle.pic.casper.webserver.api.model.ObjectSummaryCollection;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;
import java.util.List;

public class ListObjectsHandler extends SyncHandler {

    static final int DEFAULT_LIMIT = 1000;

    private final Authenticator authenticator;
    private final Backend backend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    public ListObjectsHandler(Authenticator authenticator, Backend backend, ObjectMapper mapper, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.LIST_OBJECTS);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_LIST_OBJECT_BUNDLE);
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.LISTOBJECT_REQUEST_COUNT);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = request.response();

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.LIST_OBJECTS)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        final int limit = HttpPathQueryHelpers.getLimit((request)).orElse(DEFAULT_LIMIT);
        final Character delimiter = HttpPathQueryHelpers.getDelimiter(request);
        final ImmutableList<ObjectProperties> requestedProperties =
                HttpPathQueryHelpers.getRequestedObjectParametersForList(request);
        final String startWith = HttpPathQueryHelpers.getStartWith(request).orElse(null);
        final String endBefore = HttpPathQueryHelpers.getEndBefore(request).orElse(null);
        final String prefix = HttpPathQueryHelpers.getPrefix(request).orElse(null);
        final String modifiedStartWith = KeyUtils.modifyStartWith(startWith, delimiter, prefix);
        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(context);
            final ObjectSummaryCollection summary = backend.listObjects(
                    context,
                    authInfo,
                    requestedProperties,
                    limit,
                    namespace,
                    bucketName,
                    delimiter,
                    prefix,
                    modifiedStartWith,
                    endBefore);

            HttpContentHelpers.writeJsonResponse(request,
                    response,
                    postProcessObjectSummaryCollection(summary, limit),
                    mapper);
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }


    public ObjectSummaryCollection postProcessObjectSummaryCollection(final ObjectSummaryCollection summary,
                                                                      int limit) {
        //No post-processing required if we are within limit
        int summarySize = summary.getObjects() != null ? Iterables.size(summary.getObjects()) : 0;
        int prefixesSize = summary.getPrefixes() != null ? Iterables.size(summary.getPrefixes()) : 0;
        if (summarySize + prefixesSize <= limit) {
            return summary;
        }

        List<ObjectSummary> objects = new ArrayList<>();
        List<String> prefixes = new ArrayList<>();

        summary.getObjects().forEach(objects::add);
        summary.getPrefixes().forEach(prefixes::add);


        /*
        As per the backend logic there can be one extra item(object or prefix) more than the limit.
        We need to remove the item and set the nextObject accordingly
        */
        String nextStartWith; //next start will be the last object/prefix in the result (which was removed)
        if (summarySize == 0) {
            nextStartWith = prefixes.remove(--prefixesSize);
        } else if (prefixesSize == 0) {
            nextStartWith = objects.remove(--summarySize).getName();
        } else {
            String lastPrefix = prefixes.get(prefixesSize - 1);
            String lastObject = objects.get(summarySize - 1).getName();
            if (lastPrefix.compareTo(lastObject) > 0) {
                nextStartWith = prefixes.remove(--prefixesSize);
            } else nextStartWith = objects.remove(--summarySize).getName();
        }

        return new ObjectSummaryCollection(objects, summary.getBucketName(), prefixes,
                nextStartWith, summary.getSecondLastObjectInQuery(), summary.getLastObjectInQuery());
    }
}
