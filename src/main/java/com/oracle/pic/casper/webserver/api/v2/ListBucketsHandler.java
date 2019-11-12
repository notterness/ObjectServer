package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.input.BucketSortBy;
import com.oracle.pic.casper.objectmeta.input.BucketSortOrder;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.BucketProperties;
import com.oracle.pic.casper.webserver.api.model.BucketSummary;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;
import java.util.function.Function;

/**
 * Vert.x HTTP handler for listing buckets in a namespace.
 */
public class ListBucketsHandler extends SyncHandler {
    static final int DEFAULT_LIMIT = 25;
    // : is an illegal character in bucket name
    static final String PAGE_SEPARATOR = ":";

    private final Authenticator authenticator;
    private final BucketBackend backend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    public ListBucketsHandler(Authenticator authenticator, BucketBackend backend, ObjectMapper mapper,
                              EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.LIST_BUCKETS);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_LIST_BUCKETS_BUNDLE);

        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.NO,
                HttpMatchHelpers.IfNoneMatchAllowed.NO);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.LIST_BUCKETS)
            .setNamespace(namespace)
            .build();
        embargoV3.enter(embargoV3Operation);

        final String compartmentId = HttpPathQueryHelpers.getCompartmentId(request)
                .orElseThrow(() -> new HttpException(V2ErrorCode.MISSING_COMPARTMENT,
                    "The 'compartmentId' query parameter was missing", request.path()));
        final int limit = HttpPathQueryHelpers.getLimit(request).orElse(DEFAULT_LIMIT);
        final String pageToken = HttpPathQueryHelpers.getBucketsPage(request).orElse(null);
        final Set<BucketProperties> bucketProperties =
                HttpPathQueryHelpers.getRequestedBucketParametersForList(request, BucketProperties.TAGS);
        final BucketSortBy sortBy = HttpPathQueryHelpers.getSortBy(request).orElse(null);
        final BucketSortOrder sortOrder = HttpPathQueryHelpers.getSortOrder(request).orElse(null);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(context);

            String bucketNamePage = null;
            Instant timeCreatedPage = null;
            if (pageToken != null) {
                // if sortBy name, pageToken is bucketName; if sortBy timeCreated, pageToken is bucketName:timeCreated
                bucketNamePage = pageToken;
                if (BucketSortBy.TIMECREATED == sortBy) {
                    try {
                        String[] pageSplit = pageToken.split(PAGE_SEPARATOR, 2);
                        Preconditions.checkArgument(pageSplit.length == 2);
                        bucketNamePage = pageSplit[0];
                        timeCreatedPage = Instant.parse(pageSplit[1]);
                    } catch (IllegalArgumentException | DateTimeParseException ex) {
                        throw new HttpException(V2ErrorCode.INVALID_PAGE,
                            "The 'page' query parameter is not valid when sorting by TIMECREATED", request.path(), ex);
                    }
                }

            }
            Pair<String, Instant> page = new Pair<>(bucketNamePage, timeCreatedPage);
            Function<BucketSummary, String> pageFun = BucketSortBy.TIMECREATED == sortBy ?
                    bucketSummary -> bucketSummary.getBucketName() + PAGE_SEPARATOR +
                            String.valueOf(bucketSummary.getTimeCreated()) :
                    BucketSummary::getBucketName;

            final PaginatedList<BucketSummary> bucketList = backend.listBucketsInCompartment(
                    context, authInfo, namespace, compartmentId, limit, page, bucketProperties, sortBy, sortOrder);
            HttpContentHelpers.writeJsonResponse(
                    request,
                    context.response(),
                    bucketList.getItems(),
                    mapper,
                    HttpHeaderHelpers.opcNextPageHeader(bucketList, pageFun));
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}

