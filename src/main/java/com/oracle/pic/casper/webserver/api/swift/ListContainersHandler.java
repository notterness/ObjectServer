package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.model.BucketSummary;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.model.swift.SwiftContainer;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Lists containers in an account.
 *
 * Swift implementation notes:
 *
 * Swift has a default page limit of 10000 and returns 412 Precondition Failed if you ask it for more than 10000
 * entries.  We do the same, for now.  If we run into perf issues, we may drop the limit down, or do multiple DB
 * calls with a smaller limit.
 *
 * Only users with access to the tenancy will be authorized to list Swift containers.  Only containers (buckets)
 * created at the tenancy level will be visible.  This is akin to saying that "only super users can use Swift".  In
 * the future, we plan to create a "default Swift compartment" concept, which will allow users to set a compartment
 * ID for use with Swift.  All buckets visible to Swift will need to be created in that compartment, and any user
 * with permissions on that compartment will be able to use Swift.  That will require a DB change for us though, and
 * we're currently scrambling to prep for analyst demos, so we're pushing that off a bit.
 *
 * http://developer.openstack.org/api-ref-objectstorage-v1.html#showContainerDetails
 */
public class ListContainersHandler extends SyncHandler {

    private final Authenticator authenticator;
    private final BucketBackend backend;
    private final TenantBackend tenantBackend;
    private final SwiftResponseWriter responseWriter;
    private final EmbargoV3 embargoV3;

    public ListContainersHandler(Authenticator authenticator,
                                 BucketBackend backend,
                                 TenantBackend tenantBackend,
                                 SwiftResponseWriter responseWriter,
                                 EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.tenantBackend = tenantBackend;
        this.responseWriter = responseWriter;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.SWIFT_LIST_CONTAINERS_BUNDLE);
        WSRequestContext.setOperationName("swift", getClass(), context, CasperOperation.LIST_BUCKETS);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = SwiftHttpPathHelpers.getNamespace(request, wsRequestContext);
        final int limit = SwiftHttpQueryHelpers.getLimit(request)
                .orElse(SwiftHttpQueryHelpers.MAX_LIMIT);
        final String marker = SwiftHttpQueryHelpers.getMarker(request)
                .orElse(null);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.Swift)
            .setNamespace(namespace)
            .setOperation(CasperOperation.LIST_BUCKETS)
            .build();
        embargoV3.enter(embargoV3Operation);

        final SwiftResponseFormat format = SwiftHttpContentHelpers.readResponseFormat(request);
        commonContext.getMetricScope().annotate("format", format.toString());

        // Inform clients that we don't support range requests.
        response.putHeader(HttpHeaders.ACCEPT_RANGES, "none");
        // TODO(jfriedly):  Send back an X-Timestamp header with the creation time for the principal or compartment
        try {
            final String tenantOcid = tenantBackend.getNamespace(context, scope, new NamespaceKey(Api.V2, namespace))
                    .map(MdsNamespace::getTenantOcid)
                    .orElse(null);
            final AuthenticationInfo authInfo =
                    authenticator.authenticateSwiftOrCavage(context, null, namespace, tenantOcid);
            final PaginatedList<BucketSummary> buckets = backend.listBucketsInCompartment(
                    context, authInfo, namespace,
                    tenantOcid != null ? tenantOcid : authInfo.getMainPrincipal().getTenantId(), limit, marker);
            final List<SwiftContainer> containers = buckets.getItems().stream()
                    .map(SwiftContainer::fromBucketSummary)
                    .collect(Collectors.toList());
            responseWriter.writeContainersResponse(context, format, containers);
        } catch (Exception ex) {
            throw SwiftHttpExceptionHelpers.rewrite(context, ex);
        }
    }
}
