package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectProperties;
import com.oracle.pic.casper.webserver.api.model.SwiftObjectSummaryCollection;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;
import com.oracle.pic.casper.webserver.api.model.swift.SwiftContainer;
import com.oracle.pic.casper.webserver.api.model.swift.SwiftObject;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.ConfigValue;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * List objects in a container.
 *
 * Swift implementation notes:
 *
 * Swift has a default page limit of 10000 and returns 412 Precondition Failed if you ask it for more than 10000
 * entries.  We do the same, for now.  If we run into perf issues, we may drop the limit down, or do multiple DB
 * calls with a smaller limit.
 *
 * http://developer.openstack.org/api-ref-objectstorage-v1.html#showContainerDetails
 */
public class ListObjectsHandler extends SyncHandler {

    private final Authenticator authenticator;
    private final Backend backend;
    private final TenantBackend tenantBackend;
    private final SwiftResponseWriter responseWriter;
    private final EmbargoV3 embargoV3;

    /**
     * Used to determine whether we want to disable the fix for CASPER-6145.
     */
    private final ConfigValue casper6145FixDisabled;

    public ListObjectsHandler(
            Authenticator authenticator,
            Backend backend,
            TenantBackend tenantBackend,
            SwiftResponseWriter responseWriter,
            EmbargoV3 embargoV3,
            ConfigValue casper6145FixDisabled) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.tenantBackend = tenantBackend;
        this.responseWriter = responseWriter;
        this.embargoV3 = embargoV3;
        this.casper6145FixDisabled = casper6145FixDisabled;
    }

    /**
     * Translate a paged list of objects from the v2 API to Swift's API.
     *
     * Swift uses different semantics for pagination than v2, so we have to jump through some hoops.  V2 returns a list
     * of objects and a "next page key", and it expects the user to pass the next page key back in.  Subsequent paged
     * requests will "start with" the next page key.  Swift returns a list of objects and expects the user to treat the
     * last object in the list as a next page key, but subsequent paged requests will start with the next key *after*
     * the one passed in.  This method is responsible for translating our Backend's object list into a Swift object
     * list.
     */
    private static List<SwiftObject> swiftPaginate(
            SwiftObjectSummaryCollection collection, @Nullable String marker, int limit) {
        final List<SwiftObject> objectsList = new ArrayList<>(limit);
        final Iterator<ObjectSummary> objectsIterator = collection.getObjects().iterator();

        if (objectsIterator.hasNext()) {
            ObjectSummary firstObject = objectsIterator.next();
            // If the user didn't pass in a marker, then this is the first page and we'd better include the first
            // object.
            if (marker == null) {
                objectsList.add(SwiftObject.fromObjectSummary(firstObject));
            } else if (!marker.equals(firstObject.getName())) {
                // If they did pass in a marker, then the list we got back from the Backend will probably start with
                // it.  But not necessarily, if the object was deleted, so we sanity-check here and include the first
                // object if it doesn't match.
                objectsList.add(SwiftObject.fromObjectSummary(firstObject));
            }
        }

        while (objectsIterator.hasNext() && objectsList.size() < limit) {
            objectsList.add(SwiftObject.fromObjectSummary(objectsIterator.next()));
        }

        if (objectsList.size() < limit && collection.getNextObject() != null) {
            objectsList.add(SwiftObject.fromObjectSummary(collection.getNextObject()));
        }

        return objectsList;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.SWIFT_LIST_OBJECTS_BUNDLE);
        WSRequestContext.setOperationName("swift", getClass(), context, CasperOperation.LIST_OBJECTS);


        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.LISTOBJECT_REQUEST_COUNT);

        final String namespace = SwiftHttpPathHelpers.getNamespace(request, wsRequestContext);
        final String containerName = SwiftHttpPathHelpers.getContainerName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.Swift)
            .setOperation(CasperOperation.LIST_OBJECTS)
            .setNamespace(namespace)
            .setBucket(containerName)
            .build();
        embargoV3.enter(embargoOperation);

        final int limit = SwiftHttpQueryHelpers.getLimit(request)
                .orElse(SwiftHttpQueryHelpers.MAX_LIMIT);
        final String marker = SwiftHttpQueryHelpers.getMarker(request)
                .orElse(null);
        final String prefix = SwiftHttpPathHelpers.getListPrefix(request);

        final SwiftResponseFormat format = SwiftHttpContentHelpers.readResponseFormat(request);
        commonContext.getMetricScope().annotate("format", format.toString());

        // Inform clients that we don't support range requests.
        response.putHeader(HttpHeaders.ACCEPT_RANGES, "none");

        try {
            final String tenantOcid = tenantBackend.getNamespace(context, scope, new NamespaceKey(Api.V2, namespace))
                    .map(MdsNamespace::getTenantOcid)
                    .orElse(null);
            final AuthenticationInfo authInfo =
                    authenticator.authenticateSwiftOrCavage(context, null, namespace, tenantOcid);

            final List<ObjectProperties> properties = new ArrayList<>();
            properties.add(ObjectProperties.NAME);
            properties.add(ObjectProperties.SIZE);
            properties.add(ObjectProperties.MD5);
            properties.add(ObjectProperties.TIMEMODIFIED);

            // CASPER-6145: "Swift list should not return content-type"
            // We do not want to pay the cost of decrypting the customer
            // metadata in list, so we will not request the metadata
            // property. If the fix is disabled we will request the
            // metadata property.
            if (casper6145FixDisabled.getValueAsBoolean()) {
                properties.add(ObjectProperties.METADATA);
            }

            final SwiftObjectSummaryCollection summary = backend.listSwiftObjects(
                    context,
                    authInfo,
                    properties,
                    limit,
                    namespace,
                    containerName,
                    null,
                    prefix,
                    marker,
                    null);

            final SwiftContainer container = SwiftContainer.fromBucket(summary.getBucketName(), summary.getMetadata());
            final List<SwiftObject> swiftObjects = swiftPaginate(summary, marker, limit);

            responseWriter.writeObjectsResponse(context, format, swiftObjects, container);
        } catch (Exception ex) {
            throw SwiftHttpExceptionHelpers.rewrite(context, ex);
        }
    }
}
