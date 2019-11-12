package com.oracle.pic.casper.webserver.api.swift;

import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.model.swift.ContainerAndObject;
import com.oracle.pic.casper.webserver.api.model.swift.SwiftBulkDeleteResponse;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class BulkDeleteHandler extends SyncBodyHandler {

    private final Authenticator authenticator;
    private final Backend backend;
    private final TenantBackend tenantBackend;
    private final SwiftResponseWriter swiftResponseWriter;
    private final EmbargoV3 embargoV3;
    //the maximum objects deletion requests allowed in one http request
    static final int DEFAULT_LIMIT = 10000;
    //bulk delete requests are differentiated from other POST requests to the account path by this query parameter,
    //in the form of POST /v1/myaccount?bulk-delete
    static final String QUERYPARAM = "bulk-delete";

    public BulkDeleteHandler(
            Authenticator authenticator,
            Backend backend,
            TenantBackend tenantBackend,
            SwiftResponseWriter swiftResponseWriter,
            EmbargoV3 embargoV3,
            CountingHandler.MaximumContentLength maximumContentLength) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.backend = backend;
        this.tenantBackend = tenantBackend;
        this.embargoV3 = embargoV3;
        this.swiftResponseWriter = swiftResponseWriter;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {
        final HttpServerRequest request = context.request();
        //max content body length is 10000 requests of longestbucketname/longestobjectname\n
        SwiftHttpHeaderHelpers.negotiateUTF8Content(request,
            DEFAULT_LIMIT * (Validator.MAX_BUCKET_LENGTH + 2 + Validator.MAX_OBJECT_LENGTH));
        if (!request.params().contains(QUERYPARAM)) {
            throw new HttpException(V2ErrorCode.NOT_IMPLEMENTED,
                "This Swift implementation does not support this operation.", context.request().path());
        }
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        WSRequestContext.setOperationName("swift", getClass(), context, CasperOperation.DELETE_OBJECT);
        MetricsHandler.addMetrics(context, WebServerMetrics.SWIFT_BULK_DELETE_BUNDLE);
        final HttpServerRequest request = context.request();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final String namespace = SwiftHttpPathHelpers.getNamespace(request, wsRequestContext);

        // Setting the visa to allow for re-entries, as each of the operations in the bulk make an 'authorize' call
        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final SwiftResponseFormat format = SwiftHttpContentHelpers.readResponseFormat(request);
        commonContext.getMetricScope().annotate("format", format.toString());

        Pair<List<ContainerAndObject>, List<SwiftBulkDeleteResponse.ErrorObject>> subrequests =
            SwiftHttpContentHelpers.readContainerAndObjectList(request, bytes, DEFAULT_LIMIT);
        List<SwiftBulkDeleteResponse.ErrorObject> errors = subrequests.getSecond();
        final String tenantOcid = tenantBackend.getNamespace(context, scope, new NamespaceKey(Api.V2, namespace))
                .map(MdsNamespace::getTenantOcid)
                .orElse(null);

        final String bodySha256 = Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes());
        final AuthenticationInfo authInfo =
                authenticator.authenticateSwiftOrCavage(context, bodySha256, namespace, tenantOcid);
        int numberDeleted = 0;
        int numberNotFound = 0;
        for (ContainerAndObject containerAndObject : subrequests.getFirst()) {
            wsRequestContext.setObjectName(containerAndObject.getObject());
            wsRequestContext.setBucketName(containerAndObject.getContainer());
            try {
                final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
                    .setApi(EmbargoV3Operation.Api.Swift)
                    .setOperation(CasperOperation.DELETE_OBJECT)
                    .setNamespace(namespace)
                    .setBucket(containerAndObject.getContainer())
                    .setObject(containerAndObject.getObject())
                    .build();
                // If this throws an exception it will be caught below and
                // reported as a 503 for the object.
                embargoV3.enter(embargoOperation);
                Optional<Date> result = backend.deleteV2Object(context, authInfo, namespace,
                    containerAndObject.getContainer(), containerAndObject.getObject(), null);
                if (result.isPresent()) {
                    numberDeleted++;
                } else {
                    numberNotFound++;
                    ServiceLogsHelper.logServiceEntry(context);
                }
            } catch (NoSuchBucketException e) {
                numberNotFound++;
            } catch (Exception e) {
                HttpException httpException = (HttpException) HttpException.rewrite(request, e);
                errors.add(new SwiftBulkDeleteResponse.ErrorObject(containerAndObject.toString(),
                    httpException.getErrorCode().toString()));
                ServiceLogsHelper.logServiceEntry(context, httpException.getErrorCode());
            }
        }
        SwiftBulkDeleteResponse bulkDeleteResponse = new SwiftBulkDeleteResponse(numberDeleted, numberNotFound, errors);
        swiftResponseWriter.writeDeletesResponse(context, format, bulkDeleteResponse);
    }
}
