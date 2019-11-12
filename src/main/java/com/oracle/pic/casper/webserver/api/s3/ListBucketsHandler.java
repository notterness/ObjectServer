package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.BucketSummary;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.s3.model.ListAllMyBucketsResult;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

public class ListBucketsHandler extends SyncHandler {

    private final S3Authenticator authenticator;
    private final BucketBackend backend;
    private final XmlMapper mapper;
    private final EmbargoV3 embargoV3;

    public ListBucketsHandler(
        S3Authenticator authenticator,
        BucketBackend backend,
        XmlMapper mapper,
        EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_GET_NAMESPACE_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.LIST_BUCKETS);

        final HttpServerRequest request = context.request();
        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);

        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        try {
            final AuthenticationInfo auth = authenticator.authenticate(context, contentSha256);

            final String namespaceName = S3HttpHelpers.getNamespace(request, wsRequestContext);
            final String compartmentId = request.getHeader(S3Api.OPC_COMPARTMENT_ID_HEADER);

            final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.S3)
                .setOperation(CasperOperation.LIST_BUCKETS)
                .setNamespace(namespaceName)
                .build();
            embargoV3.enter(embargoOperation);

            final PaginatedList<BucketSummary> buckets;
            if (compartmentId == null) {
                buckets = backend.listAllBucketsInNamespace(context, auth, namespaceName);
            } else {
                buckets = backend.listAllBucketsInCompartment(context, auth, namespaceName, compartmentId);
            }
            // filter out the internal buckets
            final PaginatedList<BucketSummary> filteredBuckets = BucketBackend.filterInternalBucket(buckets);

            ListAllMyBucketsResult result = new ListAllMyBucketsResult(auth.getMainPrincipal(), filteredBuckets);
            S3HttpHelpers.writeXmlResponse(context.response(), result, mapper);
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
