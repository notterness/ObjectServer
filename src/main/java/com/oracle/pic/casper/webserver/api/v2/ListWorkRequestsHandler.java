package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.encryption.Obfuscate;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.rest.CommonHeaders;
import com.oracle.pic.casper.mds.workrequest.ListWorkRequestsResponse;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.WorkRequestBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MdsTransformer;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.model.WorkRequestJson;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidWorkRequestTypeException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.casper.workrequest.WorkRequest;
import com.oracle.pic.casper.workrequest.WorkRequestDetail;
import com.oracle.pic.casper.workrequest.WorkRequestType;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.oracle.pic.casper.webserver.api.model.WorkRequestJsonHelper.WORK_REQUEST_DETAIL_CLASS;

/**
 * Handler to list work requests in a compartment.
 */
public class ListWorkRequestsHandler extends SyncHandler {

    private final Authenticator authenticator;
    private final ObjectMapper mapper;
    private final WorkRequestBackend backend;
    private final EmbargoV3 embargoV3;
    private static final int DEFAULT_LIMIT = 100;

    public ListWorkRequestsHandler(Authenticator authenticator, ObjectMapper mapper, WorkRequestBackend backend,
                                   EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.mapper = mapper;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.LIST_WORK_REQUESTS);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_LIST_WORK_REQUESTS_BUNDLE);
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final String compartmentId = HttpPathQueryHelpers.getCompartmentId(request)
            .orElseThrow(() -> new HttpException(V2ErrorCode.MISSING_COMPARTMENT,
                "The 'compartmentId' query parameter was missing", request.path()));

        // FIXME: This does not seem to be documented at
        // https://docs.cloud.oracle.com/iaas/api/#/en/objectstorage/20160918/WorkRequest/ListWorkRequests
        // Will this be documented?
        final String bucketName = HttpPathQueryHelpers.getBucketNameQueryParam(request).orElse(null);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.LIST_WORK_REQUESTS)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        final String startAfter = HttpPathQueryHelpers.getPage(request).orElse(null);
        final int limit = HttpPathQueryHelpers.getLimit(request).orElse(DEFAULT_LIMIT);
        final AuthenticationInfo authInfo = authenticator.authenticate(context);
        ListWorkRequestsResponse listWorkRequestsResponse;
        try {
            final String workRequestTypeQueryParam = HttpPathQueryHelpers.getWorkRequestType(request)
                    .orElse("COPY_OBJECT");
            WorkRequestType workRequestType = WorkRequestType.createFrom(workRequestTypeQueryParam);
            if (!workRequestType.getVisible()) {
                throw new IllegalArgumentException("Could not find a Work Request Type named " +
                        workRequestType.getName());
            }
            listWorkRequestsResponse = backend.listRequests(context, authInfo, workRequestType,
                    compartmentId, bucketName, startAfter, limit);
        } catch (IllegalArgumentException ex) {
            throw new InvalidWorkRequestTypeException(ex.getMessage());
        }
        if (!listWorkRequestsResponse.getNextPageToken().isEmpty()) {
            Preconditions.checkState(listWorkRequestsResponse.getWorkCount() != 0);
            response.putHeader(CommonHeaders.OPC_NEXT_PAGE_HEADER,
                    Obfuscate.obfuscate(listWorkRequestsResponse.getNextPageToken()));
        }

        final List<WorkRequestJson> summaries = listWorkRequestsResponse.getWorkList().stream().map(workRequest -> {
            final WorkRequestDetail detail;
            try {
                detail = getWorkRequestDetail(MdsTransformer.mdsWorkRequestToWorkRequest(workRequest));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return MdsTransformer.createWorkRequestJson(workRequest, detail);
        }).collect(Collectors.toList());
        HttpContentHelpers.writeJsonResponse(request, response, summaries, mapper);
    }

    private WorkRequestDetail getWorkRequestDetail(WorkRequest workRequest) throws IOException {
        Preconditions.checkState(workRequest.getType() == WorkRequestType.COPY
                || workRequest.getType() == WorkRequestType.BULK_RESTORE
                || workRequest.getType() == WorkRequestType.REENCRYPT);
        return mapper.readValue(workRequest.getRequestBlob(), WORK_REQUEST_DETAIL_CLASS.get(workRequest.getType()));
    }
}
