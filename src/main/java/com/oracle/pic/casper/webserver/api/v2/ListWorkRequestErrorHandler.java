package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.encryption.Obfuscate;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.rest.CommonHeaders;
import com.oracle.pic.casper.mds.workrequest.MdsWorkRequest;
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
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.casper.workrequest.WorkRequestDetail;
import com.oracle.pic.casper.workrequest.WorkRequestError;
import com.oracle.pic.casper.workrequest.WorkRequestStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.List;

public class ListWorkRequestErrorHandler extends SyncHandler {
    private final Authenticator authenticator;
    private final ObjectMapper mapper;
    private final WorkRequestBackend backend;
    private final EmbargoV3 embargoV3;

    public ListWorkRequestErrorHandler(Authenticator authenticator, ObjectMapper mapper, WorkRequestBackend backend,
                                       EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.mapper = mapper;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.GET_WORK_REQUEST);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_LIST_WORK_REQUEST_ERROR_BUNDLE);
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final String requestId = HttpPathQueryHelpers.getWorkRequestId(request);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.GET_WORK_REQUEST)
            .build();
        embargoV3.enter(embargoV3Operation);

        final AuthenticationInfo authInfo = authenticator.authenticate(context);
        final Pair<MdsWorkRequest, WorkRequestDetail> workRequestAndDetail =
                backend.getRequest(context, authInfo, requestId);
        final WorkRequestStatus status = MdsTransformer.mdsWorkRequestStatusToWorkRequestStatus(
                workRequestAndDetail.getFirst());
        final List<WorkRequestError> workRequestErrorJson = status.getErrors();
        final int startWith;
        try {
            startWith = HttpPathQueryHelpers.getPage(request).map(x -> Integer.parseInt(x))
                    .orElse(0);
        } catch (NumberFormatException e) {
            throw new HttpException(V2ErrorCode.INVALID_PAGE,
                    "The 'page' query parameter is not valid.", request.path(), e);
        }
        if (startWith != 0 && (startWith < 0 || startWith >= workRequestErrorJson.size())) {
            throw new HttpException(V2ErrorCode.INVALID_PAGE,
                    "The 'page' query parameter is not valid.", request.path());
        }
        final int limit = HttpPathQueryHelpers.getLimit(request).orElse(1000);

        List<WorkRequestError> logCurPageList = workRequestErrorJson.subList(startWith,
                Integer.min(startWith + limit, workRequestErrorJson.size()));
        if (startWith + logCurPageList.size() != workRequestErrorJson.size()) {
            response.putHeader(CommonHeaders.OPC_NEXT_PAGE_HEADER,
                    Obfuscate.obfuscate(startWith + logCurPageList.size() + ""));
        }
        HttpContentHelpers.writeJsonResponse(request, response, logCurPageList, mapper);
    }
}
