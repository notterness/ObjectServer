package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.mds.workrequest.MdsWorkRequest;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.WorkRequestBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.MdsTransformer;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.model.WorkRequestJson;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.casper.workrequest.WorkRequestDetail;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * Handler to get information and status of a work request given its request id.
 */
public class GetWorkRequestHandler extends SyncHandler {

    private final Authenticator authenticator;
    private final ObjectMapper mapper;
    private final WorkRequestBackend backend;
    private final EmbargoV3 embargoV3;
    private static final String RETRY_AFTER_HEADER = "retry-after";

    public GetWorkRequestHandler(Authenticator authenticator, ObjectMapper mapper, WorkRequestBackend backend,
                                 EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.mapper = mapper;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.GET_WORK_REQUEST);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_GET_WORK_REQUEST_BUNDLE);
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
        //for now, let them retry after 5 seconds
        response.putHeader(RETRY_AFTER_HEADER, "5");
        final WorkRequestJson workRequestJson = MdsTransformer.createWorkRequestJson(
                workRequestAndDetail.getFirst(),
                workRequestAndDetail.getSecond());
        HttpContentHelpers.writeJsonResponse(request, response, workRequestJson, mapper);
    }
}
