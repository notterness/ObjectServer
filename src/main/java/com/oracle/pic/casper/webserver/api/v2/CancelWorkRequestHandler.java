package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.NotFoundException;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.backend.WorkRequestBackend;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.casper.workrequest.WorkRequestWrongStateException;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * Handler to cancel a work request.  Request will be in CANCELING state by the time of response and later moved to
 * CANCELED once it has been properly cleaned up.
 */
public class CancelWorkRequestHandler extends SyncHandler {

    private final Authenticator authenticator;
    private final WorkRequestBackend backend;
    private final EmbargoV3 embargoV3;

    public CancelWorkRequestHandler(Authenticator authenticator, WorkRequestBackend backend, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.CANCEL_WORK_REQUEST);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_CANCEL_WORK_REQUEST_BUNDLE);
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.CANCEL_WORK_REQUEST)
            .build();
        embargoV3.enter(embargoV3Operation);

        final String requestId = HttpPathQueryHelpers.getWorkRequestId(request);
        final AuthenticationInfo authInfo = authenticator.authenticate(context);
        try {
            backend.cancelRequest(context, authInfo, requestId);
        } catch (WorkRequestWrongStateException e) {
            throw new HttpException(V2ErrorCode.NOT_CANCELABLE, request.path(), e);
        } catch (NotFoundException e) {
            throw new HttpException(V2ErrorCode.NOT_FOUND, request.path(), e);
        }
        response.setStatusCode(HttpResponseStatus.ACCEPTED).end();
    }
}
