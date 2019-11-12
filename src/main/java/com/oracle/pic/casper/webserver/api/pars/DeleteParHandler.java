package com.oracle.pic.casper.webserver.api.pars;

import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.DeletePreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vert.x HTTP handler for retrieving a PAR.
 */
public class DeleteParHandler extends SyncHandler {

    private final Authenticator authenticator;
    private final PreAuthenticatedRequestBackend parStore;
    private final EmbargoV3 embargoV3;

    private static final Logger LOG = LoggerFactory.getLogger(DeleteParHandler.class);

    public DeleteParHandler(Authenticator authenticator, PreAuthenticatedRequestBackend parStore, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.parStore = parStore;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.DELETE_PAR);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_DELETE_PAR_BUNDLE);

        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String customerParId = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);
        final BackendParId parId = new CustomerParId(customerParId).toBackendParId();

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.DELETE_PAR)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(context);
            DeletePreAuthenticatedRequestRequest deleteRequest =
                    new DeletePreAuthenticatedRequestRequest(context, authInfo, parId);
            LOG.debug("Sending delete PAR request to PAR backend: " + deleteRequest);
            parStore.deletePreAuthenticatedRequest(deleteRequest);
            response.setStatusCode(HttpResponseStatus.NO_CONTENT).end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
