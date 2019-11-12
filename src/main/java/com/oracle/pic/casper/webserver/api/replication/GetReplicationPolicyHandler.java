package com.oracle.pic.casper.webserver.api.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.exceptions.NotFoundException;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.backend.WorkRequestBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.model.WsReplicationPolicy;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

public class GetReplicationPolicyHandler extends SyncHandler {

    private final Authenticator authenticator;
    private final GetObjectBackend getBackend;
    private final WorkRequestBackend workRequestBackend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    public GetReplicationPolicyHandler(Authenticator authenticator,
                                       GetObjectBackend getBackend,
                                       WorkRequestBackend workRequestBackend,
                                       ObjectMapper mapper,
                                       EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.getBackend = getBackend;
        this.workRequestBackend = workRequestBackend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
                CasperOperation.GET_REPLICATION_POLICY);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_GET_REPLICATION_POLICY_BUNDLE);

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        HttpServerRequest request = context.request();
        HttpServerResponse response = context.response();

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String policyId = HttpPathQueryHelpers.getReplicationPolicyId(request);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.V2)
                .setOperation(CasperOperation.GET_REPLICATION_POLICY)
                .setNamespace(namespace)
                .setBucket(bucketName)
                .build();
        embargoV3.enter(embargoV3Operation);

        try {
            AuthenticationInfo authInfo = authenticator.authenticate(context);
            WsReplicationPolicy result = workRequestBackend.getReplicationPolicy(context, authInfo,
                    namespace, bucketName, policyId, CasperOperation.GET_REPLICATION_POLICY);
            if (result.getStatus() == null) {
                throw new NotFoundException(
                        String.format("Replication policy with id '%s' not found", policyId));
            } else {
                HttpContentHelpers.writeJsonResponse(request, response,
                        ReplicationUtils.toReplicationPolicy(result), mapper);
            }
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
