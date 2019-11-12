package com.oracle.pic.casper.webserver.api.replication;

import com.oracle.pic.casper.common.config.v2.ReplicationConfiguration;
import com.oracle.pic.casper.common.exceptions.NotFoundException;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.WorkRequestBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.exceptions.ReplicationPolicyTimeoutException;
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

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public class DeleteReplicationPolicyHandler extends CompletableHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteReplicationPolicyHandler.class);

    private final AsyncAuthenticator asyncAuthenticator;
    private final WorkRequestBackend workRequestBackend;
    private final ReplicationConfiguration replicationConfiguration;
    private final EmbargoV3 embargoV3;

    public DeleteReplicationPolicyHandler(AsyncAuthenticator asyncAuthenticator,
                                          WorkRequestBackend workRequestBackend,
                                          ReplicationConfiguration replicationConfiguration,
                                          EmbargoV3 embargoV3) {
        this.asyncAuthenticator = asyncAuthenticator;
        this.workRequestBackend = workRequestBackend;
        this.replicationConfiguration = replicationConfiguration;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
                CasperOperation.DELETE_REPLICATION_POLICY);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_DELETE_REPLICATION_POLICY_BUNDLE);

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        HttpServerRequest request = context.request();
        HttpServerResponse response = context.response();

        HttpMatchHelpers.validateConditionalHeaders(request,
                HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.NO);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String policyId = HttpPathQueryHelpers.getReplicationPolicyId(request);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.V2)
                .setOperation(CasperOperation.DELETE_REPLICATION_POLICY)
                .setNamespace(namespace)
                .setBucket(bucketName)
                .build();
        embargoV3.enter(embargoV3Operation);

        return asyncAuthenticator.authenticate(context).thenAcceptAsync(authInfo -> {
            workRequestBackend.deleteReplicationPolicy(context, authInfo, namespace, bucketName, policyId);

            final Instant startTime = Instant.now();
            final long timerId = context.vertx().setPeriodic(1000, id -> {
                LOG.info("Checking replication policy {} is deleted in DB", policyId);
                VertxUtil.runAsync(() -> {
                    try {
                        workRequestBackend.getReplicationPolicy(context, authInfo, namespace, bucketName,
                                policyId, CasperOperation.DELETE_REPLICATION_POLICY);
                    } catch (NotFoundException ex) {
                        response.setStatusCode(HttpResponseStatus.NO_CONTENT).end();
                        return;
                    }
                    if (Instant.now().isAfter(startTime.plusSeconds(
                            replicationConfiguration.getMasterTimeout().getSeconds()))) {
                        LOG.warn("Replication policy '{}' deletion timed out", policyId);
                        context.fail(new ReplicationPolicyTimeoutException("Request processing timed out"));
                    }
                });
            });
            WSRequestContext.get(context).pushEndHandler(
                    connectionClosed -> context.vertx().cancelTimer(timerId));
        }, VertxExecutor.workerThread());
    }
}
