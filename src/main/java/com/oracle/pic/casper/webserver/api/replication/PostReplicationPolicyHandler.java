package com.oracle.pic.casper.webserver.api.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.bmc.objectstorage.model.CreateReplicationPolicyDetails;
import com.oracle.pic.casper.common.config.v2.ReplicationConfiguration;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.NotFoundException;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.WorkRequestBackend;
import com.oracle.pic.casper.webserver.api.common.AsyncBodyHandler;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.WsReplicationPolicy;
import com.oracle.pic.casper.webserver.api.model.exceptions.ReplicationPolicyClientException;
import com.oracle.pic.casper.webserver.api.model.exceptions.ReplicationPolicyTimeoutException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;

/**
 * Vert.x HTTP handler for creating (POST) a Cross Region Replication Policy from a source bucket to a destination
 * bucket.
 */
public class PostReplicationPolicyHandler extends AsyncBodyHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PostReplicationPolicyHandler.class);

    private final AsyncAuthenticator authenticator;
    private final WorkRequestBackend workRequestBackend;
    private final ObjectMapper mapper;
    private final ReplicationConfiguration replicationConfiguration;
    private final EmbargoV3 embargoV3;

    public PostReplicationPolicyHandler(AsyncAuthenticator authenticator,
                                        WorkRequestBackend workRequestBackend,
                                        ObjectMapper mapper,
                                        CountingHandler.MaximumContentLength maximumContentLength,
                                        ReplicationConfiguration replicationConfiguration,
                                        EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.workRequestBackend = workRequestBackend;
        this.mapper = mapper;
        this.replicationConfiguration = replicationConfiguration;
        this.embargoV3 = embargoV3;
    }

    @Override
    protected void validateHeaders(RoutingContext context) {

    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context, Buffer buffer) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
                CasperOperation.CREATE_REPLICATION_POLICY);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_CREATE_REPLICATION_POLICY_BUNDLE);

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        HttpServerRequest request = context.request();
        HttpServerResponse response = context.response();

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final CreateReplicationPolicyDetails details =
                HttpContentHelpers.readReplicationPolicyDetails(request, bucketName, buffer.getBytes(), mapper);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.CREATE_REPLICATION_POLICY)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        return authenticator.authenticate(context,
                Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(buffer.getBytes()).asBytes()))
                .thenAcceptAsync(authInfo -> {
                    WsReplicationPolicy replicationPolicy =
                            workRequestBackend.createReplicationPolicy(context, authInfo, namespace, bucketName,
                                    details.getName(),
                                    details.getDestinationRegionName(),
                                    details.getDestinationBucketName());

                    final Instant startTime = Instant.now();

                    final long timerId = context.vertx().setPeriodic(1000, id -> {
                        LOG.info("Checking replication policy '{}' status to be ACTIVE",
                                replicationPolicy.getPolicyId());
                        VertxUtil.runAsync(() -> {
                            final WsReplicationPolicy wsReplicationPolicy;
                            try {
                                wsReplicationPolicy = workRequestBackend.getReplicationPolicy(context,
                                        authInfo, namespace, bucketName, replicationPolicy.getPolicyId(),
                                        CasperOperation.CREATE_REPLICATION_POLICY);
                            } catch (NotFoundException ex) {
                                LOG.warn("Replication policy '{}' not found, deleted by master",
                                        replicationPolicy.getPolicyId());
                                context.fail(new ReplicationPolicyClientException(
                                        "Failed to create replication policy"));
                                return;
                            }

                            if (wsReplicationPolicy.getStatus() != null) {
                                // Policy is created in DB.
                                HttpContentHelpers.writeJsonResponse(request, response,
                                        ReplicationUtils.toReplicationPolicy(wsReplicationPolicy), mapper);
                            } else if (Instant.now().isAfter(startTime.plusSeconds(
                                    replicationConfiguration.getMasterTimeout().getSeconds()))) {
                                LOG.warn("Replication policy '{}' creation timed out", replicationPolicy.getPolicyId());
                                context.fail(new ReplicationPolicyTimeoutException("Request processing timed out"));
                            }
                        });
                    });
                    WSRequestContext.get(context).pushEndHandler(
                            connectionClosed -> context.vertx().cancelTimer(timerId));
                }, VertxExecutor.workerThread());
    }
}
