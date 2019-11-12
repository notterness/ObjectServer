package com.oracle.pic.casper.webserver.api.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.bmc.objectstorage.model.ReplicationPolicy;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.WorkRequestBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ListReplicationPoliciesHandler extends CompletableHandler {

    private static final int DEFAULT_LIMIT = 100;
    private static final Logger LOG = LoggerFactory.getLogger(ListReplicationPoliciesHandler.class);


    private final AsyncAuthenticator authenticator;
    private final WorkRequestBackend workRequestBackend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    public ListReplicationPoliciesHandler(AsyncAuthenticator authenticator,
                                          WorkRequestBackend workRequestBackend,
                                          ObjectMapper mapper,
                                          EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.workRequestBackend = workRequestBackend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.LIST_REPLICATION_POLICIES);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_LIST_REPLICATION_POLICIES_BUNDLE);

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        HttpServerRequest request = context.request();
        HttpServerResponse response = context.response();

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.LIST_REPLICATION_POLICIES)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        final int limit = HttpPathQueryHelpers.getLimit(request).orElse(DEFAULT_LIMIT);
        final String nextStartWith = HttpPathQueryHelpers.getPage(request).orElse(null);

        return authenticator.authenticate(context)
                .thenApplyAsync(authInfo -> workRequestBackend.listReplicationPolicies(context, authInfo, namespace,
                        bucketName, limit, nextStartWith), VertxExecutor.workerThread())
                .thenAccept(result -> {
                    List<ReplicationPolicy> list = result.getReplicationPolicyList().stream()
                            .map(ReplicationUtils::toReplicationPolicy).collect(Collectors.toList());
                    HttpContentHelpers.writeJsonResponse(request, response, list, mapper);
                }).exceptionally(throwable -> {
                    throw HttpException.rewrite(request, throwable);
                });
    }
}
