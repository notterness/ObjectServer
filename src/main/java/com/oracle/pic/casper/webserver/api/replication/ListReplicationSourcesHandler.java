package com.oracle.pic.casper.webserver.api.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.WorkRequestBackend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.model.ReplicationSource;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.List;

public class ListReplicationSourcesHandler extends SyncHandler {

    private final Authenticator authenticator;
    private final ObjectMapper mapper;
    private final WorkRequestBackend backend;
    private final EmbargoV3 embargoV3;
    private static final int DEFAULT_LIMIT = 100;

    public ListReplicationSourcesHandler(Authenticator authenticator, ObjectMapper mapper, WorkRequestBackend backend,
                                         EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.mapper = mapper;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.LIST_REPLICATION_SOURCES);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_LIST_REPLICATION_SOURCES_BUNDLE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final int limit = HttpPathQueryHelpers.getLimit(request).orElse(DEFAULT_LIMIT);
        final String page = HttpPathQueryHelpers.getPage(request).orElse(null);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.V2)
                .setOperation(CasperOperation.LIST_REPLICATION_SOURCES)
                .setNamespace(namespace)
                .setBucket(bucketName)
                .build();
        embargoV3.enter(embargoV3Operation);

        final AuthenticationInfo authInfo = authenticator.authenticate(context);

        final ReplicationSource replicationSource =
            backend.listReplicationSources(context, authInfo, namespace, bucketName, limit, page);
        final List<ReplicationSource> replicationSourceList = replicationSource == null ?
            ImmutableList.of() : ImmutableList.of(replicationSource);

        HttpContentHelpers.writeJsonResponse(request, response, replicationSourceList, mapper);
    }
}
