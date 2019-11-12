package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.model.PartMetadata;
import com.oracle.pic.casper.webserver.api.model.UploadIdentifier;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

/**
 * Vert.x HTTP handler for listing buckets in a namespace.
 */
public class ListUploadPartsHandler extends SyncHandler {
    static final int DEFAULT_LIMIT = 100;

    private final Authenticator authenticator;
    private final Backend backend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;

    public ListUploadPartsHandler(Authenticator authenticator, Backend backend, ObjectMapper mapper,
                                  EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.LIST_MULTIPART_UPLOAD_PARTS);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_LIST_UPLOAD_PARTS_BUNDLE);

        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);
        final String uploadId = HttpPathQueryHelpers.getUploadId(request);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.LIST_MULTIPART_UPLOAD_PARTS)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        final UploadIdentifier uploadIdentifier = new UploadIdentifier(namespace, bucketName, objectName, uploadId);

        final int limit = HttpPathQueryHelpers.getLimit(request).orElse(DEFAULT_LIMIT);
        final Integer page = HttpPathQueryHelpers.getUploadPartPage(request);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(context);
            final PaginatedList<PartMetadata> parts = backend.listUploadParts(
                    context, authInfo, limit, uploadIdentifier, page);
            HttpContentHelpers.writeJsonResponse(
                    request,
                    context.response(),
                    parts.getItems(),
                    mapper,
                    HttpHeaderHelpers.opcNextPageHeader(parts, PartMetadata::partNumberAsString));
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}

