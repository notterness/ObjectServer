package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.json.JacksonSerDe;
import com.oracle.pic.casper.common.json.JsonSerializationException;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.model.UploadIdentifier;
import com.oracle.pic.casper.webserver.api.model.UploadMetadata;
import com.oracle.pic.casper.webserver.api.model.UploadPageToken;
import com.oracle.pic.casper.webserver.api.model.UploadPaginatedList;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

/**
 * Vert.x HTTP handler for listing uploads.
 */
public class ListUploadsHandler extends SyncHandler {
    static final int DEFAULT_LIMIT = 25;

    private final Authenticator authenticator;
    private final Backend backend;
    private final ObjectMapper mapper;
    private final JacksonSerDe jacksonSerDe;
    private final EmbargoV3 embargoV3;

    public ListUploadsHandler(Authenticator authenticator, Backend backend, ObjectMapper mapper,
                              JacksonSerDe jacksonSerDe, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.jacksonSerDe = jacksonSerDe;
        this.embargoV3 = embargoV3;
    }

    private HttpHeaderHelpers.Header nextPageHeader(PaginatedList<UploadMetadata> paginatedList) {
        return HttpHeaderHelpers.opcNextPageHeader(paginatedList, uploadMetadata ->
                jacksonSerDe.toJson(uploadMetadata.toPage()));
    }

    private PaginatedList<UploadMetadata> toPaginatedList(UploadPaginatedList uploadPaginatedList) {
        return new PaginatedList<>(uploadPaginatedList.getUploads(), uploadPaginatedList.isTruncated());
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.LIST_MULTIPART_UPLOAD);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_LIST_UPLOADS_BUNDLE);

        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.LIST_MULTIPART_UPLOAD)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        final int limit = HttpPathQueryHelpers.getLimit(request).orElse(DEFAULT_LIMIT);
        final String page = HttpPathQueryHelpers.getPage(request).orElse(null);
        // v2 doesn't need to validate UploadIdMarker
        final boolean validateUploadIdMarker = false;

        String objectName = null;
        String uploadId = null;

        if (page != null) {
            UploadPageToken pageToken;
            try {
                pageToken = jacksonSerDe.fromJson(page, UploadPageToken.class);
            } catch (JsonSerializationException e) {
                throw new HttpException(V2ErrorCode.INVALID_PAGE, request.path(), e);
            }
            objectName = pageToken.getObjectName();
            uploadId = pageToken.getUploadId();
        }

        final UploadIdentifier startFrom = new UploadIdentifier(namespace, bucketName, objectName, uploadId);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(context);
            UploadPaginatedList uploads = backend.listMultipartUploads(context, authInfo,
                    null, null, limit, validateUploadIdMarker, startFrom);
            HttpContentHelpers.writeJsonResponse(
                    request,
                    context.response(),
                    uploads.getUploads(),
                    mapper,
                    nextPageHeader(toPaginatedList(uploads)));
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}

