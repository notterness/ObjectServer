package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.model.PartMetadata;
import com.oracle.pic.casper.webserver.api.model.UploadIdentifier;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.s3.model.ListPartsResult;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ListUploadPartsHandler extends SyncHandler {

    private final S3Authenticator authenticator;
    private final Backend backend;
    private final XmlMapper mapper;
    private final EmbargoV3 embargoV3;
    static final int DEFAULT_LIMIT = 1000;

    public ListUploadPartsHandler(S3Authenticator authenticator, Backend backend, XmlMapper mapper, EmbargoV3 embargo) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargo;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_LIST_UPLOAD_PARTS_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.LIST_MULTIPART_UPLOAD_PARTS);

        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);
        final String objectName = S3HttpHelpers.getObjectName(request, wsRequestContext);
        final String uploadId = S3HttpHelpers.getUploadID(request);
        final Integer partMarker = S3HttpHelpers.getPartMarker(request).orElse(null);
        final Optional<Integer> limit = S3HttpHelpers.getMaxParts(request);
        final Optional<String> encode = S3HttpHelpers.getEncoding(request);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.LIST_MULTIPART_UPLOAD_PARTS)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);

        try {
            AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
            UploadIdentifier uploadIdentifier = new UploadIdentifier(namespace, bucketName, objectName, uploadId);
            PaginatedList<PartMetadata> paginatedList = backend.listUploadParts(context, authInfo,
                limit.orElse(DEFAULT_LIMIT),
                uploadIdentifier, partMarker);
            List<PartMetadata> partMetadatas = paginatedList.getItems();
            List<ListPartsResult.Part> parts = partMetadatas.stream().map(metadata ->
                new ListPartsResult.Part(metadata)).collect(Collectors.toList());
            ListPartsResult result = new ListPartsResult(bucketName, encode.orElse(null), objectName, uploadId,
                partMarker,
                paginatedList.isTruncated() ? partMetadatas.get(partMetadatas.size() - 1).getPartNumber() : null,
                limit.orElse(DEFAULT_LIMIT), paginatedList.isTruncated(), parts, S3StorageClass.STANDARD,
                authInfo.getMainPrincipal());
            S3HttpHelpers.writeXmlResponse(context.response(), result, mapper);
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
