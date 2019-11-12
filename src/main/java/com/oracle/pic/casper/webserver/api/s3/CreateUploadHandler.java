package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.CreateUploadRequest;
import com.oracle.pic.casper.webserver.api.model.UploadMetadata;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.s3.model.InitiateMultipartUploadResult;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.util.Map;

import static com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers.getUserMetadataHeaders;

public class CreateUploadHandler extends SyncHandler {

    private final S3Authenticator authenticator;
    private final Backend backend;
    private final XmlMapper mapper;
    private final EmbargoV3 embargoV3;

    public CreateUploadHandler(S3Authenticator authenticator, Backend backend, XmlMapper mapper, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_CREATE_UPLOAD_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.CREATE_MULTIPART_UPLOAD);

        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);
        final String objectName = S3HttpHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.CREATE_MULTIPART_UPLOAD)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);

        try {
            AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
            Map<String, String> metadata = getUserMetadataHeaders(request, HttpHeaderHelpers.UserMetadataPrefix.S3);
            CreateUploadRequest createUploadRequest = new CreateUploadRequest(namespace, bucketName, objectName,
                metadata, null, null);
            UploadMetadata uploadMetadata = backend.beginMultipartUpload(context, authInfo, createUploadRequest);
            InitiateMultipartUploadResult result = new InitiateMultipartUploadResult(uploadMetadata.getBucketName(),
                uploadMetadata.getObjectName(), uploadMetadata.getUploadId());
            S3HttpHelpers.writeXmlResponse(context.response(), result, mapper);
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
