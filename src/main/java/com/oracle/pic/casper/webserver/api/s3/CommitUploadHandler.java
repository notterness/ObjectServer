package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.collect.ImmutableSortedSet;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.InvalidUploadPartException;
import com.oracle.pic.casper.common.util.GuavaCollectors;
import com.oracle.pic.casper.objectmeta.ETagType;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.FinishUploadRequest;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.UploadIdentifier;
import com.oracle.pic.casper.webserver.api.model.s3.CompleteMultipartUpload;
import com.oracle.pic.casper.webserver.api.model.s3.CompleteMultipartUploadResult;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.codec.DecoderException;

public class CommitUploadHandler extends SyncBodyHandler {

    private final S3Authenticator authenticator;
    private final Backend backend;
    private final XmlMapper mapper;
    private final EmbargoV3 embargoV3;

    public CommitUploadHandler(S3Authenticator authenticator,
                               Backend backend,
                               XmlMapper mapper,
                               CountingHandler.MaximumContentLength maximumContentLength,
                               EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new S3HttpException(
                S3ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {
        HttpContentHelpers.validateContentLengthHeader(context.request());
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_COMMIT_UPLOAD_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.COMMIT_MULTIPART_UPLOAD);

        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);
        final String objectName = S3HttpHelpers.getObjectName(request, wsRequestContext);
        final String uploadId = S3HttpHelpers.getUploadID(request);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.COMMIT_MULTIPART_UPLOAD)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateSha256(contentSha256, bytes, request);

        final CompleteMultipartUpload completeUpload;
        try {
            completeUpload = S3HttpHelpers.parseCompleteMultipartUpload(bytes);
        } catch (Exception ex) {
            throw new S3HttpException(S3ErrorCode.MALFORMED_XML,
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                    // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                    request.path().replace("/%2F", "//"));
        }
        final ImmutableSortedSet<FinishUploadRequest.PartAndETag> partsToCommit = completeUpload.getPart().stream()
                .map(part -> {
                    try {
                        return FinishUploadRequest.PartAndETag.builder().uploadPartNum(part.getPartNumber())
                                .eTag(ChecksumHelper.hexToBase64(part.getETag())).build();
                    } catch (DecoderException e) {
                        throw new InvalidUploadPartException("Unable to decode part ETag '" + part.getETag() + "'");
                    }
                }).collect(GuavaCollectors.toImmutableSortedSet());

        AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
        UploadIdentifier uploadIdentifier = new UploadIdentifier(namespace, bucketName, objectName, uploadId);
        FinishUploadRequest finishUploadRequest = new FinishUploadRequest(uploadIdentifier, ETagType.MD5,
                partsToCommit, null, null, null);
        final ObjectMetadata objMeta = backend.finishMultipartUpload(context, authInfo, finishUploadRequest);
        final String location = S3HttpHelpers.getLocation(bucketName, objectName);
        CompleteMultipartUploadResult result = new CompleteMultipartUploadResult(location, bucketName, objectName,
                uploadId, objMeta.getChecksum().getQuotedHex());
        try {
            S3HttpHelpers.writeXmlResponse(context.response(), result, mapper);
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
