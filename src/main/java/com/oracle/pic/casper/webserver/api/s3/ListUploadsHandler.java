package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.UploadIdentifier;
import com.oracle.pic.casper.webserver.api.model.UploadMetadata;
import com.oracle.pic.casper.webserver.api.model.UploadPaginatedList;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.s3.model.ListMultipartUploadsResult;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ListUploadsHandler extends SyncHandler {

    private final S3Authenticator authenticator;
    private final Backend backend;
    private final XmlMapper mapper;
    private final EmbargoV3 embargoV3;
    static final int DEFAULT_LIMIT = 1000;

    public ListUploadsHandler(S3Authenticator authenticator, Backend backend, XmlMapper mapper, EmbargoV3 embargo) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargo;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_LIST_UPLOADS_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.LIST_MULTIPART_UPLOAD);

        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);
        final String keyMarker = S3HttpHelpers.getKeyMarker(request);
        final String uploadIdMarker = keyMarker == null ? null : S3HttpHelpers.getUploadIdMarker(request);
        final Optional<Integer> limit = S3HttpHelpers.getMaxUploads(request);
        final String encoding = S3HttpHelpers.getEncoding(request).orElse(null);
        final String prefix = S3HttpHelpers.getPrefix(request);
        final String delimiter = S3HttpHelpers.getDelimiter(request);
        // S3 needs to validate UploadIdMarker.
        final boolean validateUploadIdMarker = true;

        //we only support 1 character delimiter
        Character delimitChar = (delimiter != null && delimiter.length() == 1) ? delimiter.charAt(0) : null;

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.LIST_MULTIPART_UPLOAD)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);

        try {
            AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
            final UploadIdentifier uploadIdentifier = new UploadIdentifier(namespace, bucketName,
                keyMarker, uploadIdMarker);
            final UploadPaginatedList uploadPaginatedList = backend.listMultipartUploads(context, authInfo,
                    delimitChar, prefix, limit.orElse(DEFAULT_LIMIT), validateUploadIdMarker, uploadIdentifier);
            final List<UploadMetadata> uploadMetadata = uploadPaginatedList.getUploads();
            final List<String> uploadPrefixes = uploadPaginatedList.getCommonPrefixes();

            List<ListMultipartUploadsResult.Upload> uploads = uploadMetadata.stream().map(metadata ->
                new ListMultipartUploadsResult.Upload(metadata, encoding != null, authInfo.getMainPrincipal()))
                .collect(Collectors.toList());
            final String nextKeyMarker = uploadMetadata.size() == 0 ? null :
                    uploadMetadata.get(uploadMetadata.size() - 1).getObjectName();
            final String nextUploadIdMarker = uploadMetadata.size() == 0  ? null :
                    uploadMetadata.get(uploadMetadata.size() - 1).getUploadId();

            List<ListMultipartUploadsResult.Prefix> commonPrefixes = uploadPrefixes == null ? null :
                    uploadPrefixes.stream().map(uploadPrefix ->
                            new ListMultipartUploadsResult.Prefix(uploadPrefix))
                            .collect(Collectors.toList());
            ListMultipartUploadsResult result = new ListMultipartUploadsResult(bucketName, keyMarker, uploadIdMarker,
                    nextKeyMarker, nextUploadIdMarker, encoding, limit.orElse(DEFAULT_LIMIT), delimiter, prefix,
                    commonPrefixes, uploadPaginatedList.isTruncated(), uploads, authInfo.getMainPrincipal());
            S3HttpHelpers.writeXmlResponse(context.response(), result, mapper);
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
