package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.s3.model.CopyObjectDetails;
import com.oracle.pic.casper.webserver.api.s3.model.CopyObjectResult;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.NotImplementedException;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

public class UpdateMetadataHandler extends SyncHandler {

    private final S3Authenticator authenticator;
    private final Backend backend;
    private final XmlMapper mapper;
    private final EmbargoV3 embargoV3;

    public UpdateMetadataHandler(S3Authenticator authenticator, Backend backend, XmlMapper mapper, EmbargoV3 embargo) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargo;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.REPLACE_OBJECT_METADATA);
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_PUT_OBJECT_BUNDLE);
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.PUTOBJECT_REQUEST_COUNT);
        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);
        final String destinationObject = S3HttpHelpers.getObjectName(request, wsRequestContext);
        final String destinationBucket = S3HttpHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.REPLACE_OBJECT_METADATA)
            .setNamespace(namespace)
            .setBucket(destinationBucket)
            .setObject(destinationObject)
            .build();
        embargoV3.enter(embargoOperation);

        final AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
        final String ifMatch = S3HttpHelpers.unquoteIfNeeded(
                request.getHeader("x-amz-copy-source-if-match"));
        final String ifNoneMatch = S3HttpHelpers.unquoteIfNeeded(
                request.getHeader("x-amz-copy-source-if-none-match"));
        final Date ifModifiedSince = DateUtil.s3HttpHeaderStringToDate(S3HttpHelpers.unquoteIfNeeded(
                request.getHeader("x-amz-copy-source-if-modified-since")));
        final Date ifUnmodifiedSince = DateUtil.s3HttpHeaderStringToDate(S3HttpHelpers.unquoteIfNeeded(
                request.getHeader("x-amz-copy-source-if-unmodified-since")));
        final String copySource = S3HttpHelpers.unquoteIfNeeded(request.getHeader("x-amz-copy-source"));
        final String metadataDirective = S3HttpHelpers.unquoteIfNeeded(
                request.getHeader("x-amz-metadata-directive"));
        try {
            // remove the first prefix slash from copy source header
            final String splitCopySource = copySource.startsWith("/") ? copySource.substring(1) : copySource;
            final String decodeSource = URLDecoder.decode(splitCopySource, StandardCharsets.UTF_8.name());
            final String[] source = decodeSource.split("/", 2);
            if (source.length != 2) {
                throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT,
                        "x-amz-copy-source is not specified correctly. it should be source_bucket/object",
                        // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                        request.path().replace("/%2F", "//"));
            }
            final String sourceBucket = source[0];
            final String sourceObject = source[1];
            final Map<String, String> metadata = HttpHeaderHelpers.getUserMetadataHeaders(
                    request, HttpHeaderHelpers.UserMetadataPrefix.S3);

            if (!sourceBucket.equals(destinationBucket) || !sourceObject.equals(destinationObject)) {
                throw new NotImplementedException("S3 Copy Object operation to different object is not supported.");
            }
            if (metadataDirective == null || !metadataDirective.equals("REPLACE") || metadata.isEmpty()) {
                throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT,
                        "x-amz-metadata-directive should be equal to REPLACE for copy object to itself and " +
                        "new user metadata should be specified",
                        // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                        request.path().replace("/%2F", "//"));
            }
            CopyObjectDetails copyObjectDetails = new CopyObjectDetails(
                    sourceBucket,
                    sourceObject,
                    destinationBucket,
                    destinationObject,
                    ifMatch,
                    ifNoneMatch,
                    ifUnmodifiedSince != null ? ifUnmodifiedSince.toInstant() : null,
                    ifModifiedSince != null ? ifModifiedSince.toInstant() : null,
                    metadata);
            CopyObjectResult result = backend.s3CopyObjectToSelf(context, authInfo, namespace,
                    sourceBucket, copyObjectDetails);
            S3HttpHelpers.writeXmlResponse(response, result, mapper);
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
