package com.oracle.pic.casper.webserver.api.s3;

import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.transform.Unmarshallers;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.ContentLengthRequiredException;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Vert.x HTTP handler for S3 Api to update tagging of a given bucket.
 */
public class PutBucketTaggingHandler extends SyncBodyHandler {

    private final S3Authenticator authenticator;
    private final BucketBackend backend;
    private final EmbargoV3 embargoV3;

    public PutBucketTaggingHandler(S3Authenticator authenticator,
                                   BucketBackend backend,
                                   CountingHandler.MaximumContentLength maximumContentLength,
                                   EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new S3HttpException(
                S3ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void handle(RoutingContext context) {
        super.handle(context);
    }

    @Override
    public void validateHeaders(RoutingContext context) {
        String contentLenStr = context.request().getHeader(HttpHeaders.CONTENT_LENGTH);
        if (contentLenStr == null) {
            throw new ContentLengthRequiredException("The Content-Length header is required");
        }
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_PUT_BUCKET_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.UPDATE_BUCKET);

        HttpServerRequest request = context.request();
        HttpServerResponse response = context.response();

        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.UPDATE_BUCKET)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);

        if (bytes.length > 0) {
            S3HttpHelpers.validateSha256(contentSha256, bytes, request);
        } else {
            S3HttpHelpers.validateEmptySha256(contentSha256, request);
        }

        final BucketTaggingConfiguration bucketTaggingConfiguration;
        try {
            // deserialize Tagging xml to BucketTaggingConfiguration
            final Unmarshallers.BucketTaggingConfigurationUnmarshaller unmarshaller =
                    new Unmarshallers.BucketTaggingConfigurationUnmarshaller();
            bucketTaggingConfiguration = unmarshaller.unmarshall(new ByteArrayInputStream(bytes));
        } catch (Exception ex) {
            throw new S3HttpException(S3ErrorCode.MALFORMED_XML,
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                    request.path());
        }
        // Amazon S3 only support one TagSet
        if (bucketTaggingConfiguration != null && bucketTaggingConfiguration.getAllTagSets().size() > 1) {
            throw  new S3HttpException(S3ErrorCode.MALFORMED_XML,
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                    request.path());
        }

        try {
            Map<String, String> freeformTags = new HashMap<>();
            if (bucketTaggingConfiguration != null && bucketTaggingConfiguration.getAllTagSets().size() > 0) {
                freeformTags = bucketTaggingConfiguration.getAllTagSets().get(0).getAllTags();
            }
            final AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
            backend.updateBucketTags(context, authInfo, namespace, bucketName, freeformTags, null);
            response.setStatusCode(HttpResponseStatus.OK).end();
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
