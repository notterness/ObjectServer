package com.oracle.pic.casper.webserver.api.s3;

import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.transform.BucketConfigurationXmlFactory;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchTagSetException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.tagging.client.tag.TagSet;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Map;
import java.util.Optional;

import static com.oracle.pic.casper.common.rest.ContentType.APPLICATION_XML_UTF8;

/**
 * Vert.x HTTP handler for S3 Api to get tagging of a given bucket.
 */
public class GetBucketTaggingHandler extends SyncHandler {

    private final S3Authenticator authenticator;
    private final BucketBackend bucketBackend;
    private final EmbargoV3 embargoV3;

    public GetBucketTaggingHandler(S3Authenticator authenticator, BucketBackend bucketBackend, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.bucketBackend = bucketBackend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handle(RoutingContext context) {
        super.handle(context);
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_GET_BUCKET_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.GET_BUCKET);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.GET_BUCKET)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);

        try {
            AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
            Optional<TagSet> tagSet = bucketBackend.getBucketTags(context, authInfo, namespace, bucketName);

            // AWS S3 returns 404 NoSuchTagSet exception if there is no TagSet of bucket
            if (!tagSet.isPresent() || tagSet.get().getFreeformTags().isEmpty()) {
                throw new NoSuchTagSetException("No Such TagSet exist in bucket '" + bucketName + "'");
            }

            // convert com.oracle.pic.tagging.client.tag.TagSet to Amazon BucketTaggingConfiguration
            BucketTaggingConfiguration bucketTaggingConfiguration = new BucketTaggingConfiguration();
            Map<String, String> freeformTags = tagSet.get().getFreeformTags();
            com.amazonaws.services.s3.model.TagSet s3TagSet = new com.amazonaws.services.s3.model.TagSet(freeformTags);
            bucketTaggingConfiguration.withTagSets(s3TagSet);
            byte[] body = (new BucketConfigurationXmlFactory()).convertToXmlByteArray(bucketTaggingConfiguration);

            response.setStatusCode(HttpResponseStatus.OK);
            response.putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_XML_UTF8);
            response.end(Buffer.buffer().appendBytes(body));
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
