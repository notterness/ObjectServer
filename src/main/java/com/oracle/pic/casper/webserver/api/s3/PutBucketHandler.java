package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.ContentLengthRequiredException;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.common.V2BucketCreationChecks;
import com.oracle.pic.casper.webserver.api.model.BucketCreate;
import com.oracle.pic.casper.webserver.api.model.s3.CreateBucketConfiguration;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;

/**
 * Vert.x HTTP handler for S3 Api to create a bucket.
 */
public class PutBucketHandler extends SyncBodyHandler {

    private final S3Authenticator authenticator;
    private final BucketBackend bucketBackend;
    private final TenantBackend tenantBackend;
    private final XmlMapper xmlMapper;
    private final ResourceControlledMetadataClient metadataClient;
    private final EmbargoV3 embargoV3;

    public PutBucketHandler(
            S3Authenticator authenticator,
            BucketBackend bucketBackend,
            TenantBackend tenantBackend,
            XmlMapper xmlMapper,
            ResourceControlledMetadataClient metadataClient,
            CountingHandler.MaximumContentLength maximumContentLength,
            EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.bucketBackend = bucketBackend;
        this.tenantBackend = tenantBackend;
        this.xmlMapper = xmlMapper;
        this.metadataClient = metadataClient;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new S3HttpException(
                S3ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
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
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.CREATE_BUCKET);

        final HttpServerRequest request = context.request();
        HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.CREATE_BUCKET)
            .setBucket(bucketName)
            .setNamespace(namespace)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        // if body is not empty, verify the region
        if (bytes.length > 0) {
            S3HttpHelpers.validateSha256(contentSha256, bytes, request);
            try {
                CreateBucketConfiguration configuration = xmlMapper.readValue(bytes, CreateBucketConfiguration.class);
                if (!ConfigRegion.fromSystemProperty().getFullName()
                    .equalsIgnoreCase(configuration.getLocationConstraint())) {
                    throw new S3HttpException(S3ErrorCode.INVALID_REQUEST,
                            "The region of the bucket must be the same as the region you are sending the request to.",
                            request.path());
                }
            } catch (IOException e) {
                throw new S3HttpException(S3ErrorCode.MALFORMED_XML,
                        "The XML you provided was not well-formed or did not validate against our published schema.",
                        request.path());
            }
        } else {
            S3HttpHelpers.validateEmptySha256(contentSha256, request);
        }

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();

        try {
            AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
            String compartmentId = request.getHeader(S3Api.OPC_COMPARTMENT_ID_HEADER);
            Tenant tenant =
                V2BucketCreationChecks.checkNamespace(metadataClient, authInfo, compartmentId, namespace);
            WSRequestContext.get(context).setResourceTenantOcid(tenant.getId());

            // Create the namespace with the tenant ocid associated with the namespace.
            MdsNamespace mdsNamespace =
                tenantBackend.getOrCreateNamespace(context, scope, new NamespaceKey(Api.V2, namespace), tenant.getId());
            if (compartmentId == null) {
                compartmentId = mdsNamespace.getDefaultS3Compartment();
            }

            final String createdBy = authInfo.getMainPrincipal().getSubjectId();
            // S3 has no bucket metadata
            final BucketCreate bucketCreate = new BucketCreate(namespace, bucketName, compartmentId, createdBy, null);

            bucketBackend.createV2Bucket(context, authInfo, bucketCreate);
            response.putHeader(HttpHeaders.LOCATION, S3HttpHelpers.getLocation(bucketName));
            response.setStatusCode(HttpResponseStatus.OK).end();
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
