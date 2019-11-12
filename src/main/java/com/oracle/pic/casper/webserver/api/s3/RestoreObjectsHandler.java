package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.AlreadyRestoredObjectException;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.RestoreObjectsDetails;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.s3.model.RestoreRequest;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.time.Duration;

/**
 * Compatibility layer for S3's singular RestoreObject API:
 * https://docs.aws.amazon.com/AmazonS3/latest/dev/restoring-objects-java.html
 *
 * Note:  S3 returns 409 if you try to restore an object that's already being restored.  We return 202.
 */
public class RestoreObjectsHandler extends SyncBodyHandler {

    private final S3Authenticator authenticator;
    private final Backend backend;
    private final XmlMapper mapper;
    private final EmbargoV3 embargoV3;

    public RestoreObjectsHandler(S3Authenticator authenticator,
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
        S3HttpHelpers.validateContentMD5ContentLengthHeaders(context.request());

    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_RESTORE_OBJECT_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.RESTORE_OBJECT);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);
        final String objectName = S3HttpHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.RESTORE_OBJECT)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateSha256(contentSha256, bytes, request);

        ChecksumHelper.checkContentMD5(request, bytes);

        final RestoreRequest restoreRequest = S3HttpHelpers.parseRestoreRequest(bytes, request);
        final Duration timeToArchive = Duration.ofDays(restoreRequest.getDays());

        try {
            AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
            final RestoreObjectsDetails restoreObjectsDetails = new RestoreObjectsDetails(objectName,
                    timeToArchive.toHours());
            backend.restoreObject(context, authInfo, namespace, bucketName, restoreObjectsDetails);
            response.setStatusCode(HttpResponseStatus.ACCEPTED).end();
        } catch (AlreadyRestoredObjectException ex) {
            /* S3 returns 200 on multiple restore request */
            response.setStatusCode(HttpResponseStatus.OK).end();
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
