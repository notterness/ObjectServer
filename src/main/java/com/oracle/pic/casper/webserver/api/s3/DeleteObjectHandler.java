package com.oracle.pic.casper.webserver.api.s3;

import com.oracle.pic.casper.common.exceptions.NotFoundException;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.common.util.ThrowableUtil;
import com.oracle.pic.casper.common.vertx.HttpServerUtil;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidObjectNameException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.NotImplementedException;

import java.util.Date;
import java.util.Optional;

/**
 * Vert.x HTTP handler for S3 Api to delete objects.
 */
public class DeleteObjectHandler extends SyncHandler {
    private final S3Authenticator authenticator;
    private final Backend backend;
    private final EmbargoV3 embargoV3;

    public DeleteObjectHandler(S3Authenticator authenticator, Backend backend, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_DELETE_OBJECT_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.DELETE_OBJECT);

        final HttpServerRequest request = context.request();
        final String firstParam = HttpServerUtil.getRoutingParam(request);
        if (firstParam != null) {
            if (S3Api.TAGGING_PARAM.equals(firstParam)) {
                throw new NotImplementedException("S3 Delete Object Tagging operation is not supported.");
            }
            throw new NotFoundException("Not Found");
        }

        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.DELETEOBJECT_REQUEST_COUNT);

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);
        final String objectName = S3HttpHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.DELETE_OBJECT)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);

        // S3 does not use conditional headers for object deletion, so we just pass null.
        final String expectedETag = null;

        try {
            AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
            final Optional<Date> deleteDate = backend.deleteV2Object(context, authInfo, namespace, bucketName,
                    objectName, expectedETag);

            deleteDate.ifPresent(date ->
                    response.putHeader(HttpHeaders.LAST_MODIFIED, DateUtil.httpFormattedDate(date)));
            response.setStatusCode(HttpResponseStatus.NO_CONTENT);
            //Since we don't throw an error if S3 deleted a non existent object, we log a service entry here
            if (!deleteDate.isPresent()) {
                ServiceLogsHelper.logServiceEntry(context);
            }
            response.end();
        } catch (Exception ex) {
            Throwable thrown = ThrowableUtil.getUnderlyingThrowable(ex);
            if (thrown instanceof InvalidObjectNameException) {
                response.setStatusCode(HttpResponseStatus.NO_CONTENT).end();
                return;
            }
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
