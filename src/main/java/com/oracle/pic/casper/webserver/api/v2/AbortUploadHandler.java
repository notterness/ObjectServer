package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.common.exceptions.MultipartUploadNotFoundException;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.UploadIdentifier;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * Vert.x HTTP handler for aborting an existing upload.
 */
public class AbortUploadHandler extends SyncHandler {
    private final Authenticator authenticator;
    private final Backend backend;
    private final EmbargoV3 embargoV3;

    public AbortUploadHandler(Authenticator authenticator, Backend backend, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.ABORT_MULTIPART_UPLOAD);

        MetricsHandler.addMetrics(context, WebServerMetrics.V2_ABORT_UPLOAD_BUNDLE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);
        final String uploadId = HttpPathQueryHelpers.getUploadId(request);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.ABORT_MULTIPART_UPLOAD)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        final UploadIdentifier uploadMetadata = new UploadIdentifier(namespace, bucketName, objectName, uploadId);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(context);
            backend.abortMultipartUpload(context, authInfo, uploadMetadata);
            response.setStatusCode(HttpResponseStatus.NO_CONTENT).end();
        } catch (MultipartUploadNotFoundException ex) {
            response.setStatusCode(HttpResponseStatus.NO_CONTENT);
            // Since 204 entries are not logged by the ServiceLogHandler
            ServiceLogsHelper.logServiceEntry(context);
            response.end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
