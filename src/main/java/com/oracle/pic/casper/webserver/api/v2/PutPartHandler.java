package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

/**
 * Vert.x HTTP handler for fetching storage objects.
 */
public class PutPartHandler extends CompletableHandler {

    private final V2PutPartHelper v2PutPartHelper;
    private final EmbargoV3 embargoV3;

    public PutPartHandler(TrafficController controller,
                          AsyncAuthenticator authenticator,
                          PutObjectBackend backend,
                          WebServerConfiguration webServerConfiguration,
                          EmbargoV3 embargoV3) {
        this.v2PutPartHelper = new V2PutPartHelper(authenticator, backend, webServerConfiguration, controller);
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.UPLOAD_PART);

        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);
        final String uploadId = HttpPathQueryHelpers.getUploadId(request);
        final int uploadPartNum = HttpPathQueryHelpers.getUploadPartNumParam(request);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.UPLOAD_PART)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        return v2PutPartHelper.handleCompletably(context, namespace, bucketName, objectName, uploadId, uploadPartNum);
    }
}
