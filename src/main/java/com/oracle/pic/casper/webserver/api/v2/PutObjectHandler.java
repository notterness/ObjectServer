package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
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

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Vert.x HTTP handler for creating or overwriting objects.
 */
public class PutObjectHandler extends CompletableHandler {

    private final V2PutObjectHelper v2PutObjectHelper;
    private final EmbargoV3 embargoV3;

    public PutObjectHandler(TrafficController controller,
                            AsyncAuthenticator authenticator,
                            PutObjectBackend backend,
                            WebServerConfiguration webServerConfiguration,
                            List<String> servicePrincipals,
                            EmbargoV3 embargoV3) {
        this.v2PutObjectHelper = new V2PutObjectHelper(controller, authenticator, backend, webServerConfiguration,
                servicePrincipals);
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.PUT_OBJECT);
        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.PUTOBJECT_REQUEST_COUNT);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.PUT_OBJECT)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        return v2PutObjectHelper.handleCompletably(context, namespace, bucketName, objectName);
    }
}
