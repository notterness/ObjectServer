package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.ParAuthenticator;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;


import java.util.concurrent.CompletableFuture;

/**
 * Runtime handler for supporting HEAD operations on objects via PARs.
 * Note that the HEAD handler uses a custom authenticator i.e PAR based
 * {@link ParAuthenticator}
 *
 * The authenticator looks up the PAR associated with the request.
 * Once the PAR is looked up successfully, the handler delegates the read call to the backend
 * and processes the operation as if it were a regular authenticated HEAD request for that object.
 */
public class ParHeadObjectHandler extends CompletableHandler {

    private final V2ReadObjectHelper readObjectHelper;
    private final EmbargoV3 embargoV3;

    public ParHeadObjectHandler(ParAuthenticator parAuthenticator,
                                GetObjectBackend getObjectBackend,
                                DecidingKeyManagementService kms,
                                EmbargoV3 embargoV3) {
        this.readObjectHelper = new V2ReadObjectHelper(parAuthenticator, getObjectBackend, kms);
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.HEAD_OBJECT);
        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final HttpServerRequest request = context.request();
        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.HEAD_OBJECT)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        return readObjectHelper
                .beginHandleCompletably(
                        context, WebServerMetrics.V2_GET_OBJECT_BUNDLE, GetObjectBackend.ReadOperation.HEAD)
                .thenAccept(optionalReadStorageObjectExchange ->
                    optionalReadStorageObjectExchange
                        .ifPresent(readStorageObjectExchange -> context.response().end())
                );
    }
}
