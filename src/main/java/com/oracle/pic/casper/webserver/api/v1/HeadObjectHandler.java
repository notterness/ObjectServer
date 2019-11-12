package com.oracle.pic.casper.webserver.api.v1;

import com.oracle.pic.casper.objectmeta.ObjectKey;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.ext.web.RoutingContext;

/**
 * Returns metadata about an object as HTTP headers. Metadata includes the eTag,
 * MD5 and any user metadata saved on the object. If the object doesn't exist,
 * this handler will return a 404 error.
 */
public class HeadObjectHandler extends AbstractRouteHandler {

    private final V1ReadObjectHelper readObjectHelper;
    private final EmbargoV3 embargoV3;

    public HeadObjectHandler(V1ReadObjectHelper readObjectHelper, EmbargoV3 embargoV3) {
        this.readObjectHelper = readObjectHelper;
        this.embargoV3 = embargoV3;
    }

    @Override
    protected void subclassHandle(RoutingContext context) {
        final ObjectKey objectKey = RequestHelpers.computeObjectKey(context.request());
        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V1)
            .setNamespace(objectKey.getBucket().getNamespace())
            .setBucket(objectKey.getBucket().getName())
            .setObject(objectKey.getName())
            .setOperation(CasperOperation.HEAD_OBJECT)
            .build();
        embargoV3.enter(embargoV3Operation);

        readObjectHelper
                .beginHandleCompletably(
                        context, WebServerMetrics.V1_HEAD_OBJECT_BUNDLE, GetObjectBackend.ReadOperation.HEAD)
                .thenAccept(optionalReadStorageObjectExchange ->
                        optionalReadStorageObjectExchange
                                .ifPresent(readStorageObjectExchange -> context.response().end())
                )
                .exceptionally(throwable -> HeadObjectHandler.fail(context, throwable));

    }
}
