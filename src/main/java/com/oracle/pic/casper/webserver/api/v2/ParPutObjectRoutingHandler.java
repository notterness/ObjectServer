package com.oracle.pic.casper.webserver.api.v2;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

public class ParPutObjectRoutingHandler implements Handler<RoutingContext> {

    private final ParPutObjectHandler putObjectHandler;
    private final ParCreateUploadHandler createUploadHandler;

    public ParPutObjectRoutingHandler(ParPutObjectHandler putObjectHandler,
                                      ParCreateUploadHandler createUploadHandler) {
        this.putObjectHandler = putObjectHandler;
        this.createUploadHandler = createUploadHandler;
    }

    @Override
    public void handle(RoutingContext context) {
        if ("true".equals(context.request().getHeader(CasperApiV2.MULTIPART_PAR_HEADER))) {
            createUploadHandler.handle(context);
        } else {
            putObjectHandler.handle(context);
        }
    }
}
