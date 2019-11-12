package com.oracle.pic.casper.webserver.api;

import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;

public interface Api {
    Router createRouter(Vertx vertx);
}
