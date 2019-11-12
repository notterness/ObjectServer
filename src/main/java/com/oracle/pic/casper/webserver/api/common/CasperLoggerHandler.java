package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.LoggerFormat;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.impl.Utils;

import java.text.DateFormat;
import java.util.Date;

/**
 * A handler for writing access logs based on Vertx' {@link io.vertx.ext.web.handler.impl.LoggerHandlerImpl}, but with
 * PAR nonces (secrets) sanitized out of the request paths before logging them.
 *
 * The code in this class is almost identical to the Vertx 3.4.2 LoggerHandlerImpl, but with the sanitization regex
 * applied and some checkstyle stuff fixed to make it pass our compilation.  Vertx is licensed under the Apache
 * License v2.0, so copying and modifying without redistributing is safe to do.  --jfriedly 2017-11-08
 */
public class CasperLoggerHandler implements LoggerHandler {

    private final Logger logger;
    private final DateFormat dateTimeFormat;
    private final boolean immediate;
    private final LoggerFormat format;

    public CasperLoggerHandler(boolean immediate, LoggerFormat format) {
        this.logger = LoggerFactory.getLogger(this.getClass());
        this.dateTimeFormat = Utils.createRFC1123DateTimeFormatter();
        this.immediate = immediate;
        this.format = format;
    }

    public CasperLoggerHandler(LoggerFormat format) {
        this(false, format);
    }

    public CasperLoggerHandler() {
        this(LoggerFormat.DEFAULT);
    }

    private String getClientAddress(SocketAddress inetSocketAddress) {
        return inetSocketAddress == null ? null : inetSocketAddress.host();
    }

    private void log(RoutingContext context, long timestamp, String remoteClient, HttpVersion version,
                     HttpMethod method, String uri) {
        HttpServerRequest request = context.request();
        long contentLength = 0L;
        String versionFormatted;
        if (this.immediate) {
            versionFormatted = request.headers().get("content-length");
            if (versionFormatted != null) {
                try {
                    contentLength = Long.parseLong(versionFormatted);
                } catch (NumberFormatException var16) {
                    contentLength = 0L;
                }
            }
        } else {
            contentLength = request.response().bytesWritten();
        }

        switch (version) {
            case HTTP_1_0:
                versionFormatted = "HTTP/1.0";
                break;
            case HTTP_1_1:
                versionFormatted = "HTTP/1.1";
                break;
            case HTTP_2:
                versionFormatted = "HTTP/2.0";
                break;
            default:
                versionFormatted = "-";
                break;
        }

        int status = request.response().getStatusCode();
        String message = null;
        switch (this.format) {
            case SHORT:
                message = String.format("%s - %s %s %s %d %d - %d ms", remoteClient,
                        method, uri, versionFormatted,
                        status, contentLength, System.currentTimeMillis() - timestamp);
                break;
            case TINY:
                message = String.format("%s %s %d %d - %d ms", method, uri, status, contentLength,
                        System.currentTimeMillis() - timestamp);
                break;
            case DEFAULT:
            default:
                String referrer = request.headers().get("referrer");
                String userAgent = request.headers().get("user-agent");
                referrer = referrer == null ? "-" : referrer;
                userAgent = userAgent == null ? "-" : userAgent;
                message = String.format("%s - - [%s] \"%s %s %s\" %d %d \"%s\" \"%s\"",
                        remoteClient,
                        this.dateTimeFormat.format(new Date(timestamp)), method, uri, versionFormatted, status,
                        contentLength, referrer, userAgent);
                break;
        }

        this.doLog(status, message);
    }

    protected void doLog(int status, String message) {
        if (status >= 500) {
            this.logger.error(message);
        } else if (status >= 400) {
            this.logger.warn(message);
        } else {
            this.logger.info(message);
        }

    }

    public void handle(RoutingContext context) {
        long timestamp = System.currentTimeMillis();
        String remoteClient = this.getClientAddress(context.request().remoteAddress());
        HttpMethod method = context.request().method();
        // Sanitize PAR nonces (secrets) out of request paths.
        String uri = CommonRequestContext.casperSanitize(context.request().uri());
        HttpVersion version = context.request().version();
        if (this.immediate) {
            this.log(context, timestamp, remoteClient, version, method, uri);
        } else {
            final WSRequestContext wsRequestContext = WSRequestContext.get(context);
            wsRequestContext.pushEndHandler(c -> this.log(context, timestamp, remoteClient, version, method, uri));
        }
        context.next();
    }
}
