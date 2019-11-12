package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.rest.CommonHeaders;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public final class CorsHelpers {

    public static void addCorsHeaders(HttpServerRequest request, HttpServerResponse response) {

        Set<CharSequence> allHeaders = new TreeSet<>();
        for (String header : response.headers().names()) {
            allHeaders.add(header.toLowerCase());
        }

        String requestedHeaders = request.getHeader(HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS);
        if (requestedHeaders != null) {
            response.putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS, requestedHeaders);
        }

        allHeaders.add(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN.toString());
        allHeaders.add(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS.toString());
        allHeaders.add(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS.toString());
        allHeaders.add(CommonHeaders.OPC_CLIENT_INFO);

        response.putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        response.putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS, "POST,PUT,GET,HEAD,DELETE,OPTIONS");
        response.putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        response.putHeader(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS,
                allHeaders.stream().collect(Collectors.joining(CommonHeaders.HEADER_VALUE_SEPARATOR)));
    }

    private CorsHelpers() {

    }
}
