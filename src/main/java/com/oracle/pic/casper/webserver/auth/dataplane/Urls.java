package com.oracle.pic.casper.webserver.auth.dataplane;

import org.glassfish.jersey.uri.internal.JerseyUriBuilder;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public final class Urls {
    private Urls() {
    }

    public static URI derivedKey(String keyId, String region, String date, String service) {
        return new JerseyUriBuilder()
                .path("/v1")
                .path("keys")
                .path("{keyId}")
                .path("{region}")
                .path("{date}")
                .path("{service}")
                .build(keyId, region, date, service);
    }

    public static URI swiftAuth() {
        return UriBuilder.fromPath("/v1/authentication").queryParam("mode", "swift").build();
    }

    static URI swiftCredentials() {
        return UriBuilder.fromPath("/v1/authentication/credentials").queryParam("mode", "swift").build();
    }

}
