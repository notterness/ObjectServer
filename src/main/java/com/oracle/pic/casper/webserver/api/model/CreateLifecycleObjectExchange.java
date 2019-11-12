package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import io.vertx.core.http.HttpServerRequest;

/**
 * Class that encapsulates all request details for creating a lifecycle object.
 */
public final class CreateLifecycleObjectExchange {

    private final HttpServerRequest request;
    private final PutObjectLifecyclePolicyDetails putObjectLifecyclePolicyDetails;
    private final ObjectMetadata objectMetadata;
    private final AuthenticationInfo authInfo;
    private final ObjectMapper mapper;

    public CreateLifecycleObjectExchange(HttpServerRequest request,
                                         PutObjectLifecyclePolicyDetails putObjectLifecyclePolicyDetails,
                                         ObjectMetadata objectMetadata,
                                         AuthenticationInfo authInfo,
                                         ObjectMapper mapper) {
        this.request = request;
        this.putObjectLifecyclePolicyDetails = putObjectLifecyclePolicyDetails;
        this.objectMetadata = objectMetadata;
        this.authInfo = authInfo;
        this.mapper = mapper;
    }

    public HttpServerRequest getRequest() {
        return request;
    }

    public PutObjectLifecyclePolicyDetails getPutObjectLifecyclePolicyDetails() {
        return putObjectLifecyclePolicyDetails;
    }

    public ObjectMetadata getObjectMetadata() {
        return objectMetadata;
    }

    public AuthenticationInfo getAuthInfo() {
        return authInfo;
    }

    public ObjectMapper getMapper() {
        return mapper;
    }
}
