package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.common.NamespaceCaseWhiteList;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpServerRequest;

import javax.annotation.Nullable;

public final class SwiftHttpPathHelpers {

    /**
     * Fetch the account name from the URL of the request and validate it.
     * Lower cases the account name to support case insensitive look ups.
     */
    public static String getNamespace(HttpServerRequest request, WSRequestContext context) {
        String accountName = request.getParam(SwiftApi.ACCOUNT_PARAM);
        if (accountName == null) {
            throw new HttpException(V2ErrorCode.INTERNAL_SERVER_ERROR, "Internal server error", request.path());
        }
        accountName = NamespaceCaseWhiteList.lowercaseNamespace(Api.V2, accountName);
        context.setNamespaceName(accountName);
        return accountName;
    }

    // TODO(jfriedly):  Swift truncates characters after a hash or question mark in container names.  We should too.
    // TODO(jfriedly):  Swift allows container names to be > 63 characters.  If we start supporting Swift create
    // container, we'll need to figure something out there.
    public static String getContainerName(HttpServerRequest request, WSRequestContext context) {
        final String name = request.getParam(SwiftApi.CONTAINER_PARAM);
        if (name == null) {
            throw new HttpException(V2ErrorCode.INTERNAL_SERVER_ERROR, "Internal server error", request.path());
        }
        context.setBucketName(name);
        return name;
    }

    // TODO(jfriedly):  Swift truncates characters after a hash or question mark in object names.  We should too.
    // TODO(jfriedly):  Test Swift behavior with carriage return, new line, and the null byte.
    public static String getObjectName(HttpServerRequest request, WSRequestContext context) {
        String objectName = request.getParam(SwiftApi.OBJECT_PARAM);
        if (objectName == null) {
            throw new HttpException(V2ErrorCode.INTERNAL_SERVER_ERROR, "Internal server error", request.path());
        }
        context.setObjectName(objectName);
        return objectName;
    }

    @Nullable
    public static String getListPrefix(HttpServerRequest request) {
        String prefix = request.getParam(SwiftApi.PREFIX_PARAM);
        if (prefix == null) {
            return null;
        }

        return prefix;
    }

    private SwiftHttpPathHelpers() {
    }
}
