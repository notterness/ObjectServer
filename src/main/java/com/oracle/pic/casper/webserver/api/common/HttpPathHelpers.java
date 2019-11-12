package com.oracle.pic.casper.webserver.api.common;

import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.metrics.MetricScope;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public final class HttpPathHelpers {

    private HttpPathHelpers() {
    }

    public static String encodeUri(String uri) {
        try {
            return URLEncoder.encode(uri, StandardCharsets.UTF_8.name()).replace("+", "%20");
        } catch (UnsupportedEncodingException ueex) {
            throw new RuntimeException(ueex);
        }
    }

    /**
     * Annotate path parameters onto the root MetricScope for a request (request log annotations).
     *
     * Note:  This method logs path parameters from both v2 and Swift.  Swift uses the terms "account" and "container"
     * in place of "namespace" and "bucket", but we normalize them here.
     *
     * @param scope  Root MetricScope to annotate the parameters onto.
     */
    public static void logPathParameters(MetricScope scope, String namespaceName, @Nullable  String bucketName,
                                         @Nullable String objectName) {
        Preconditions.checkArgument(scope.isRoot());
        scope.annotate("namespaceName", namespaceName);
        if (bucketName != null) {
            scope.annotate("bucketName", bucketName);
        }
        if (objectName != null) {
            scope.annotate("objectName", objectName);
        }
    }

    /**
     * Logs parameters for multi-part request.
     */
    public static void logMultipartPathParameters(MetricScope scope, String namespaceName, @Nullable  String bucketName,
        @Nullable String objectName, @Nullable String uploadId, @Nullable String uploadPartNum) {
        logPathParameters(scope, namespaceName, bucketName, objectName);
        Preconditions.checkArgument(scope.isRoot());
        if (uploadId != null) {
            scope.annotate("uploadId", uploadId);
        }
        if (uploadPartNum != null) {
            scope.annotate("uploadPartNum", uploadPartNum);
        }
    }
}
