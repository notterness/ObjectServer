package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.common.metrics.MetricScope;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nullable;

/**
 * Interface for classes that authenticate HTTP requests.
 */
public interface Authenticator {

    /**
     * Authenticate an HTTP request with no body.
     *
     * @param context the vertx routing context.
     * @return an AuthenticationInfo or a {@link NotAuthenticatedException}.
     */
    AuthenticationInfo authenticate(RoutingContext context);

    /**
     * Authenticate an HTTP request with a body.
     *
     * @param context the vertx routing context.
     * @param bodySha256 a SHA-256 computed from the body, used for requests with a small body.
     * @return an AuthenticationInfo or a {@link NotAuthenticatedException}.
     */
    AuthenticationInfo authenticate(RoutingContext context, String bodySha256);

    /**
     * Authenticate a PUT object request (with a body, but no SHA-256).
     *
     * The PUT object request requires only the date header and the request target. The client can optionally specify
     * a Content-Type, Content-Length and X-Content-SHA256 header in the string to sign, and we will verify those
     * header values. However, we do not currently verify that the X-Content-SHA256 header value matches the computed
     * SHA-256 of the bytes of the request body (we intend to do this in the future). This differs from the
     * authenticate method, which requires values for those headers, and validates all of them.
     *
     * @param context the vertx routing context.
     * @return an AuthenticationInfo or a {@link NotAuthenticatedException}.
     */
    AuthenticationInfo authenticatePutObject(RoutingContext context);

    /**
     * Authenticate a PUT object request (with a body, but no SHA-256).
     *
     * The PUT object request requires only the date header and the request target. The client can optionally specify
     * a Content-Type, Content-Length and X-Content-SHA256 header in the string to sign, and we will verify those
     * header values. However, we do not currently verify that the X-Content-SHA256 header value matches the computed
     * SHA-256 of the bytes of the request body (we intend to do this in the future). This differs from the
     * authenticate method, which requires values for those headers, and validates all of them.
     *
     * @param headersOrig the headers
     * @return an AuthenticationInfo or a {@link NotAuthenticatedException}.
     */
    AuthenticationInfo authenticatePutObject(javax.ws.rs.core.HttpHeaders headersOrig, String uriString, String namespace, String method, MetricScope metricScope);

    /**
     * Authenticate a request made to the Swift API.
     *
     * This is a higher level authenticate function that will allow either Authentication of type `Basic`
     * or `Signature` (following the Cavage RFC as laid
     * out in https://tools.ietf.org/html/draft-cavage-http-signatures-10#section-3.1) which is similar to
     * {@link #authenticate(RoutingContext, String)}
     *
     * Do not call this method for PutObject or any handler that wants to skip signing the body.
     *
     * @param context the vertx routing context.
     * @param namespace  The namespace for the incoming request. Since all implemented Swift APIs require a namespace,
     *                   this parameter is not optional.
     * @param tenantOcid Optional. Will fall back to using the namespace if null.
     * @return an AuthenticationInfo or a {@link NotAuthenticatedException}.
     */
    AuthenticationInfo authenticateSwiftOrCavage(RoutingContext context,
                                                 @Nullable String bodySha256,
                                                 String namespace,
                                                 @Nullable String tenantOcid);

    /**
     * Similar to {@link #authenticateSwiftOrCavage(RoutingContext, String, String, String)}
     * however this is to be used for
     * API calls that upload a large body (i.e. PutObject) and want to omit including the SHA256 body signature
     * See {@link #authenticatePutObject}
     *
     * @param context the vertx routing context.
     * @param namespace  The namespace for the incoming request. Since all implemented Swift APIs require a namespace,
     *                   this parameter is not optional.
     * @param tenantOcid Optional. Will fall back to using the namespace if null.
     * @return an AuthenticationInfo or a {@link NotAuthenticatedException}.
     */
    AuthenticationInfo authenticateSwiftOrCavageForPutObject(RoutingContext context,
                                                             String namespace,
                                                             @Nullable String tenantOcid);


    /**
     * Authenticate an HTTP request.
     * The request can either be authenticated as a Cavage signature or using a JWT.
     *
     * @param context the vertx routing context.
     * @param bodySha256 the sha256 of the body which is needed if you'd like to authenticate via cavage
     * @return an AuthenticationInfo or a {@link NotAuthenticatedException}.
     */
    AuthenticationInfo authenticateCavageOrJWT(RoutingContext context, @Nullable String bodySha256);
}
