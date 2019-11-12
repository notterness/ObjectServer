package com.oracle.pic.casper.webserver.api.metering;

import com.google.common.base.Throwables;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nullable;
import java.util.Optional;

public interface MeteringHelper {

    void logMeteredRequest(RoutingContext context);

    void logMeteredBandwidth(RoutingContext context);

    /***
     * Determines for a given {@link Throwable} if metering the current request in the log
     * should be skipped.
     * All requests other than those caused by {@link HttpResponseStatus#INTERNAL_SERVER_ERROR}
     * should be logged.
     * This method will go through the throwable causal chain.
     * @param throwable the throwable to inspected
     * @return true/false wether to skip metering
     */
    static boolean isBillableException(@Nullable Throwable throwable) {
        return Optional.ofNullable(throwable).map(thr -> Throwables.getCausalChain(thr).stream()
                .filter(t -> HttpException.class.isAssignableFrom(t.getClass()))
                .map(t -> (HttpException) t)
                .map(t -> t.getErrorCode().getStatusCode())
                .anyMatch(HttpResponseStatus::isClientError)).orElse(true);
    }

    /***
     * Determines if the request / {@link HttpMethod} should be billed for.
     * Everything other than {@link HttpMethod#DELETE} is billed.
     * @param request The request to check
     * @return true/false if this request shoudl be metered.
     */
    static boolean isBillableHttpMethod(HttpServerRequest request) {
        return request.method() != HttpMethod.DELETE;
    }

    /***
     * Determines if the operation / {@link CasperOperation} should be billed for.
     * This method must used in conjunction with {@link MeteringHelper#isBillableHttpMethod(HttpServerRequest)} to
     * determine if a request/operation is to be billed or not. This is due to the fact that the logic in
     * {@link MeteringHelper#isBillableHttpMethod(HttpServerRequest)} cannot be replaced by a check for
     * {@link com.oracle.pic.identity.authorization.permissions.ActionKind#DELETE}.
     * {@link com.oracle.pic.casper.webserver.api.v2.DeleteLifecyclePolicyHandler} is
     * invoked as a result of a DELETE HTTP call but it uses {@link CasperOperation#UPDATE_BUCKET} which isn't
     * marked as {@link com.oracle.pic.identity.authorization.permissions.ActionKind#DELETE}. Also,
     * {@link com.oracle.pic.casper.webserver.api.v1.AbstractRouteHandler} sets operation to null, which means that
     * this method will return true for all v1 operations (including those invoked via a HTTP DELETE).
     * This method returns false for {@link CasperOperation#GET_NAMESPACE} and {@link CasperOperation#LIST_BUCKETS}
     * operations.
     * @param operation The operation to check
     * @return true/false if this operation should be metered.
     */
    static boolean isBillableOperation(@Nullable CasperOperation operation) {
        return operation != CasperOperation.GET_NAMESPACE &&
                operation != CasperOperation.LIST_BUCKETS;
    }

}
