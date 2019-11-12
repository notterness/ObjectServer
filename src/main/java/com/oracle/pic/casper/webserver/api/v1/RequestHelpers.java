package com.oracle.pic.casper.webserver.api.v1;

import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.common.model.PageableList;
import com.oracle.pic.casper.common.rest.CommonQueryParams;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.BucketKey;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.objectmeta.ObjectKey;
import com.oracle.pic.casper.webserver.api.common.NamespaceCaseWhiteList;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidBucketNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidObjectNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.TooLongObjectNameException;
import com.oracle.pic.casper.webserver.util.Validator;
import io.vertx.core.http.HttpServerRequest;


/**
 * Helpers that deal with HttpServerRequests.
 */
public final class RequestHelpers {

    /**
     * Private constructor (this is a static class).
     */
    private RequestHelpers() {
    }

    static NamespaceKey computeNamespaceKey(HttpServerRequest request) throws BadRequestException {
        try {
            String scope = request.getParam(CasperApiV1.SCOPE_PARAM);
            scope = NamespaceCaseWhiteList.lowercaseNamespace(Api.V1, scope);
            Validator.validateV1Namespace(scope);
            return new NamespaceKey(Api.V1, scope);
        } catch (IllegalArgumentException ex) {
            throw new BadRequestException(ex.getMessage());
        }
    }

    /**
     * Given an HTTP request extract the namespace+bucket and create a
     * bucket key.
     *
     * @param request The HTTP request.
     * @return A bucket key created with the parameters in the request.
     * @throws BadRequestException if the request contains invalid parameters
     */
    static BucketKey computeBucketKey(HttpServerRequest request) throws BadRequestException {
        try {
            String scope = request.getParam(CasperApiV1.SCOPE_PARAM);
            scope = NamespaceCaseWhiteList.lowercaseNamespace(Api.V1, scope);
            Validator.validateV1Namespace(scope);
            final String namespaceName = request.getParam(CasperApiV1.NS_PARAM);
            Validator.validateBucket(namespaceName);
            return new BucketKey(scope, namespaceName, Api.V1);
        } catch (IllegalArgumentException | InvalidBucketNameException ex) {
            throw new BadRequestException(ex.getMessage());
        }
    }

    /**
     * Given an HTTP request extract the scope+namespace+name and create
     * an object key.
     *
     * @param request The HTTP request.
     * @return A object key created with the parameters in the request.
     * @throws BadRequestException if the request contains invalid parameters
     */
    static ObjectKey computeObjectKey(HttpServerRequest request) throws BadRequestException {
        try {
            final BucketKey bucketKey = computeBucketKey(request);
            final String objectName = request.getParam(CasperApiV1.OBJECT_PARAM);
            Validator.validateObjectName(objectName);
            return new ObjectKey(bucketKey, objectName);
        } catch (InvalidObjectNameException | TooLongObjectNameException ex) {
            throw new BadRequestException(ex.getMessage());
        }
    }

    /**
     * Given an HTTP request get the page size parameter, or return a
     * default value.
     *
     * @param request The HTTP request.
     * @return The value of the 'limit' query parameter, or a default value.
     */
    static int getPageSize(HttpServerRequest request) {
        return request.getParam(CommonQueryParams.LIMIT_PARAM) == null ? PageableList.DEFAULT_PAGE_SIZE :
                Math.min(Integer.parseInt(request.getParam(CommonQueryParams.LIMIT_PARAM)), 1000);
    }

}
