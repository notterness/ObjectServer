package com.oracle.pic.casper.webserver.api.v1;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.objectmeta.ObjectKey;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Date;

/**
 * Deletes an object from the metadata store and then from the storage servers
 */
public class DeleteObjectHandler extends AbstractRouteHandler {

    private final Backend backend;
    private final EmbargoV3 embargoV3;

    public DeleteObjectHandler(Backend backend, EmbargoV3 embargoV3) {
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void subclassHandle(RoutingContext context) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        MetricsHandler.addMetrics(context, WebServerMetrics.V1_DELETE_OBJECT_BUNDLE);

        final ObjectKey objectKey = RequestHelpers.computeObjectKey(request);
        final String namespace = objectKey.getBucket().getNamespace();
        final String bucketName = objectKey.getBucket().getName();
        final String objectName = objectKey.getName();

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V1)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .setOperation(CasperOperation.DELETE_OBJECT)
            .build();
        embargoV3.enter(embargoV3Operation);

        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.NO);
        final String ifMatch = HttpMatchHelpers.getIfMatchHeader(request);

        VertxUtil.runAsync(() -> {
            try {
                final Date deleteDate = backend.deleteV1Object(context, namespace, bucketName, objectName, ifMatch)
                        .orElseThrow(() -> new HttpException(V2ErrorCode.OBJECT_NOT_FOUND,
                            "The object '" + objectName + "' does not exist in bucket '" + bucketName +
                                "' with namespace '" + namespace + "'", request.path()));

                response.putHeader(HttpHeaders.LAST_MODIFIED, DateUtil.httpFormattedDate(deleteDate))
                        .setStatusCode(HttpResponseStatus.NO_CONTENT)
                        .end();
            } catch (Exception ex) {
                throw HttpException.rewrite(request, ex);
            }
        }).exceptionally(throwable -> DeleteObjectHandler.fail(context, throwable));
    }

}
