package com.oracle.pic.casper.webserver.api.s3;

import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.vertx.HttpServerUtil;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.NotFoundHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.NotImplementedException;

import java.util.Map;

/**
 * Vert.x HTTP handler for S3 Api to dispatch PUT (create) bucket request and sub-resource PUT requests like
 * uploads, tagging, acl, logging, metrics etc
 */
public class PutBucketDispatchHandler implements Handler<RoutingContext> {

    private final PutBucketHandler putBucketHandler;
    private final PutBucketTaggingHandler putBucketTaggingHandler;
    private final NotFoundHandler notFoundHandler;
    private final EmbargoV3 embargoV3;

    private Map<String, Handler<RoutingContext>> supportedPutConfiguration;

    public PutBucketDispatchHandler(PutBucketHandler putBucketHandler,
                                    PutBucketTaggingHandler putBucketTaggingHandler,
                                    NotFoundHandler notFoundHandler,
                                    EmbargoV3 embargoV3) {
        this.putBucketHandler = putBucketHandler;
        this.putBucketTaggingHandler = putBucketTaggingHandler;
        this.notFoundHandler = notFoundHandler;
        this.embargoV3 = embargoV3;

        supportedPutConfiguration = ImmutableMap.<String, Handler<RoutingContext>>builder()
                .put(S3Api.TAGGING_PARAM, this.putBucketTaggingHandler)
                .build();
    }

    @Override
    public void handle(RoutingContext routingContext) {
        WSRequestContext.setOperationName("s3", getClass(), routingContext, null);

        final HttpServerRequest request = routingContext.request();
        String firstParam = HttpServerUtil.getRoutingParam(request);
        S3HttpHelpers.getNamespace(request, WSRequestContext.get(routingContext));

        if (firstParam == null) {
            putBucketHandler.handle(routingContext);
        } else if (supportedPutConfiguration.containsKey(firstParam)) {
            supportedPutConfiguration.get(firstParam).handle(routingContext);
        } else if (S3Api.UNSUPPORTED_BUCKET_CONFIGURATION_PARAMS.contains(firstParam)) {
            // Even though this PutBucket variation is not supported we will check embargo.
            // This lets us test Embargo V3 support for all operations.
            //
            // Here we assume any not-implemented handlers will be UpdateBucket operations.
            final WSRequestContext wsRequestContext = WSRequestContext.get(routingContext);
            final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
            final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);

            final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.S3)
                .setOperation(CasperOperation.UPDATE_BUCKET)
                .setNamespace(namespace)
                .setBucket(bucketName)
                .build();
            embargoV3.enter(embargoOperation);
            throw new NotImplementedException(String.format(
                    "S3 Set Bucket %s configuration operation is not supported.", firstParam));
        } else {
            notFoundHandler.handle(routingContext);
        }

    }
}
