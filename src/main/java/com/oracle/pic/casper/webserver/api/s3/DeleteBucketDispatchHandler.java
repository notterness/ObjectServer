package com.oracle.pic.casper.webserver.api.s3;

import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.vertx.HttpServerUtil;
import com.oracle.pic.casper.webserver.api.common.NotFoundHandler;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.NotImplementedException;

import java.util.Map;

/**
 * Vert.x HTTP handler for S3 Api to dispatch DELETE bucket request and sub-resource DELETE requests like
 * uploads, tagging, acl, logging, metrics etc
 */
public class DeleteBucketDispatchHandler implements Handler<RoutingContext> {

    private final DeleteBucketHandler deleteBucketHandler;
    private final DeleteBucketTaggingHandler deleteBucketTaggingHandler;
    private final NotFoundHandler notFoundHandler;

    private Map<String, Handler<RoutingContext>> supportedDeleteConfiguration;

    public DeleteBucketDispatchHandler(DeleteBucketHandler deleteBucketHandler,
                                       DeleteBucketTaggingHandler deleteBucketTaggingHandler,
                                       NotFoundHandler notFoundHandler) {
        this.deleteBucketHandler = deleteBucketHandler;
        this.deleteBucketTaggingHandler = deleteBucketTaggingHandler;
        this.notFoundHandler = notFoundHandler;

        supportedDeleteConfiguration = ImmutableMap.<String, Handler<RoutingContext>>builder()
                .put(S3Api.TAGGING_PARAM, this.deleteBucketTaggingHandler)
                .build();
    }

    @Override
    public void handle(RoutingContext routingContext) {
        WSRequestContext.setOperationName("s3", getClass(), routingContext, null);

        final HttpServerRequest request = routingContext.request();
        String firstParam = HttpServerUtil.getRoutingParam(request);

        if (firstParam == null) {
            deleteBucketHandler.handle(routingContext);
        } else if (supportedDeleteConfiguration.containsKey(firstParam)) {
            supportedDeleteConfiguration.get(firstParam).handle(routingContext);
        } else if (S3Api.UNSUPPORTED_BUCKET_CONFIGURATION_PARAMS.contains(firstParam)) {
            throw new NotImplementedException(String.format(
                    "S3 Delete Bucket %s configuration operation is not supported.", firstParam));
        } else {
            notFoundHandler.handle(routingContext);
        }
    }
}
