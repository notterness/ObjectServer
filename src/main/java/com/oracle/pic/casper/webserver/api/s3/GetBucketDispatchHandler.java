package com.oracle.pic.casper.webserver.api.s3;

import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.exceptions.BadRequestException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Vert.x HTTP handler for S3 Api to dispatch sub-resource GET requests like
 * uploads, tagging, acl, logging, metrics etc
 */
public class GetBucketDispatchHandler implements Handler<RoutingContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetBucketDispatchHandler.class);
    private final ListObjectsHandler listObjectsHandler;
    private final ListUploadsHandler listUploadsHandler;
    private final GetBucketTaggingHandler getBucketTaggingHandler;
    private final GetBucketLocationHandler getBucketLocationHandler;
    private final GetBucketObjectVersionsHandler getBucketObjectVersionsHandler;
    private final GetBucketAccelerateHandler getBucketAccelerateConfigurationHandler;
    private final GetBucketCORSHandler getBucketCORSHandler;
    private final GetBucketLifecycleHandler getBucketLifecycleHandler;
    private final GetBucketLoggingStatusHandler getBucketLoggingStatusHandler;
    private final GetBucketNotificationHandler getBucketNotificationHandler;
    private final GetBucketPolicyHandler getBucketPolicyHandler;
    private final GetBucketReplicationHandler getBucketReplicationHandler;
    private final GetBucketRequestPaymentHandler getBucketRequestPaymentHandler;
    private final GetBucketWebsiteHandler getBucketWebsiteHandler;

    private final NotFoundHandler notFoundHandler;

    private final EmbargoV3 embargoV3;

    private Map<String, Handler<RoutingContext>> supportedGetConfiguration;

    public GetBucketDispatchHandler(ListObjectsHandler listObjectsHandler,
                                    ListUploadsHandler listUploadsHandler,
                                    GetBucketTaggingHandler getBucketTaggingHandler,
                                    GetBucketLocationHandler getBucketLocationHandler,
                                    GetBucketObjectVersionsHandler getBucketObjectVersionsHandler,
                                    GetBucketAccelerateHandler getBucketAccelerateConfigurationHandler,
                                    GetBucketCORSHandler getBucketCORSHandler,
                                    GetBucketLifecycleHandler getBucketLifecycleHandler,
                                    GetBucketLoggingStatusHandler getBucketLoggingStatusHandler,
                                    GetBucketNotificationHandler getBucketNotificationHandler,
                                    GetBucketPolicyHandler getBucketPolicyHandler,
                                    GetBucketReplicationHandler getBucketReplicationHandler,
                                    GetBucketRequestPaymentHandler getBucketRequestPaymentHandler,
                                    GetBucketWebsiteHandler getBucketWebsiteHandler,
                                    NotFoundHandler notFoundHandler,
                                    EmbargoV3 embargoV3) {
        this.listObjectsHandler = listObjectsHandler;
        this.listUploadsHandler = listUploadsHandler;
        this.getBucketTaggingHandler = getBucketTaggingHandler;
        this.getBucketLocationHandler = getBucketLocationHandler;
        this.getBucketObjectVersionsHandler = getBucketObjectVersionsHandler;
        this.getBucketAccelerateConfigurationHandler = getBucketAccelerateConfigurationHandler;
        this.getBucketCORSHandler = getBucketCORSHandler;
        this.getBucketLifecycleHandler = getBucketLifecycleHandler;
        this.getBucketLoggingStatusHandler = getBucketLoggingStatusHandler;
        this.getBucketNotificationHandler = getBucketNotificationHandler;
        this.getBucketPolicyHandler = getBucketPolicyHandler;
        this.getBucketReplicationHandler = getBucketReplicationHandler;
        this.getBucketRequestPaymentHandler = getBucketRequestPaymentHandler;
        this.getBucketWebsiteHandler = getBucketWebsiteHandler;
        this.notFoundHandler = notFoundHandler;
        this.embargoV3 = embargoV3;

        supportedGetConfiguration = ImmutableMap.<String, Handler<RoutingContext>>builder()
                .put(S3Api.UPLOADS_PARAM, this.listUploadsHandler)
                .put(S3Api.TAGGING_PARAM, this.getBucketTaggingHandler)
                .put(S3Api.LOCATION_PARAM, this.getBucketLocationHandler)
                .put(S3Api.VERSIONS_PARAM, this.getBucketObjectVersionsHandler)
                .put(S3Api.ACCELERATE_PARAM, this.getBucketAccelerateConfigurationHandler)
                .put(S3Api.CORS_PARAM, this.getBucketCORSHandler)
                .put(S3Api.LIFECYCLE_PARAM, this.getBucketLifecycleHandler)
                .put(S3Api.LOGGING_PARAM, this.getBucketLoggingStatusHandler)
                .put(S3Api.NOTIFICATION_PARAM, this.getBucketNotificationHandler)
                .put(S3Api.POLICY_PARAM, this.getBucketPolicyHandler)
                .put(S3Api.REPLICATION_PARAM, this.getBucketReplicationHandler)
                .put(S3Api.REQUESTPAYMENT_PARAM, this.getBucketRequestPaymentHandler)
                .put(S3Api.WEBSITE_PARAM, this.getBucketWebsiteHandler)
                .build();
    }

    @Override
    public void handle(RoutingContext context) {
        WSRequestContext.setOperationName("s3", getClass(), context, null);
        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String routingParam = getRoutingParameterFromActionParameters(
                HttpServerUtil.getActionParameters(request));
        S3HttpHelpers.getNamespace(request, wsRequestContext);

        // No parameters were specified, so we list
        if (routingParam == null) {
            listObjectsHandler.handle(context);
        } else if (supportedGetConfiguration.containsKey(routingParam)) {
            supportedGetConfiguration.get(routingParam).handle(context);
        } else if (S3Api.UNSUPPORTED_GET_BUCKET_CONFIGURATION_PARAMS.contains(routingParam)) {
            // Even though this GetBucket variation is not supported we will check embargo.
            // This lets us test Embargo V3 support for all operations.
            //
            // We do not do this for all sub-handlers because some of them (e.g. ListUploads)
            // use different operations in their call to Embargo. Here we assume any
            // not-implemented handlers will be GetBucket operations.
            final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
            final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);

            final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.S3)
                .setOperation(CasperOperation.GET_BUCKET)
                .setNamespace(namespace)
                .setBucket(bucketName)
                .build();
            embargoV3.enter(embargoOperation);

            throw new NotImplementedException(String.format(
                    "S3 Get Bucket %s configuration operation is not supported.", routingParam));
        } else {
            notFoundHandler.handle(context);
        }
    }

    /**
     * Get the query parameter used for routing the request to the appropriate handler
     *
     * The S3 api accepts un-valued query parameters to signify sub-resources and/or configuration
     * sets. This method handles both verifying the correctness of the incoming parameters, as well as retrieving the
     * appropriate un-valued parameter (if there are more than one valid un-valued params) to use for routing the
     * request to the correct handler.
     * @param parameters
     * @return The parameter to use for routing to the appropriate handler
     */
    private String getRoutingParameterFromActionParameters(List<String> parameters) {
        if (parameters.size() == 0) { // No parameters
            return null;
        }

        // this is the list of all parameters that are recognized by Casper (both supported and unsupported)
        final List<String> allValidParameters = new ArrayList<>(supportedGetConfiguration.keySet());
        allValidParameters.addAll(S3Api.UNSUPPORTED_BUCKET_CONFIGURATION_PARAMS);

        // find the valid parameters in the request
        List<String> validParameters = parameters.stream().filter(p -> allValidParameters.contains(p))
                .collect(Collectors.toList());
        if (validParameters.size() == 0) { // No valid parameter found
            return null;
        } else if (validParameters.size() > 1) { // More than one valid parameter in a GET bucket request => bad request
            throw new BadRequestException("More than one parameter in the request is not allowed.");
        }

        // single valid parameter found, return it
        return validParameters.get(0);
    }

}
