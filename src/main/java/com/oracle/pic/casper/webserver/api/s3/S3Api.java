package com.oracle.pic.casper.webserver.api.s3;

import com.google.common.collect.ImmutableSet;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.metrics.Annotation;
import com.oracle.pic.casper.common.metrics.Annotations;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.common.vertx.handler.HealthCheckHandler;
import com.oracle.pic.casper.webserver.api.Api;
import com.oracle.pic.casper.webserver.api.auditing.AuditFilterHandler;
import com.oracle.pic.casper.webserver.api.common.CasperLoggerHandler;
import com.oracle.pic.casper.webserver.api.common.CommonHandler;
import com.oracle.pic.casper.webserver.api.common.FailureHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.MeteringHandler;
import com.oracle.pic.casper.webserver.api.common.MetricAnnotationHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.URIValidationHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHandler;
import com.oracle.pic.casper.webserver.api.monitoring.MonitoringHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoHandler;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.Date;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is the entry point to <a href="https://confluence.oci.oraclecorp.com/display/CASPER/S3+API">CASPER's S3 API</a>
 * implementation.  This is a partial implementation of Amazon's
 * <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html"> Simple Storage Service</a>.
 */
public class S3Api implements Api {
    // adding : (illegal in bucket name) so that customers can't hit this url
    private static final String HEALTHCHECK_ROUTE = "/health[:]check";
    private static final String S3_HEALTHCHECK_ROUTE = "/s3[:]healthcheck";
    private static final String NAMESPACE_ROUTE = "/";
    private static final String BUCKET_ROUTE = "/([^/]+)/?";
    private static final String OBJECT_ROUTE = "/([^/]+)/(.+)";

    static final String BUCKET_PARAM = "param0";
    static final String OBJECT_PARAM = "param1";
    static final String MAX_KEY_PARAM = "max-keys";
    static final String KEY_MARKER_PARAM = "key-marker";
    static final String MARKER_PARAM = "marker";
    static final String PREFIX_PARAM = "prefix";
    static final String DELIMITER_PARAM = "delimiter";
    static final String START_AFTER_PARAM = "start-after";
    static final String CONTINUATION_TOKEN_PARAM = "continuation-token";
    static final String FETCH_OWNER = "fetch-owner";
    static final String MAX_UPLOADS_PARAM = "max-uploads";
    static final String UPLOADID_MARKER_PARAM = "upload-id-marker";
    static final String UPLOADID_PARAM = "uploadId";
    static final String UPLOADS_PARAM = "uploads";
    static final String PARTNUM_PARAM = "partNumber";
    static final String ENCODING_PARAM = "encoding-type";
    static final String PART_MARKER_PARAM = "part-number-marker";
    static final String MAX_PARTS_PARAM = "max-parts";
    static final String LIST_TYPE_PARAM = "list-type";
    static final String TAGGING_PARAM = "tagging";
    static final String DELETE_PARAM = "delete";
    static final String CONTENT_SHA256 = "x-amz-content-sha256";
    static final String BUCKET_REGION = "x-amz-bucket-region";
    static final String LOCATION_PARAM = "location";
    static final String RESTORE_PARAM = "restore";
    static final String VERSIONS_PARAM = "versions";
    static final String ACCELERATE_PARAM = "accelerate";
    static final String CORS_PARAM = "cors";
    static final String LIFECYCLE_PARAM = "lifecycle";
    static final String LOGGING_PARAM = "logging";
    static final String NOTIFICATION_PARAM = "notification";
    static final String POLICY_PARAM = "policy";
    static final String REPLICATION_PARAM = "replication";
    static final String REQUESTPAYMENT_PARAM = "requestPayment";
    static final String WEBSITE_PARAM = "website";
    static final String VERSION_ID_MARKER_PARAM = "version-id-marker";
    static final String ACL_PARAM = "acl";
    static final String ANALYTICS_PARAM = "analytics";
    static final String INVENTORY_PARAM = "inventory";
    static final String METRICS_PARAM = "metrics";
    static final String TORRENT_PARAM = "torrent";
    static final String VERSIONING_PARAM = "versioning";
    static final String ENCRYPTION = "encryption";

    static final String STREAMING_SHA256_PAYLOAD = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
    static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
    static final String SHA256_EMPTY = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    static final String OPC_COMPARTMENT_ID_HEADER = "opc-compartment-id";
    static final String REQUEST_ID_HEADER = "x-amz-request-id";
    static final String STORAGE_CLASS_HEADER = "x-amz-storage-class";
    static final String RESTORE_HEADER = "x-amz-restore";
    static final String COPY_SOURCE_HEADER = "x-amz-copy-source";
    static final String COPY_SOURCE_RANGE_HEADER = "x-amz-copy-source-range";
    static final String COPY_SOURCE_IF_MATCH = "x-amz-copy-source-if-match";
    static final String COPY_SOURCE_IF_NONE_MATCH = "x-amz-copy-source-if-none-match";
    static final String COPY_SOURCE_IF_MODIFIED_SINCE = "x-amz-copy-source-if-modified-since";
    static final String COPY_SOURCE_IF_UNMODIFIED_SINCE = "x-amz-copy-source-if-unmodified-since";
    static final String METADATA_DIRECTIVE_HEADER = "x-amz-metadata-directive";

    static final Set<String> UNSUPPORTED_CONFIGURATION_PARAMS = ImmutableSet.of(
            ACCELERATE_PARAM, ACL_PARAM, ANALYTICS_PARAM, CORS_PARAM, INVENTORY_PARAM, LIFECYCLE_PARAM,  LOGGING_PARAM,
            METRICS_PARAM, NOTIFICATION_PARAM, POLICY_PARAM, REPLICATION_PARAM, REQUESTPAYMENT_PARAM, TAGGING_PARAM,
            VERSIONING_PARAM, WEBSITE_PARAM, TORRENT_PARAM, ENCRYPTION);

    static final Set<String> UNSUPPORTED_BUCKET_CONFIGURATION_PARAMS = UNSUPPORTED_CONFIGURATION_PARAMS
            .stream()
            .filter(s -> !s.equals(TAGGING_PARAM))
            .collect(Collectors.toSet());

    static final Set<String> SUPPORTED_GET_BUCKET_CONFIGURATION_PARAMS = ImmutableSet.of(
            UPLOADS_PARAM, TAGGING_PARAM, LOCATION_PARAM, VERSIONS_PARAM, ACCELERATE_PARAM, CORS_PARAM, LIFECYCLE_PARAM,
            LOGGING_PARAM, NOTIFICATION_PARAM, POLICY_PARAM, REPLICATION_PARAM, REQUESTPAYMENT_PARAM, WEBSITE_PARAM);

    static final Set<String> UNSUPPORTED_GET_BUCKET_CONFIGURATION_PARAMS = UNSUPPORTED_CONFIGURATION_PARAMS
            .stream()
            .filter(s -> !SUPPORTED_GET_BUCKET_CONFIGURATION_PARAMS.contains(s))
            .collect(Collectors.toSet());

    // common handlers
    private final CommonHandler commonHandler;
    private final MetricsHandler metricsHandler;
    private final MetricAnnotationHandlerFactory metricAnnotationHandlerFactory;
    private final MeteringHandler meteringHandler;
    private final S3ExceptionTranslator s3ExceptionTranslator;
    private final FailureHandlerFactory failureHandlerFactory;
    private final CasperLoggerHandler loggerHandler;
    private final EmbargoHandler embargoHandler;
    private final AuditFilterHandler auditFilterHandler;
    //todo: remove once fully migrated
    private final HealthCheckHandler healthCheckHandler;
    private final HealthCheckHandler s3HealthCheckHandler;
    private final URIValidationHandler uriValidationHandler;
    // request handlers
    private final ListBucketsHandler listBucketsHandler;
    private final HeadBucketHandler headBucketHandler;
    private final PostBucketHandler postBucketHandler;
    private final S3ObjectPathHandler s3ObjectPathHandler;
    private final PutBucketDispatchHandler putBucketDispatchHandler;
    private final GetBucketDispatchHandler getBucketDispatchHandler;
    private final DeleteBucketDispatchHandler deleteBucketDispatchHandler;
    private final MonitoringHandler monitoringHandler;
    private final ServiceLogsHandler serviceLogsHandler;

    public S3Api(CommonHandler commonHandler,
                 MetricsHandler metricsHandler,
                 MetricAnnotationHandlerFactory metricAnnotationHandlerFactory,
                 MeteringHandler meteringHandler,
                 S3ExceptionTranslator s3ExceptionTranslator,
                 FailureHandlerFactory failureHandlerFactory,
                 CasperLoggerHandler loggerHandler,
                 EmbargoHandler embargoHandler,
                 AuditFilterHandler auditFilterHandler,
                 HealthCheckHandler healthCheckHandler,
                 HealthCheckHandler s3HealthCheckHandler,
                 URIValidationHandler uriValidationHandler,
                 ListBucketsHandler listBucketsHandler,
                 HeadBucketHandler headBucketHandler,
                 PostBucketHandler postBucketHandler,
                 S3ObjectPathHandler s3ObjectPathHandler,
                 PutBucketDispatchHandler putBucketDispatchHandler,
                 GetBucketDispatchHandler getBucketDispatchHandler,
                 DeleteBucketDispatchHandler deleteBucketDispatchHandler,
                 MonitoringHandler monitoringHandler,
                 ServiceLogsHandler serviceLogsHandler) {
        // common handlers
        this.commonHandler = commonHandler;
        this.metricsHandler = metricsHandler;
        this.metricAnnotationHandlerFactory = metricAnnotationHandlerFactory;
        this.meteringHandler = meteringHandler;
        this.s3ExceptionTranslator = s3ExceptionTranslator;
        this.failureHandlerFactory = failureHandlerFactory;
        this.loggerHandler = loggerHandler;
        this.embargoHandler = embargoHandler;
        this.auditFilterHandler = auditFilterHandler;
        this.healthCheckHandler = healthCheckHandler;
        this.s3HealthCheckHandler = s3HealthCheckHandler;
        this.uriValidationHandler = uriValidationHandler;
        // request handlers
        this.listBucketsHandler = listBucketsHandler;
        this.headBucketHandler = headBucketHandler;
        this.postBucketHandler = postBucketHandler;
        this.s3ObjectPathHandler = s3ObjectPathHandler;
        this.putBucketDispatchHandler = putBucketDispatchHandler;
        this.getBucketDispatchHandler = getBucketDispatchHandler;
        this.deleteBucketDispatchHandler = deleteBucketDispatchHandler;
        this.monitoringHandler = monitoringHandler;
        this.serviceLogsHandler = serviceLogsHandler;
    }

    @Override
    public Router createRouter(Vertx vertx) {
        Router router = Router.router(vertx);

        router.get(HEALTHCHECK_ROUTE).useNormalisedPath(false).handler(healthCheckHandler);
        router.get(S3_HEALTHCHECK_ROUTE).useNormalisedPath(false).handler(s3HealthCheckHandler);

        router.route().useNormalisedPath(false).handler(commonHandler);
        router.route().useNormalisedPath(false).handler(loggerHandler);
        router.route().useNormalisedPath(false).handler(meteringHandler);
        router.route().handler(S3_RESPONSE_HEADERS);
        router.route().useNormalisedPath(false).failureHandler(failureHandlerFactory.create(s3ExceptionTranslator));

        router.route().useNormalisedPath(false).handler(metricsHandler);
        Annotation application = Annotations.application(S3Api.class);
        router.route().useNormalisedPath(false).handler(metricAnnotationHandlerFactory.create(application));

        //refuse requests that match embargo and rate-limit rules
        router.route().useNormalisedPath(false).handler(embargoHandler);
        router.route().useNormalisedPath(false).handler(auditFilterHandler);

        // validate URI before Vert.x parses it and matches against regex
        router.route().useNormalisedPath(false).handler(uriValidationHandler);

        //Monitoring - public metrics
        router.route().useNormalisedPath(false).handler(monitoringHandler);

        // Service logs
        router.route().useNormalisedPath(false).handler(serviceLogsHandler);

        router.get().pathRegex(NAMESPACE_ROUTE).useNormalisedPath(false).handler(listBucketsHandler);

        router.putWithRegex(BUCKET_ROUTE).useNormalisedPath(false).handler(putBucketDispatchHandler);
        router.headWithRegex(BUCKET_ROUTE).useNormalisedPath(false).handler(headBucketHandler);
        router.getWithRegex(BUCKET_ROUTE).useNormalisedPath(false).handler(getBucketDispatchHandler);
        router.deleteWithRegex(BUCKET_ROUTE).useNormalisedPath(false).handler(deleteBucketDispatchHandler);
        router.postWithRegex(BUCKET_ROUTE).useNormalisedPath(false).handler(postBucketHandler);

        router.putWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(s3ObjectPathHandler);
        router.headWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(s3ObjectPathHandler);
        router.getWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(s3ObjectPathHandler);
        router.deleteWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(s3ObjectPathHandler);
        router.postWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(s3ObjectPathHandler);

        router.route().handler(UNMATCHED_ROUTE_HANDLER);

        return router;
    }

    private static final Handler<RoutingContext> S3_RESPONSE_HEADERS = context -> {
        CommonRequestContext ws = WSRequestContext.getCommonRequestContext(context);
        context.response().putHeader(REQUEST_ID_HEADER, ws.getOpcRequestId());
        context.addHeadersEndHandler(event ->
            context.response().putHeader(HttpHeaders.DATE, DateUtil.httpFormattedDate(new Date())));
        context.next();
    };

    /**
     * S3 does not have a generic 404, so requests that don't have a configured handler yield InvalidURI.
     */
    private static final Handler<RoutingContext> UNMATCHED_ROUTE_HANDLER = context -> {
        throw new S3HttpException(S3ErrorCode.INVALID_URI, "Couldn't parse the specified URI.",
            context.request().path());
    };
}
