package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.metrics.Annotation;
import com.oracle.pic.casper.common.metrics.Annotations;
import com.oracle.pic.casper.common.vertx.handler.HealthCheckHandler;
import com.oracle.pic.casper.webserver.api.Api;
import com.oracle.pic.casper.webserver.api.auditing.AuditFilterHandler;
import com.oracle.pic.casper.webserver.api.common.CasperLoggerHandler;
import com.oracle.pic.casper.webserver.api.common.CommonHandler;
import com.oracle.pic.casper.webserver.api.common.FailureHandler;
import com.oracle.pic.casper.webserver.api.common.FailureHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.HeaderCorsHandler;
import com.oracle.pic.casper.webserver.api.common.MeteringHandler;
import com.oracle.pic.casper.webserver.api.common.MetricAnnotationHandler;
import com.oracle.pic.casper.webserver.api.common.MetricAnnotationHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.NotFoundHandler;
import com.oracle.pic.casper.webserver.api.common.OptionCorsHandler;
import com.oracle.pic.casper.webserver.api.common.URIValidationHandler;
import com.oracle.pic.casper.webserver.api.logging.DeleteLoggingHandler;
import com.oracle.pic.casper.webserver.api.logging.PostLoggingHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHandler;
import com.oracle.pic.casper.webserver.api.monitoring.MonitoringHandler;
import com.oracle.pic.casper.webserver.api.pars.CreateParHandler;
import com.oracle.pic.casper.webserver.api.pars.DeleteParHandler;
import com.oracle.pic.casper.webserver.api.pars.GetParHandler;
import com.oracle.pic.casper.webserver.api.pars.ListParsHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoHandler;
import com.oracle.pic.casper.webserver.api.replication.DeleteReplicationPolicyHandler;
import com.oracle.pic.casper.webserver.api.replication.GetReplicationPolicyHandler;
import com.oracle.pic.casper.webserver.api.replication.ListReplicationPoliciesHandler;
import com.oracle.pic.casper.webserver.api.replication.ListReplicationSourcesHandler;
import com.oracle.pic.casper.webserver.api.replication.PostReplicationOptionsHandler;
import com.oracle.pic.casper.webserver.api.replication.PostReplicationPolicyHandler;
import com.oracle.pic.casper.webserver.api.replication.PostBucketWritableHandler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;

public class CasperApiV2 implements Api {
    public static final String NAMESPACE_COLLECTION_ROUTE = "/n/?";
    public static final String NAMESPACE_ROUTE = "/n/([^/]+)/?";
    public static final String BUCKET_COLLECTION_ROUTE = "/n/([^/]+)/b/?";
    public static final String BUCKET_ROUTE = "/n/([^/]+)/b/([^/]+)/?";

    /**
     * Provide a way for a caller to get information about the status of the
     * server. This route must be registered BEFORE the object route (the
     * Vert.x router uses the first matching route).
     */
    public static final String SERVER_INFO_ROUTE = "/n/bmcostests/b/dev/o/info";


    /**
     * Provide a way for  a caller to get information about how the server views them.
     * Like {@link CasperApiV2#SERVER_INFO_ROUTE}, this must be registered before the object route.
     */
    public static final String CLIENT_INFO_ROUTE = "/n/bmcostests/b/dev/o/client";

    public static final String OBJECT_ROUTE = "/n/([^/]+)/b/([^/]+)/o/(.+)";
    public static final String OBJECT_COLLECTION_ROUTE = "/n/([^/]+)/b/([^/]+)/o/?";
    public static final String RESTORE_OBJECT_ROUTE = "/n/([^/]+)/b/([^/]+)/actions/restoreObjects";
    static final String UPLOAD_COLLECTION_ROUTE = "/n/([^/]+)/b/([^/]+)/u/?";
    static final String UPLOAD_ROUTE = "/n/([^/]+)/b/([^/]+)/u/(.+)?";
    public static final String PAR_COLLECTION_ROUTE = "/n/([^/]+)/b/([^/]+)/p/?";
    public static final String PAR_ROUTE = "/n/([^/]+)/b/([^/]+)/p/(.+)";
    public static final String RENAME_ROUTE = "/n/([^/]+)/b/([^/]+)/actions/renameObject";
    public static final String BUCKET_METER_SETTINGS_ROUTE = "/n/([^/]+)/b/([^/]+)/actions/bucketMeterSettings";
    public static final String BUCKET_OPTIONS_ROUTE = "/n/([^/]+)/b/([^/]+)/actions/options";
    static final String HEALTHCHECK_ROUTE = "/healthcheck";
    static final String V2_HEALTHCHECK_ROUTE = "/v2[:]healthcheck";

    public static final String PAR_OBJECT_ROUTE = "/p/([^/]+)/n/([^/]+)/b/([^/]+)/o/(.+)";
    static final String PAR_UPLOAD_PART_ROUTE = "/p/([^/]+)/n/([^/]+)/b/([^/]+)/u/(.+)/id/([a-f0-9-]+)/([0-9]+)";
    static final String PAR_MULTIPART_ROUTE = "/p/([^/]+)/n/([^/]+)/b/([^/]+)/u/(.+)/id/([a-f0-9-]+)/?";

    static final String LIFECYCLE_POLICY_ROUTE = "/n/([^/]+)/b/([^/]+)/l";

    static final String WORK_REQUEST_ROUTE = "/workRequests/([^/]+)/?";
    static final String WORK_REQUEST_LOG_ROUTE = "/workRequests/([^/]+)/logs/?";
    static final String WORK_REQUEST_ERROR_ROUTE = "/workRequests/([^/]+)/errors/?";
    static final String WORK_REQUEST_COLLECTION_ROUTE = "/workRequests/?";
    static final String COPY_PART_ROUTE = "/n/([^/]+)/b/([^/]+)/cp/([^/]+)";
    static final String COPY_OBJECT_ROUTE = "/n/([^/]+)/b/([^/]+)/actions/copyObject";
    static final String BULK_RESTORE_ROUTE = "/n/([^/]+)/b/([^/]+)/actions/bulkRestore";
    public static final String OBJECT_METADATA_REPLACE_ROUTE =
            "/n/([^/]+)/b/([^/]+)/actions/replaceObjectMetadata/(.+)";
    public static final String OBJECT_METADATA_MERGE_ROUTE = "/n/([^/]+)/b/([^/]+)/actions/mergeObjectMetadata/(.+)";

    public static final String REENCRYPT_OBJECT_ROUTE = "/n/([^/]+)/b/([^/]+)/actions/reencrypt/(.+)";
    public static final String REENCRYPT_BUCKET_ROUTE = "/n/([^/]+)/b/([^/]+)/actions/reencrypt";

    private static final String REPLICATION_COLLECTION_ROUTE = "/n/([^/]+)/b/([^/]+)/replicationPolicies/?";
    private static final String REPLICATION_ROUTE = "/n/([^/]+)/b/([^/]+)/replicationPolicies/([a-zA-Z0-9-]+)";
    private static final String REPLICATION_SOURCES_ROUTE = "/n/([^/]+)/b/([^/]+)/replicationSources/?";
    public static final String REPLICATION_OPTIONS_ROUTE = "/n/([^/]+)/b/([^/]+)/actions/replicationOptions";
    public static final String REPLICATION_MAKE_BUCKET_WRITABLE_ROUTE =
            "/n/([^/]+)/b/([^/]+)/actions/makeBucketWritable";

    public static final String PURGE_DELETED_TAGS_BUCKET_ROUTE = "/n/([^/]+)/b/([^/]+)/actions/purgeDeletedTags";

    private static final String POST_LOGGING_ROUTE = "/20190909/logging";
    private static final String DELETE_LOGGING_ROUTE = "/20190909/logging/([^/]+)";

    private static final String GET_USAGE_ROUTE = "/20190901/internalUsage/([^/]+)";

    public static final String NS_PARAM = "param0";
    public static final String BUCKET_PARAM = "param1";
    public static final String OBJECT_PARAM = "param2";

    public static final String PARAM_0 = "param0";
    public static final String PARAM_1 = "param1";
    public static final String PARAM_2 = "param2";
    public static final String PARAM_3 = "param3";
    public static final String PARAM_4 = "param4";
    public static final String PARAM_5 = "param5";

    static final String START_WITH_PARAM = "start";
    static final String END_BEFORE_PARAM = "end";
    static final String PREFIX_PARAM = "prefix";
    static final String OBJECT_NAME_PREFIX_PARAM = "objectNamePrefix";
    static final String PAR_URL_PARAM = "parUrl";
    static final String DELIMITER_PARAM = "delimiter";
    static final String FIELDS_PARAM = "fields";
    static final String UPLOAD_PARAM = "uploadId";
    static final String UPLOAD_PART_NUMBER_PARAM = "uploadPartNum";
    static final String SORT_BY_PARAM = "sortBy";
    static final String SORT_ORDER_PARAM = "sortOrder";
    static final String METER_SETTING_PARAM = "meterSetting";
    static final String MULTIPART_PAR_HEADER = "opc-multipart";
    /**
     * The following 3 headers are secret headers that should only be used internally by cross-region replication
     * workers.
     */
    static final String ETAG_OVERRIDE_HEADER = "etag-override";
    static final String MD5_OVERRIDE_HEADER = "md5-override";
    static final String PART_COUNT_OVERRIDE_HEADER = "partcount-override";
    static final String POLICY_ROUND_HEADER = "policy-round";

    private final GetNamespaceCollectionHandler getNamespaceCollectionHandler;
    private final GetNamespaceMetadataHandler getNamespaceMetadataHandler;
    private final PutNamespaceMetadataHandler putNamespaceMetadataHandler;
    private final ListBucketsHandler listBucketsHandler;
    private final PostBucketCollectionHandler postBucketCollectionHandler;
    private final PostBucketHandler postBucketHandler;
    private final GetBucketHandler getBucketHandler;
    private final DeleteBucketHandler deleteBucketHandler;
    private final HeadBucketHandler headBucketHandler;
    private final PutObjectHandler putObjectHandler;
    private final RenameObjectHandler renameObjectHandler;
    private final GetObjectHandler getObjectHandler;
    private final ParPutObjectRoutingHandler parPutObjectRoutingHandler;
    private final ParHeadObjectHandler parHeadObjectHandler;
    private final ParGetObjectHandler parGetObjectHandler;
    private final ParPutPartHandler parPutPartHandler;
    private final ParCommitUploadHandler parCommitUploadHandler;
    private final ParAbortUploadHandler parAbortUploadHandler;
    private final HeadObjectHandler headObjectHandler;
    private final DeleteObjectHandler deleteObjectHandler;
    private final ListObjectsHandler listObjectHandler;
    private final CreateUploadHandler createUploadHandler;
    private final ListUploadsHandler listUploadsHandler;
    private final CommitUploadHandler commitUploadHandler;
    private final AbortUploadHandler abortUploadHandler;
    private final ListUploadPartsHandler listUploadPartsHandler;
    private final CreateParHandler createParHandler;
    private final GetParHandler getParHandler;
    private final DeleteParHandler deleteParHandler;
    private final ListParsHandler listParsHandler;
    private final GetWorkRequestHandler getWorkRequestHandler;
    private final ListWorkRequestLogHandler listWorkRequestLogHandler;
    private final ListWorkRequestErrorHandler listWorkRequestErrorHandler;
    private final CopyObjectHandler copyObjectHandler;
    private final BulkRestoreHandler bulkRestoreHandler;
    private final ListWorkRequestsHandler listWorkRequestsHandler;
    private final CancelWorkRequestHandler cancelWorkRequestHandler;
    private final PutLifecyclePolicyHandler putLifecyclePolicyHandler;
    private final GetLifecyclePolicyHandler getLifecyclePolicyHandler;
    private final DeleteLifecyclePolicyHandler deleteLifecyclePolicyHandler;
    private final PutPartHandler putPartHandler;
    private final RestoreObjectsHandler restoreObjectsHandler;
    private final PostObjectMetadataReplaceHandler postObjectMetadataReplaceHandler;
    private final PostObjectMetadataMergeHandler postObjectMetadataMergeHandler;
    private final PostObjectReencryptHandler postObjectReencryptHandler;
    private final PostBucketReencryptHandler postbucketReencryptHandler;
    private final CopyPartHandler copyPartHandler;
    private final PostReplicationPolicyHandler postReplicationPolicyHandler;
    private final GetReplicationPolicyHandler getReplicationPolicyHandler;
    private final DeleteReplicationPolicyHandler deleteReplicationPolicyHandler;
    private final ListReplicationPoliciesHandler listReplicationPoliciesHandler;
    private final PostReplicationOptionsHandler postReplicationOptionsHandler;
    private final PostBucketWritableHandler postBucketWritableHandler;
    private final ListReplicationSourcesHandler listReplicationSourcesHandler;

    private final NotFoundHandler notFoundHandler;

    private final CommonHandler commonHandler;
    private final MetricsHandler metricsHandler;
    private final OptionCorsHandler optionsCorsHandler;
    private final HeaderCorsHandler headerCorsHandler;
    private final V2ExceptionTranslator v2ExceptionTranslator;
    private final MetricAnnotationHandlerFactory metricAnnotationHandlerFactory;
    private final FailureHandlerFactory failureHandlerFactory;
    private final MeteringHandler meteringHandler;
    //todo: remove once fully migrated
    private final HealthCheckHandler healthCheckHandler;
    private final HealthCheckHandler v2HealthCheckHandler;
    private final EmbargoHandler embargoHandler;
    private final AuditFilterHandler auditFilterHandler;
    private final URIValidationHandler uriValidationHandler;
    private final CasperLoggerHandler loggerHandler;
    private final ServerInfoHandler serverInfoHandler;
    private final ClientInfoHandler clientInfoHandler;
    private final BucketMeterSettingsHandler bucketMeterSettingsHandler;
    private final PostBucketOptionsHandler postBucketOptionsHandler;
    private final GetBucketOptionsHandler getBucketOptionsHandler;
    private final MonitoringHandler monitoringHandler;
    private final PostBucketPurgeDeletedTagsHandler postBucketPurgeDeletedTagsHandler;
    private final ServiceLogsHandler serviceLogsHandler;
    private final PostLoggingHandler postLoggingHandler;
    private final DeleteLoggingHandler deleteLoggingHandler;
    private final GetUsageHandler getUsageHandler;

    public CasperApiV2(CommonHandler commonHandler,
                       MetricsHandler metricsHandler,
                       OptionCorsHandler corsHandler,
                       HeaderCorsHandler headerCorsHandler,
                       HealthCheckHandler healthCheckHandler,
                       HealthCheckHandler v2HealthCheckHandler,
                       V2ExceptionTranslator v2ExceptionTranslator,
                       MetricAnnotationHandlerFactory metricAnnotationHandlerFactory,
                       FailureHandlerFactory failureHandlerFactory,
                       MeteringHandler meteringHandler,
                       EmbargoHandler embargoHandler,
                       AuditFilterHandler auditFilterHandler,
                       CasperLoggerHandler loggerHandler,
                       URIValidationHandler uriValidationHandler,
                       ServerInfoHandler serverInfoHandler,
                       ClientInfoHandler clientInfoHandler,
                       BucketMeterSettingsHandler bucketMeterSettingsHandler,
                       PostBucketOptionsHandler postBucketOptionsHandler,
                       GetBucketOptionsHandler getBucketOptionsHandler,
                       MonitoringHandler monitoringHandler,
                       NotFoundHandler notFoundHandler,
                       GetNamespaceCollectionHandler getNamespaceCollectionHandler,
                       GetNamespaceMetadataHandler getNamespaceMetadataHandler,
                       PutNamespaceMetadataHandler putNamespaceMetadataHandler,
                       ListBucketsHandler listBucketsHandler,
                       PostBucketCollectionHandler postBucketCollectionHandler,
                       PostBucketHandler postBucketHandler,
                       GetBucketHandler getBucketHandler,
                       DeleteBucketHandler deleteBucketHandler,
                       HeadBucketHandler headBucketHandler,
                       PutObjectHandler putObjectHandler,
                       RenameObjectHandler renameObjectHandler,
                       GetObjectHandler getObjectHandler,
                       ParPutObjectRoutingHandler parPutObjectRoutingHandler,
                       ParGetObjectHandler parGetObjectHandler,
                       ParHeadObjectHandler parHeadObjectHandler,
                       ParPutPartHandler parPutPartHandler,
                       ParCommitUploadHandler parCommitUploadHandler,
                       ParAbortUploadHandler parAbortUploadHandler,
                       HeadObjectHandler headObjectHandler,
                       DeleteObjectHandler deleteObjectHandler,
                       ListObjectsHandler listObjectHandler,
                       CreateUploadHandler createUploadHandler,
                       ListUploadsHandler listUploadsHandler,
                       CommitUploadHandler commitUploadHandler,
                       AbortUploadHandler abortUploadHandler,
                       ListUploadPartsHandler listUploadPartsHandler,
                       CreateParHandler createParHandler,
                       GetParHandler getParHandler,
                       DeleteParHandler deleteParHandler,
                       ListParsHandler listParsHandler,
                       PutPartHandler putPartHandler,
                       RestoreObjectsHandler restoreObjectsHandler,
                       GetWorkRequestHandler getWorkRequestHandler,
                       ListWorkRequestLogHandler listWorkRequestLogHandler,
                       ListWorkRequestErrorHandler listWorkRequestErrorHandler,
                       ListWorkRequestsHandler listWorkRequestsHandler,
                       CancelWorkRequestHandler cancelWorkRequestHandler,
                       CopyObjectHandler copyObjectHandler,
                       BulkRestoreHandler bulkRestoreHandler,
                       PutLifecyclePolicyHandler putLifecyclePolicyHandler,
                       GetLifecyclePolicyHandler getLifecyclePolicyHandler,
                       DeleteLifecyclePolicyHandler deleteLifecyclePolicyHandler,
                       PostObjectMetadataReplaceHandler postObjectMetadataReplaceHandler,
                       PostObjectMetadataMergeHandler postObjectMetadataMergeHandler,
                       PostObjectReencryptHandler postObjectReencryptHandler,
                       PostBucketReencryptHandler postbucketReencryptHandler,
                       CopyPartHandler copyPartHandler,
                       PostReplicationPolicyHandler postReplicationPolicyHandler,
                       GetReplicationPolicyHandler getReplicationPolicyHandler,
                       DeleteReplicationPolicyHandler deleteReplicationPolicyHandler,
                       ListReplicationPoliciesHandler listReplicationPoliciesHandler,
                       PostReplicationOptionsHandler postReplicationOptionsHandler,
                       PostBucketWritableHandler postBucketWritableHandler,
                       ListReplicationSourcesHandler listReplicationSourcesHandler,
                       PostBucketPurgeDeletedTagsHandler postBucketPurgeDeletedTagsHandler,
                       ServiceLogsHandler serviceLogsHandler,
                       PostLoggingHandler postLoggingHandler,
                       DeleteLoggingHandler deleteLoggingHandler,
                       GetUsageHandler getUsageHandler) {
        // common handlers
        this.commonHandler = commonHandler;
        this.metricsHandler = metricsHandler;
        this.optionsCorsHandler = corsHandler;
        this.headerCorsHandler = headerCorsHandler;
        this.healthCheckHandler = healthCheckHandler;
        this.v2HealthCheckHandler = v2HealthCheckHandler;
        this.v2ExceptionTranslator = v2ExceptionTranslator;
        this.metricAnnotationHandlerFactory = metricAnnotationHandlerFactory;
        this.failureHandlerFactory = failureHandlerFactory;
        this.meteringHandler = meteringHandler;
        this.embargoHandler = embargoHandler;
        this.serverInfoHandler = serverInfoHandler;
        this.clientInfoHandler = clientInfoHandler;
        this.auditFilterHandler = auditFilterHandler;
        this.loggerHandler = loggerHandler;
        this.uriValidationHandler = uriValidationHandler;
        this.bucketMeterSettingsHandler = bucketMeterSettingsHandler;
        // bucket options handlers
        this.postBucketOptionsHandler = postBucketOptionsHandler;
        this.getBucketOptionsHandler = getBucketOptionsHandler;
        this.monitoringHandler = monitoringHandler;
        this.notFoundHandler = notFoundHandler;
        // request handlers
        this.getNamespaceCollectionHandler = getNamespaceCollectionHandler;
        this.getNamespaceMetadataHandler = getNamespaceMetadataHandler;
        this.putNamespaceMetadataHandler = putNamespaceMetadataHandler;
        this.listBucketsHandler = listBucketsHandler;
        this.postBucketCollectionHandler = postBucketCollectionHandler;
        this.postBucketHandler = postBucketHandler;
        this.getBucketHandler = getBucketHandler;
        this.deleteBucketHandler = deleteBucketHandler;
        this.headBucketHandler = headBucketHandler;
        this.putObjectHandler = putObjectHandler;
        this.renameObjectHandler = renameObjectHandler;
        this.getObjectHandler = getObjectHandler;
        this.parPutObjectRoutingHandler = parPutObjectRoutingHandler;
        this.parHeadObjectHandler = parHeadObjectHandler;
        this.parGetObjectHandler = parGetObjectHandler;
        this.parPutPartHandler = parPutPartHandler;
        this.parCommitUploadHandler = parCommitUploadHandler;
        this.parAbortUploadHandler = parAbortUploadHandler;
        this.headObjectHandler = headObjectHandler;
        this.deleteObjectHandler = deleteObjectHandler;
        this.listObjectHandler = listObjectHandler;
        this.createUploadHandler = createUploadHandler;
        this.listUploadsHandler = listUploadsHandler;
        this.commitUploadHandler = commitUploadHandler;
        this.abortUploadHandler = abortUploadHandler;
        this.listUploadPartsHandler = listUploadPartsHandler;
        this.createParHandler = createParHandler;
        this.getParHandler = getParHandler;
        this.deleteParHandler = deleteParHandler;
        this.listParsHandler = listParsHandler;
        this.getWorkRequestHandler = getWorkRequestHandler;
        this.listWorkRequestLogHandler = listWorkRequestLogHandler;
        this.listWorkRequestErrorHandler = listWorkRequestErrorHandler;
        this.copyObjectHandler = copyObjectHandler;
        this.bulkRestoreHandler = bulkRestoreHandler;
        this.listWorkRequestsHandler = listWorkRequestsHandler;
        this.cancelWorkRequestHandler = cancelWorkRequestHandler;
        this.putLifecyclePolicyHandler = putLifecyclePolicyHandler;
        this.getLifecyclePolicyHandler = getLifecyclePolicyHandler;
        this.deleteLifecyclePolicyHandler = deleteLifecyclePolicyHandler;
        this.putPartHandler = putPartHandler;
        this.restoreObjectsHandler = restoreObjectsHandler;
        this.postObjectMetadataReplaceHandler = postObjectMetadataReplaceHandler;
        this.postObjectMetadataMergeHandler = postObjectMetadataMergeHandler;
        this.postObjectReencryptHandler = postObjectReencryptHandler;
        this.postbucketReencryptHandler = postbucketReencryptHandler;
        this.copyPartHandler = copyPartHandler;
        this.postReplicationPolicyHandler = postReplicationPolicyHandler;
        this.getReplicationPolicyHandler = getReplicationPolicyHandler;
        this.deleteReplicationPolicyHandler = deleteReplicationPolicyHandler;
        this.listReplicationPoliciesHandler = listReplicationPoliciesHandler;
        this.postReplicationOptionsHandler = postReplicationOptionsHandler;
        this.postBucketWritableHandler = postBucketWritableHandler;
        this.listReplicationSourcesHandler = listReplicationSourcesHandler;
        this.postBucketPurgeDeletedTagsHandler = postBucketPurgeDeletedTagsHandler;
        this.serviceLogsHandler = serviceLogsHandler;
        this.postLoggingHandler = postLoggingHandler;
        this.deleteLoggingHandler = deleteLoggingHandler;
        this.getUsageHandler = getUsageHandler;
    }

    @Override
    public Router createRouter(Vertx vertx) {
        // TODO(csgordon): return a 405 on unsupported methods instead of a 404.

        Router router = Router.router(vertx);

        router.get(HEALTHCHECK_ROUTE).useNormalisedPath(false).handler(healthCheckHandler);
        router.get(V2_HEALTHCHECK_ROUTE).useNormalisedPath(false).handler(v2HealthCheckHandler);

        // the execution order for headerEndsHandler has been reversed in Vert.x 3.3.
        router.options().useNormalisedPath(false).handler(optionsCorsHandler);
        router.route().useNormalisedPath(false).handler(headerCorsHandler);

        router.route().useNormalisedPath(false).handler(commonHandler);
        router.route().useNormalisedPath(false).handler(loggerHandler);

        //Record metered requests/bandwidth for everything other than delete
        router.route().useNormalisedPath(false).handler(meteringHandler);

        FailureHandler failureHandler = failureHandlerFactory.create(v2ExceptionTranslator);
        router.route().useNormalisedPath(false).failureHandler(failureHandler);

        router.route().useNormalisedPath(false).handler(metricsHandler);

        Annotation application = Annotations.application(CasperApiV2.class);
        MetricAnnotationHandler annotationHandler = metricAnnotationHandlerFactory.create(application);
        router.route().useNormalisedPath(false).handler(annotationHandler);

        //refuse requests that match embargo and ratelimit rules
        router.route().useNormalisedPath(false).handler(embargoHandler);

        router.route().useNormalisedPath(false).handler(auditFilterHandler);

        // validate URI before Vert.x parses it and matches against regex
        router.route().useNormalisedPath(false).handler(uriValidationHandler);

        //Monitoring - public metrics
        router.route().useNormalisedPath(false).handler(monitoringHandler);

        // Service logs
        router.route().useNormalisedPath(false).handler(serviceLogsHandler);

        router.getWithRegex(NAMESPACE_COLLECTION_ROUTE).useNormalisedPath(false).handler(getNamespaceCollectionHandler);
        router.getWithRegex(NAMESPACE_ROUTE).useNormalisedPath(false).handler(getNamespaceMetadataHandler);
        router.putWithRegex(NAMESPACE_ROUTE).useNormalisedPath(false).handler(putNamespaceMetadataHandler);

        router.getWithRegex(BUCKET_COLLECTION_ROUTE).useNormalisedPath(false).handler(listBucketsHandler);
        router.postWithRegex(BUCKET_COLLECTION_ROUTE).useNormalisedPath(false).handler(postBucketCollectionHandler);

        router.postWithRegex(BUCKET_ROUTE).useNormalisedPath(false).handler(postBucketHandler);
        router.getWithRegex(BUCKET_ROUTE).useNormalisedPath(false).handler(getBucketHandler);
        router.deleteWithRegex(BUCKET_ROUTE).useNormalisedPath(false).handler(deleteBucketHandler);
        router.headWithRegex(BUCKET_ROUTE).useNormalisedPath(false).handler(headBucketHandler);

        router.postWithRegex(PURGE_DELETED_TAGS_BUCKET_ROUTE).useNormalisedPath(false)
                .handler(postBucketPurgeDeletedTagsHandler);

        // Remember to register SERVER_INFO_ROUTE and SERVER_CLIENT_ROUTE before OBJECT_ROUTE
        router.get(SERVER_INFO_ROUTE).useNormalisedPath(false).handler(serverInfoHandler);
        router.get(CLIENT_INFO_ROUTE).useNormalisedPath(false).handler(clientInfoHandler);

        router.getWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(getObjectHandler);
        router.headWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(headObjectHandler);
        router.putWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(putObjectHandler);
        router.deleteWithRegex(OBJECT_ROUTE).useNormalisedPath(false).handler(deleteObjectHandler);

        router.postWithRegex(RENAME_ROUTE).useNormalisedPath(false).handler(renameObjectHandler);

        router.putWithRegex(BUCKET_METER_SETTINGS_ROUTE).useNormalisedPath(false).handler(bucketMeterSettingsHandler);

        router.postWithRegex(BUCKET_OPTIONS_ROUTE).useNormalisedPath(false).handler(postBucketOptionsHandler);
        router.getWithRegex(BUCKET_OPTIONS_ROUTE).useNormalisedPath(false).handler(getBucketOptionsHandler);

        router.postWithRegex(POST_LOGGING_ROUTE).useNormalisedPath(false).handler(postLoggingHandler);
        router.deleteWithRegex(DELETE_LOGGING_ROUTE).useNormalisedPath(false).handler(deleteLoggingHandler);

        router.headWithRegex(PAR_OBJECT_ROUTE).useNormalisedPath(false).handler(parHeadObjectHandler);
        router.getWithRegex(PAR_OBJECT_ROUTE).useNormalisedPath(false).handler(parGetObjectHandler);
        router.putWithRegex(PAR_OBJECT_ROUTE).useNormalisedPath(false).handler(parPutObjectRoutingHandler);
        router.putWithRegex(PAR_UPLOAD_PART_ROUTE).useNormalisedPath(false).handler(parPutPartHandler);
        router.postWithRegex(PAR_MULTIPART_ROUTE).useNormalisedPath(false).handler(parCommitUploadHandler);
        router.deleteWithRegex(PAR_MULTIPART_ROUTE).useNormalisedPath(false).handler(parAbortUploadHandler);

        router.getWithRegex(OBJECT_COLLECTION_ROUTE).useNormalisedPath(false).handler(listObjectHandler);

        router.getWithRegex(UPLOAD_COLLECTION_ROUTE).useNormalisedPath(false).handler(listUploadsHandler);
        router.postWithRegex(UPLOAD_COLLECTION_ROUTE).useNormalisedPath(false).handler(createUploadHandler);

        router.postWithRegex(UPLOAD_ROUTE).useNormalisedPath(false).handler(commitUploadHandler);
        router.deleteWithRegex(UPLOAD_ROUTE).useNormalisedPath(false).handler(abortUploadHandler);
        router.getWithRegex(UPLOAD_ROUTE).useNormalisedPath(false).handler(listUploadPartsHandler);
        router.putWithRegex(UPLOAD_ROUTE).useNormalisedPath(false).handler(putPartHandler);

        router.postWithRegex(PAR_COLLECTION_ROUTE).useNormalisedPath(false).handler(createParHandler);
        router.getWithRegex(PAR_COLLECTION_ROUTE).useNormalisedPath(false).handler(listParsHandler);

        router.getWithRegex(PAR_ROUTE).useNormalisedPath(false).handler(getParHandler);
        router.deleteWithRegex(PAR_ROUTE).useNormalisedPath(false).handler(deleteParHandler);

        router.postWithRegex(RESTORE_OBJECT_ROUTE).useNormalisedPath(false).handler(restoreObjectsHandler);

        router.postWithRegex(COPY_OBJECT_ROUTE).useNormalisedPath(false).handler(copyObjectHandler);
        router.postWithRegex(BULK_RESTORE_ROUTE).useNormalisedPath(false).handler(bulkRestoreHandler);
        router.getWithRegex(WORK_REQUEST_LOG_ROUTE).useNormalisedPath(false).handler(listWorkRequestLogHandler);
        router.getWithRegex(WORK_REQUEST_ERROR_ROUTE).useNormalisedPath(false).handler(listWorkRequestErrorHandler);
        router.getWithRegex(WORK_REQUEST_ROUTE).useNormalisedPath(false).handler(getWorkRequestHandler);
        router.getWithRegex(WORK_REQUEST_COLLECTION_ROUTE).useNormalisedPath(false).handler(listWorkRequestsHandler);
        router.deleteWithRegex(WORK_REQUEST_ROUTE).useNormalisedPath(false).handler(cancelWorkRequestHandler);

        router.putWithRegex(COPY_PART_ROUTE).useNormalisedPath(false).handler(copyPartHandler);

        router.putWithRegex(LIFECYCLE_POLICY_ROUTE).useNormalisedPath(false).handler(putLifecyclePolicyHandler);
        router.getWithRegex(LIFECYCLE_POLICY_ROUTE).useNormalisedPath(false).handler(getLifecyclePolicyHandler);
        router.deleteWithRegex(LIFECYCLE_POLICY_ROUTE).useNormalisedPath(false).handler(deleteLifecyclePolicyHandler);

        router.postWithRegex(OBJECT_METADATA_MERGE_ROUTE).useNormalisedPath(false)
                .handler(postObjectMetadataMergeHandler);
        router.postWithRegex(OBJECT_METADATA_REPLACE_ROUTE).useNormalisedPath(false)
                .handler(postObjectMetadataReplaceHandler);

        router.postWithRegex(REENCRYPT_OBJECT_ROUTE).useNormalisedPath(false).handler(postObjectReencryptHandler);
        router.postWithRegex(REENCRYPT_BUCKET_ROUTE).useNormalisedPath(false).handler(postbucketReencryptHandler);

        router.postWithRegex(REPLICATION_COLLECTION_ROUTE).useNormalisedPath(false)
                .handler(postReplicationPolicyHandler);
        router.getWithRegex(REPLICATION_COLLECTION_ROUTE).useNormalisedPath(false)
                .handler(listReplicationPoliciesHandler);

        router.getWithRegex(REPLICATION_ROUTE).useNormalisedPath(false).handler(getReplicationPolicyHandler);
        router.deleteWithRegex(REPLICATION_ROUTE).useNormalisedPath(false).handler(deleteReplicationPolicyHandler);

        router.postWithRegex(REPLICATION_OPTIONS_ROUTE).useNormalisedPath(false).handler(postReplicationOptionsHandler);
        router.postWithRegex(REPLICATION_MAKE_BUCKET_WRITABLE_ROUTE).useNormalisedPath(false)
                .handler(postBucketWritableHandler);
        router.getWithRegex(REPLICATION_SOURCES_ROUTE).useNormalisedPath(false).handler(listReplicationSourcesHandler);

        router.postWithRegex(GET_USAGE_ROUTE).useNormalisedPath(false).handler(getUsageHandler);

        // This catches requests that don't have a configured handler, so it must be the last handler in the chain.
        router.route().handler(notFoundHandler);

        return router;
    }
}
