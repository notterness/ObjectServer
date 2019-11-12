package com.oracle.pic.casper.webserver.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.v2.BoatConfiguration;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.common.config.v2.ServicePrincipalConfiguration;
import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.config.v2.WebServerConfigurations;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.healthcheck.HealthChecker;
import com.oracle.pic.casper.common.healthcheck.SimpleHealthChecker;
import com.oracle.pic.casper.common.json.JacksonSerDe;
import com.oracle.pic.casper.common.metering.InMemoryMeterWriter;
import com.oracle.pic.casper.common.metering.MeterWriter;
import com.oracle.pic.casper.common.metering.Slf4jMeterWriter;
import com.oracle.pic.casper.common.metrics.Annotation;
import com.oracle.pic.casper.common.metrics.MetricScopeWriter;
import com.oracle.pic.casper.common.monitoring.MonitoringMetricsReporter;
import com.oracle.pic.casper.common.vertx.handler.HealthCheckHandler;
import com.oracle.pic.casper.common.vertx.handler.JmxHandler;
import com.oracle.pic.casper.webserver.api.auditing.AuditFilterHandler;
import com.oracle.pic.casper.webserver.api.auditing.AuditLogger;
import com.oracle.pic.casper.webserver.api.auditing.InMemoryAuditLogger;
import com.oracle.pic.casper.webserver.api.auditing.Log4jAuditLogger;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.ParAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.auth.S3AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.common.CasperAPI;
import com.oracle.pic.casper.webserver.api.common.CasperLoggerHandler;
import com.oracle.pic.casper.webserver.api.common.CommonHandler;
import com.oracle.pic.casper.webserver.api.common.CommonHandlerFactory;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
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
import com.oracle.pic.casper.webserver.api.control.ArchiveObjectControlHandler;
import com.oracle.pic.casper.webserver.api.control.CasperApiControl;
import com.oracle.pic.casper.webserver.api.control.CheckObjectHandler;
import com.oracle.pic.casper.webserver.api.control.EmbargoControllers;
import com.oracle.pic.casper.webserver.api.control.GetVmdChangeSeqHandler;
import com.oracle.pic.casper.webserver.api.control.ListBucketsControlHandler;
import com.oracle.pic.casper.webserver.api.control.ListNamespacesControlHandler;
import com.oracle.pic.casper.webserver.api.control.ListObjectsControlHandler;
import com.oracle.pic.casper.webserver.api.control.RefreshVolumeMetadataControlHandler;
import com.oracle.pic.casper.webserver.api.healthcheck.ApiSpecificHealthChecker;
import com.oracle.pic.casper.webserver.api.logging.DeleteLoggingHandler;
import com.oracle.pic.casper.webserver.api.logging.InMemoryServiceLogWriter;
import com.oracle.pic.casper.webserver.api.logging.PostLoggingHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogWriter;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogWriterImpl;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHandlerFactory;
import com.oracle.pic.casper.webserver.api.metering.MeteringHelperImpl;
import com.oracle.pic.casper.webserver.api.monitoring.MonitoringHandler;
import com.oracle.pic.casper.webserver.api.monitoring.MonitoringHelper;
import com.oracle.pic.casper.webserver.api.pars.CreateParHandler;
import com.oracle.pic.casper.webserver.api.pars.DeleteParHandler;
import com.oracle.pic.casper.webserver.api.pars.GetParHandler;
import com.oracle.pic.casper.webserver.api.pars.ListParsHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.replication.ListReplicationSourcesHandler;
import com.oracle.pic.casper.webserver.api.replication.PostReplicationOptionsHandler;
import com.oracle.pic.casper.webserver.api.replication.PostReplicationPolicyHandler;
import com.oracle.pic.casper.webserver.api.replication.DeleteReplicationPolicyHandler;
import com.oracle.pic.casper.webserver.api.replication.GetReplicationPolicyHandler;
import com.oracle.pic.casper.webserver.api.replication.ListReplicationPoliciesHandler;
import com.oracle.pic.casper.webserver.api.replication.PostBucketWritableHandler;
import com.oracle.pic.casper.webserver.api.s3.DeleteBucketDispatchHandler;
import com.oracle.pic.casper.webserver.api.s3.DeleteBucketTaggingHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketAccelerateHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketCORSHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketDispatchHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketLifecycleHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketLocationHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketLoggingStatusHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketNotificationHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketObjectVersionsHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketPolicyHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketReplicationHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketRequestPaymentHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketTaggingHandler;
import com.oracle.pic.casper.webserver.api.s3.GetBucketWebsiteHandler;
import com.oracle.pic.casper.webserver.api.s3.PutBucketDispatchHandler;
import com.oracle.pic.casper.webserver.api.s3.PutBucketHandler;
import com.oracle.pic.casper.webserver.api.s3.PutBucketTaggingHandler;
import com.oracle.pic.casper.webserver.api.s3.S3Api;
import com.oracle.pic.casper.webserver.api.s3.S3ExceptionTranslator;
import com.oracle.pic.casper.webserver.api.s3.S3HttpHelpers;
import com.oracle.pic.casper.webserver.api.s3.S3ObjectPathHandler;
import com.oracle.pic.casper.webserver.api.s3.S3ReadObjectHelper;
import com.oracle.pic.casper.webserver.api.swift.BulkDeleteHandler;
import com.oracle.pic.casper.webserver.api.swift.DeleteContainerHandler;
import com.oracle.pic.casper.webserver.api.swift.HeadContainerHandler;
import com.oracle.pic.casper.webserver.api.swift.ListContainersHandler;
import com.oracle.pic.casper.webserver.api.swift.PostContainerHandler;
import com.oracle.pic.casper.webserver.api.swift.PutContainerHandler;
import com.oracle.pic.casper.webserver.api.swift.SwiftApi;
import com.oracle.pic.casper.webserver.api.swift.SwiftExceptionTranslator;
import com.oracle.pic.casper.webserver.api.swift.SwiftReadObjectHelper;
import com.oracle.pic.casper.webserver.api.swift.SwiftResponseWriter;
import com.oracle.pic.casper.webserver.api.v1.CasperApiV1;
import com.oracle.pic.casper.webserver.api.v1.DeleteObjectHandler;
import com.oracle.pic.casper.webserver.api.v1.GetObjectHandler;
import com.oracle.pic.casper.webserver.api.v1.HeadObjectHandler;
import com.oracle.pic.casper.webserver.api.v1.PutNamespaceHandler;
import com.oracle.pic.casper.webserver.api.v1.PutObjectHandler;
import com.oracle.pic.casper.webserver.api.v1.ScanNamespacesHandler;
import com.oracle.pic.casper.webserver.api.v1.ScanObjectsHandler;
import com.oracle.pic.casper.webserver.api.v1.V1ExceptionTranslator;
import com.oracle.pic.casper.webserver.api.v1.V1ReadObjectHelper;
import com.oracle.pic.casper.webserver.api.v2.AbortUploadHandler;
import com.oracle.pic.casper.webserver.api.v2.BucketMeterSettingsHandler;
import com.oracle.pic.casper.webserver.api.v2.BulkRestoreHandler;
import com.oracle.pic.casper.webserver.api.v2.CancelWorkRequestHandler;
import com.oracle.pic.casper.webserver.api.v2.CasperApiV2;
import com.oracle.pic.casper.webserver.api.v2.ClientInfoHandler;
import com.oracle.pic.casper.webserver.api.v2.CommitUploadHandler;
import com.oracle.pic.casper.webserver.api.v2.CopyObjectHandler;
import com.oracle.pic.casper.webserver.api.v2.CopyPartHandler;
import com.oracle.pic.casper.webserver.api.v2.CreateUploadHandler;
import com.oracle.pic.casper.webserver.api.v2.DeleteBucketHandler;
import com.oracle.pic.casper.webserver.api.v2.DeleteLifecyclePolicyHandler;
import com.oracle.pic.casper.webserver.api.v2.GetBucketHandler;
import com.oracle.pic.casper.webserver.api.v2.GetBucketOptionsHandler;
import com.oracle.pic.casper.webserver.api.v2.GetLifecyclePolicyHandler;
import com.oracle.pic.casper.webserver.api.v2.GetNamespaceCollectionHandler;
import com.oracle.pic.casper.webserver.api.v2.GetNamespaceMetadataHandler;
import com.oracle.pic.casper.webserver.api.v2.GetUsageHandler;
import com.oracle.pic.casper.webserver.api.v2.GetWorkRequestHandler;
import com.oracle.pic.casper.webserver.api.v2.HeadBucketHandler;
import com.oracle.pic.casper.webserver.api.v2.ListBucketsHandler;
import com.oracle.pic.casper.webserver.api.v2.ListObjectsHandler;
import com.oracle.pic.casper.webserver.api.v2.ListUploadPartsHandler;
import com.oracle.pic.casper.webserver.api.v2.ListUploadsHandler;
import com.oracle.pic.casper.webserver.api.v2.ListWorkRequestErrorHandler;
import com.oracle.pic.casper.webserver.api.v2.ListWorkRequestLogHandler;
import com.oracle.pic.casper.webserver.api.v2.ListWorkRequestsHandler;
import com.oracle.pic.casper.webserver.api.v2.ParAbortUploadHandler;
import com.oracle.pic.casper.webserver.api.v2.ParCommitUploadHandler;
import com.oracle.pic.casper.webserver.api.v2.ParCreateUploadHandler;
import com.oracle.pic.casper.webserver.api.v2.ParGetObjectHandler;
import com.oracle.pic.casper.webserver.api.v2.ParHeadObjectHandler;
import com.oracle.pic.casper.webserver.api.v2.ParPutObjectHandler;
import com.oracle.pic.casper.webserver.api.v2.ParPutObjectRoutingHandler;
import com.oracle.pic.casper.webserver.api.v2.ParPutPartHandler;
import com.oracle.pic.casper.webserver.api.v2.PostBucketCollectionHandler;
import com.oracle.pic.casper.webserver.api.v2.PostBucketHandler;
import com.oracle.pic.casper.webserver.api.v2.PostBucketOptionsHandler;
import com.oracle.pic.casper.webserver.api.v2.PostBucketPurgeDeletedTagsHandler;
import com.oracle.pic.casper.webserver.api.v2.PostBucketReencryptHandler;
import com.oracle.pic.casper.webserver.api.v2.PostObjectMetadataMergeHandler;
import com.oracle.pic.casper.webserver.api.v2.PostObjectMetadataReplaceHandler;
import com.oracle.pic.casper.webserver.api.v2.PostObjectReencryptHandler;
import com.oracle.pic.casper.webserver.api.v2.PutLifecyclePolicyHandler;
import com.oracle.pic.casper.webserver.api.v2.PutNamespaceMetadataHandler;
import com.oracle.pic.casper.webserver.api.v2.PutPartHandler;
import com.oracle.pic.casper.webserver.api.v2.RenameObjectHandler;
import com.oracle.pic.casper.webserver.api.v2.RestoreObjectsHandler;
import com.oracle.pic.casper.webserver.api.v2.ServerInfoHandler;
import com.oracle.pic.casper.webserver.api.v2.V2ExceptionTranslator;
import com.oracle.pic.casper.webserver.api.v2.V2ReadObjectHelper;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.util.ConfigValue;
import com.oracle.pic.casper.webserver.util.TestTenants;
import com.uber.jaeger.Tracer;

import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A collection of APIs, each of which has a Vert.x router that maps URLs to Vert.x web handlers.
 *
 * The web request handlers have a lot of dependencies, so most of this code is simply constructing those handlers and
 * their associated APIs from their dependencies. This class is the last to be constructed, and depends on all of the
 * other WebServer* classes.
 */
public final class WebServerAPIs {

    private final CasperApiV1 casperApiV1;
    private final CasperApiV2 casperApiV2;
    private final S3Api s3Api;
    private final SwiftApi swiftApi;
    private final CasperApiControl casperApiControl;
    private final MeterWriter meterWriter;
    private final AuditLogger auditLogger;
    private final CountingHandler.MaximumContentLength maximumContentLength;
    private final ServiceLogWriter serviceLogWriter;
    private final CasperVIPRouter casperVIPRouter;

    private CasperApiV1 createCasperApiV1(
            WebServerConfiguration config,
            WebServerBackends backends,
            ObjectMapper mapper,
            MetricAnnotationHandlerFactory metricAnnotationHandlerFactory,
            FailureHandlerFactory failureHandlerFactory,
            CommonHandlerFactory commonHandlerFactory,
            HealthChecker healthChecker,
            MeteringHandler meteringHandler,
            MetricScopeWriter metricScopeWriter,
            CasperLoggerHandler loggerHandler,
            DecidingKeyManagementService kms,
            TrafficController controller,
            EmbargoV3 embargoV3) {
        V1ReadObjectHelper readHelper = new V1ReadObjectHelper(backends.getGetObjBackend(), kms);

        return new CasperApiV1(
            new MetricsHandler(metricScopeWriter, config.getThroughputMetricsObjectFilterSize()),
            new ScanNamespacesHandler(backends.getBucketBackend(), mapper, embargoV3),
            new ScanObjectsHandler(backends.getBackend(), mapper, embargoV3),
            new PutNamespaceHandler(backends.getBucketBackend(), backends.getTenantBackend(), embargoV3),
            new HeadObjectHandler(readHelper, embargoV3),
            new GetObjectHandler(controller, readHelper, embargoV3),
            new PutObjectHandler(controller, backends.getPutObjBackend(), config, embargoV3),
            new DeleteObjectHandler(backends.getBackend(), embargoV3),
            commonHandlerFactory,
            new V1ExceptionTranslator(),
            metricAnnotationHandlerFactory,
            failureHandlerFactory,
            new HealthCheckHandler(healthChecker),
            new HealthCheckHandler(new ApiSpecificHealthChecker(healthChecker, "V1")),
            meteringHandler,
            loggerHandler);
    }

    private CasperApiV2 createCasperApiV2(WebServerConfigurations configs,
                                          WebServerClients clients,
                                          WebServerAuths auths,
                                          WebServerBackends backends,
                                          ObjectMapper mapper,
                                          JacksonSerDe jacksonSerDe,
                                          MetricAnnotationHandlerFactory metricAnnotationHandlerFactory,
                                          FailureHandlerFactory failureHandlerFactory,
                                          AuditFilterHandler auditFilterHandler,
                                          HealthChecker healthChecker,
                                          MeteringHandler meteringHandler,
                                          MetricScopeWriter metricScopeWriter,
                                          Tracer tracer,
                                          CasperLoggerHandler loggerHandler,
                                          URIValidationHandler uriValidationHandler,
                                          DecidingKeyManagementService kms,
                                          BoatConfiguration boatConfiguration,
                                          TrafficController controller,
                                          MonitoringHandler monitoringHandler,
                                          ServiceLogsHandlerFactory serviceLogsHandlerFactory,
                                          EmbargoV3 embargoV3) {
        WebServerConfiguration config = configs.getApiConfiguration();
        List<String> servicePrincipals = configs.getServicePrincipalConfigurationList().stream()
                .map(ServicePrincipalConfiguration::getTenantId).collect(Collectors.toList());
        Authenticator authn = auths.getAuthenticator();
        AsyncAuthenticator asyncAuthn = auths.getAsyncAuthenticator();
        ParAuthenticator parAuthn = backends.getParAuthenticator();
        ResourceControlledMetadataClient metadataClient = auths.getMetadataClient();

        V2ReadObjectHelper readHelper = new V2ReadObjectHelper(
                auths.getAsyncAuthenticator(), backends.getGetObjBackend(), kms);
        ParPutObjectHandler parPutObjectHandler = new ParPutObjectHandler(parAuthn, backends.getParPutObjBackend(),
                config, controller, servicePrincipals, embargoV3);
        ParCreateUploadHandler parCreateUploadHandler = new ParCreateUploadHandler(parAuthn, backends.getParBackend(),
                mapper, embargoV3);
        V2ExceptionTranslator v2ExceptionTranslator = new V2ExceptionTranslator(mapper);
        PostLoggingHandler postLoggingHandler = new PostLoggingHandler(authn, maximumContentLength, mapper,
                backends.getBucketBackend(), metadataClient);
        return new CasperApiV2(
                new CommonHandler(tracer,
                                  configs.getApiConfiguration().getEagleConfiguration().getPort(),
                                  CasperAPI.V2),
                new MetricsHandler(metricScopeWriter,
                        configs.getApiConfiguration().getThroughputMetricsObjectFilterSize()),
                new OptionCorsHandler(),
                new HeaderCorsHandler(),
                new HealthCheckHandler(healthChecker),
                new HealthCheckHandler(new ApiSpecificHealthChecker(healthChecker, "V2")),
                v2ExceptionTranslator,
                metricAnnotationHandlerFactory,
                failureHandlerFactory,
                meteringHandler,
                new EmbargoHandler(auths.getEmbargo()),
                auditFilterHandler,
                loggerHandler,
                uriValidationHandler,
                new ServerInfoHandler(authn, auths.getAuthorizer(), mapper, boatConfiguration),
                new ClientInfoHandler(authn, auths.getAuthorizer(), mapper, boatConfiguration),
                new BucketMeterSettingsHandler(
                        authn, backends.getBucketBackend(), mapper, maximumContentLength, embargoV3),
                new PostBucketOptionsHandler(
                        authn, backends.getBucketBackend(), mapper, maximumContentLength, embargoV3),
                new GetBucketOptionsHandler(authn, backends.getBucketBackend(), mapper, embargoV3),
                monitoringHandler,
                new NotFoundHandler(),
                new GetNamespaceCollectionHandler(authn, backends.getBackend(), mapper, embargoV3),
                new GetNamespaceMetadataHandler(authn, backends.getBucketBackend(), mapper, metadataClient, embargoV3),
                new PutNamespaceMetadataHandler(
                        authn, backends.getBucketBackend(), mapper, metadataClient, maximumContentLength, embargoV3),
                new ListBucketsHandler(authn, backends.getBucketBackend(), mapper, embargoV3),
                new PostBucketCollectionHandler(authn,
                        mapper,
                        backends.getBucketBackend(),
                        backends.getTenantBackend(),
                        metadataClient,
                        configs.getAuditConfiguration(),
                        maximumContentLength, embargoV3),
                new PostBucketHandler(authn, backends.getBucketBackend(), mapper, metadataClient,
                        configs.getAuditConfiguration(), maximumContentLength, embargoV3),
                new GetBucketHandler(authn, mapper, backends.getBucketBackend(), embargoV3),
                new DeleteBucketHandler(authn, backends.getBucketBackend(), embargoV3),
                new HeadBucketHandler(authn, backends.getBucketBackend(), embargoV3),
                new com.oracle.pic.casper.webserver.api.v2.PutObjectHandler(
                        controller, asyncAuthn, backends.getPutObjBackend(), config, servicePrincipals, embargoV3),
                new RenameObjectHandler(authn, backends.getBackend(), mapper, maximumContentLength, embargoV3),
                new com.oracle.pic.casper.webserver.api.v2.GetObjectHandler(controller, readHelper, embargoV3),
                new ParPutObjectRoutingHandler(parPutObjectHandler, parCreateUploadHandler),
                new ParGetObjectHandler(parAuthn, backends.getParGetObjBackend(), kms, embargoV3),
                new ParHeadObjectHandler(parAuthn, backends.getParGetObjBackend(), kms, embargoV3),
                new ParPutPartHandler(parAuthn, backends.getParPutObjBackend(), config, controller, embargoV3),
                new ParCommitUploadHandler(parAuthn, backends.getParBackend(), embargoV3),
                new ParAbortUploadHandler(parAuthn, backends.getParBackend(), embargoV3),
                new com.oracle.pic.casper.webserver.api.v2.HeadObjectHandler(readHelper, embargoV3),
                new com.oracle.pic.casper.webserver.api.v2.DeleteObjectHandler(authn, backends.getBackend(), embargoV3),
                new ListObjectsHandler(authn, backends.getBackend(), mapper, embargoV3),
                new CreateUploadHandler(authn, backends.getBackend(), mapper, maximumContentLength, embargoV3),
                new ListUploadsHandler(authn, backends.getBackend(), mapper, jacksonSerDe, embargoV3),
                new CommitUploadHandler(
                        authn, backends.getBackend(), mapper, maximumContentLength, servicePrincipals, embargoV3),
                new AbortUploadHandler(authn, backends.getBackend(), embargoV3),
                new ListUploadPartsHandler(authn, backends.getBackend(), mapper, embargoV3),
                new CreateParHandler(
                        asyncAuthn, backends.getPreAuthenticatedRequestBackend(), mapper, maximumContentLength,
                        embargoV3),
                new GetParHandler(asyncAuthn, backends.getPreAuthenticatedRequestBackend(), mapper, embargoV3),
                new DeleteParHandler(authn, backends.getPreAuthenticatedRequestBackend(), embargoV3),
                new ListParsHandler(asyncAuthn, backends.getPreAuthenticatedRequestBackend(), mapper, embargoV3),
                new PutPartHandler(controller, asyncAuthn, backends.getPutObjBackend(), config, embargoV3),
                new RestoreObjectsHandler(authn, backends.getBackend(), mapper, maximumContentLength, embargoV3),
                new GetWorkRequestHandler(authn, mapper, backends.getWorkRequestBackend(), embargoV3),
                new ListWorkRequestLogHandler(authn, mapper, backends.getWorkRequestBackend(), embargoV3),
                new ListWorkRequestErrorHandler(authn, mapper, backends.getWorkRequestBackend(), embargoV3),
                new ListWorkRequestsHandler(authn, mapper, backends.getWorkRequestBackend(), embargoV3),
                new CancelWorkRequestHandler(authn, backends.getWorkRequestBackend(), embargoV3),
                new CopyObjectHandler(
                        authn, mapper, backends.getWorkRequestBackend(), maximumContentLength, embargoV3),
                new BulkRestoreHandler(
                        authn, mapper, backends.getWorkRequestBackend(), maximumContentLength, embargoV3),
                new PutLifecyclePolicyHandler(authn, backends.getPutObjBackend(), backends.getBucketBackend(),
                        backends.getTenantBackend(), mapper, clients.getLifecycleEngineClient(), embargoV3),
                new GetLifecyclePolicyHandler(
                        authn, backends.getBucketBackend(), backends.getGetObjBackend(), mapper, embargoV3),
                new DeleteLifecyclePolicyHandler(authn, backends.getBucketBackend(),
                        clients.getLifecycleEngineClient(), embargoV3),
                new PostObjectMetadataReplaceHandler(authn, backends.getBackend(), mapper, maximumContentLength,
                        servicePrincipals, embargoV3),
                new PostObjectMetadataMergeHandler(authn, backends.getBackend(), mapper, maximumContentLength,
                        servicePrincipals, embargoV3),
                new PostObjectReencryptHandler(authn, backends.getBackend(), mapper, maximumContentLength, embargoV3),
                new PostBucketReencryptHandler(authn, backends.getBucketBackend(), maximumContentLength, embargoV3),
                new CopyPartHandler(asyncAuthn, backends.getPutObjBackend(), config, backends.getGetObjBackend(),
                        mapper, kms, v2ExceptionTranslator, maximumContentLength, embargoV3),
                new PostReplicationPolicyHandler(asyncAuthn, backends.getWorkRequestBackend(),
                        mapper, maximumContentLength, configs.getReplicationConfiguration(), embargoV3),
                new GetReplicationPolicyHandler(authn, backends.getGetObjBackend(),
                        backends.getWorkRequestBackend(), mapper, embargoV3),
                new DeleteReplicationPolicyHandler(
                        asyncAuthn, backends.getWorkRequestBackend(), configs.getReplicationConfiguration(), embargoV3),
                new ListReplicationPoliciesHandler(asyncAuthn, backends.getWorkRequestBackend(), mapper, embargoV3),
                new PostReplicationOptionsHandler(
                        authn, backends.getBucketBackend(), mapper, maximumContentLength, embargoV3),
                new PostBucketWritableHandler(authn, backends.getBucketBackend(), maximumContentLength, embargoV3),
                new ListReplicationSourcesHandler(authn, mapper, backends.getWorkRequestBackend(), embargoV3),
                new PostBucketPurgeDeletedTagsHandler(
                        authn, backends.getBucketBackend(), maximumContentLength, embargoV3),
                serviceLogsHandlerFactory.create(v2ExceptionTranslator),
                postLoggingHandler,
                new DeleteLoggingHandler(authn, backends.getBucketBackend(), metadataClient),
                new GetUsageHandler(authn, backends.getGetUsageBackend(), maximumContentLength, mapper));
    }

    private S3Api createS3Api(
        WebServerConfiguration configs,
        WebServerAuths auths,
        WebServerBackends backends,
        MetricAnnotationHandlerFactory metricAnnotationHandlerFactory,
        FailureHandlerFactory failureHandlerFactory,
        HealthChecker healthChecker,
        MeteringHandler meteringHandler,
        MetricScopeWriter metricScopeWriter,
        Tracer tracer,
        CasperLoggerHandler loggerHandler,
        AuditFilterHandler auditFilterHandler,
        DecidingKeyManagementService kms,
        TrafficController controller,
        MonitoringHandler monitoringHandler,
        ServiceLogsHandlerFactory serviceLogsHandlerFactory,
        EmbargoV3 embargoV3) {

        S3Authenticator s3Authn = auths.getS3Authenticator();
        S3AsyncAuthenticator s3AsyncAuthn = auths.getS3AsyncAuthenticator();
        ResourceControlledMetadataClient metadataClient = auths.getMetadataClient();

        XmlMapper xmlMapper = S3HttpHelpers.getXmlMapper();
        XmlMapper xmlMapperWithoutDeclaration = S3HttpHelpers.getXmlMapperWithoutXmlDeclaration();

        com.oracle.pic.casper.webserver.api.s3.ListObjectsHandler listObjectsHandler =
                new com.oracle.pic.casper.webserver.api.s3.ListObjectsHandler(
                        s3Authn, xmlMapper, backends.getBackend(), embargoV3);
        com.oracle.pic.casper.webserver.api.s3.ListUploadsHandler listUploadsHandler =
                new com.oracle.pic.casper.webserver.api.s3.ListUploadsHandler(
                        s3Authn, backends.getBackend(), xmlMapper, embargoV3);

        S3ReadObjectHelper readHelper = new S3ReadObjectHelper(s3AsyncAuthn,
                backends.getGetObjBackend(), kms);
        S3ObjectPathHandler s3ObjectPathHandler = new S3ObjectPathHandler(
                new com.oracle.pic.casper.webserver.api.s3.PutObjectHandler(
                        controller, s3AsyncAuthn, backends.getPutObjBackend(), configs, embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.HeadObjectHandler(readHelper, embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.GetObjectHandler(controller, readHelper, embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.DeleteObjectHandler(
                        s3Authn, backends.getBackend(), embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.CreateUploadHandler(
                        s3Authn, backends.getBackend(), xmlMapper, embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.AbortUploadHandler(
                        s3Authn, backends.getBackend(), embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.CommitUploadHandler(
                        s3Authn, backends.getBackend(), xmlMapper, maximumContentLength, embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.ListUploadPartsHandler(
                        s3Authn, backends.getBackend(), xmlMapper, embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.PutPartHandler(
                        controller, s3AsyncAuthn, backends.getPutObjBackend(), configs, embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.RestoreObjectsHandler(
                        s3Authn, backends.getBackend(), xmlMapper, maximumContentLength, embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.UpdateMetadataHandler(
                        s3Authn, backends.getBackend(), xmlMapper, embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.CopyObjectHandler(
                        s3AsyncAuthn,
                        backends.getPutObjBackend(),
                        backends.getGetObjBackend(),
                        xmlMapperWithoutDeclaration,
                        kms,
                        embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.CopyPartHandler(s3AsyncAuthn, backends.getPutObjBackend(),
                        configs, backends.getGetObjBackend(), xmlMapperWithoutDeclaration, kms, embargoV3));
        NotFoundHandler notFoundHandler = new NotFoundHandler();
        S3ExceptionTranslator s3ExceptionTranslator = new S3ExceptionTranslator(xmlMapper);
        return new S3Api(
                new CommonHandler(true,
                                  tracer,
                                  configs.getEagleConfiguration().getPort(),
                                  CasperAPI.S3),
                new MetricsHandler(metricScopeWriter, configs.getThroughputMetricsObjectFilterSize()),
                metricAnnotationHandlerFactory,
                meteringHandler,
                s3ExceptionTranslator,
                failureHandlerFactory,
                loggerHandler,
                new EmbargoHandler(auths.getEmbargo()),
                auditFilterHandler,
                new HealthCheckHandler(healthChecker),
                new HealthCheckHandler(new ApiSpecificHealthChecker(healthChecker, "S3")),
                new URIValidationHandler(),
                new com.oracle.pic.casper.webserver.api.s3.ListBucketsHandler(
                    s3Authn,
                    backends.getBucketBackend(),
                    xmlMapper,
                    embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.HeadBucketHandler(
                        s3Authn, backends.getBucketBackend(), embargoV3),
                new com.oracle.pic.casper.webserver.api.s3.PostBucketHandler(
                        s3Authn, backends.getBackend(), xmlMapper, maximumContentLength, embargoV3),
                s3ObjectPathHandler,
                new PutBucketDispatchHandler(new PutBucketHandler(s3Authn, backends.getBucketBackend(),
                        backends.getTenantBackend(), xmlMapper, metadataClient, maximumContentLength, embargoV3),
                        new PutBucketTaggingHandler(
                            s3Authn, backends.getBucketBackend(), maximumContentLength, embargoV3),
                        notFoundHandler,
                        embargoV3),
                new GetBucketDispatchHandler(listObjectsHandler, listUploadsHandler,
                        new GetBucketTaggingHandler(s3Authn, backends.getBucketBackend(), embargoV3),
                        new GetBucketLocationHandler(s3Authn, backends.getBucketBackend(), xmlMapper, embargoV3),
                        new GetBucketObjectVersionsHandler(s3Authn, backends.getBackend(), xmlMapper, embargoV3),
                        new GetBucketAccelerateHandler(s3Authn, backends.getBucketBackend(), xmlMapper, embargoV3),
                        new GetBucketCORSHandler(s3Authn, backends.getBucketBackend(), embargoV3),
                        new GetBucketLifecycleHandler(s3Authn, backends.getBucketBackend(), embargoV3),
                        new GetBucketLoggingStatusHandler(s3Authn, backends.getBucketBackend(), xmlMapper, embargoV3),
                        new GetBucketNotificationHandler(s3Authn, backends.getBucketBackend(), xmlMapper, embargoV3),
                        new GetBucketPolicyHandler(s3Authn, backends.getBucketBackend(), embargoV3),
                        new GetBucketReplicationHandler(s3Authn, backends.getBucketBackend(), embargoV3),
                        new GetBucketRequestPaymentHandler(s3Authn, backends.getBucketBackend(), xmlMapper, embargoV3),
                        new GetBucketWebsiteHandler(s3Authn, backends.getBucketBackend(), embargoV3),
                        notFoundHandler,
                        embargoV3),
                new DeleteBucketDispatchHandler(new com.oracle.pic.casper.webserver.api.s3.DeleteBucketHandler(s3Authn,
                    backends.getBucketBackend(), embargoV3),
                    new DeleteBucketTaggingHandler(s3Authn, backends.getBucketBackend(), embargoV3), notFoundHandler),
                monitoringHandler,
                serviceLogsHandlerFactory.create(s3ExceptionTranslator));
    }

    private SwiftApi createSwiftApi(WebServerConfiguration config,
                                    WebServerAuths auths,
                                    WebServerBackends backends,
                                    MetricAnnotationHandlerFactory metricAnnotationHandlerFactory,
                                    FailureHandlerFactory failureHandlerFactory,
                                    HealthChecker healthChecker,
                                    MeteringHandler meteringHandler,
                                    ObjectMapper mapper,
                                    MetricScopeWriter metricScopeWriter,
                                    Tracer tracer,
                                    CasperLoggerHandler loggerHandler,
                                    AuditFilterHandler auditFilterHandler,
                                    URIValidationHandler uriValidationHandler,
                                    DecidingKeyManagementService kms,
                                    TrafficController controller,
                                    MonitoringHandler monitoringHandler,
                                    EmbargoV3 embargoV3,
                                    ConfigValue casper6145FixDisabled,
                                    ServiceLogsHandlerFactory serviceLogsHandlerFactory) {
        Authenticator authn = auths.getAuthenticator();
        SwiftResponseWriter responseWriter = new SwiftResponseWriter(mapper);
        SwiftReadObjectHelper readHelper =
                new SwiftReadObjectHelper(authn, backends.getGetObjBackend(), backends.getTenantBackend(), kms);
        SwiftExceptionTranslator swiftExceptionTranslator = new SwiftExceptionTranslator();
        return new SwiftApi(new ListContainersHandler(authn,
            backends.getBucketBackend(),
            backends.getTenantBackend(),
            responseWriter,
            embargoV3),
            new PostContainerHandler(authn,
                backends.getBucketBackend(),
                backends.getTenantBackend(),
                responseWriter,
                maximumContentLength,
                embargoV3),
            new HeadContainerHandler(
                authn,
                backends.getBucketBackend(),
                backends.getTenantBackend(),
                responseWriter,
                embargoV3),
            new PutContainerHandler(authn,
                backends.getBucketBackend(),
                backends.getTenantBackend(),
                responseWriter,
                auths.getMetadataClient(),
                maximumContentLength,
                embargoV3),
            new DeleteContainerHandler(
                authn,
                backends.getBucketBackend(),
                backends.getTenantBackend(),
                responseWriter,
                embargoV3),
            new com.oracle.pic.casper.webserver.api.swift.ListObjectsHandler(authn,
                backends.getBackend(),
                backends.getTenantBackend(),
                responseWriter,
                embargoV3,
                casper6145FixDisabled),
            new com.oracle.pic.casper.webserver.api.swift.PutObjectHandler(controller,
                authn,
                backends.getPutObjBackend(),
                backends.getTenantBackend(),
                responseWriter,
                config,
                embargoV3),
            new com.oracle.pic.casper.webserver.api.swift.GetObjectHandler(
                controller,
                readHelper,
                responseWriter,
                kms,
                embargoV3),
            new com.oracle.pic.casper.webserver.api.swift.HeadObjectHandler(readHelper, responseWriter, kms, embargoV3),
            new com.oracle.pic.casper.webserver.api.swift.DeleteObjectHandler(
                authn,
                backends.getBackend(),
                backends.getTenantBackend(),
                responseWriter,
                embargoV3),
            new com.oracle.pic.casper.webserver.api.swift.HeadAccountHandler(
                authn,
                backends.getTenantBackend(),
                responseWriter,
                embargoV3),
            new BulkDeleteHandler(
                authn,
                backends.getBackend(),
                backends.getTenantBackend(),
                responseWriter,
                embargoV3,
                maximumContentLength),
            new CommonHandler(tracer, config.getEagleConfiguration().getPort(), CasperAPI.SWIFT),
            new MetricsHandler(metricScopeWriter, config.getThroughputMetricsObjectFilterSize()),
            swiftExceptionTranslator,
            metricAnnotationHandlerFactory,
            meteringHandler,
            failureHandlerFactory,
            new HealthCheckHandler(healthChecker),
            new HealthCheckHandler(new ApiSpecificHealthChecker(healthChecker, "SWIFT")),
            new OptionCorsHandler(),
            new HeaderCorsHandler(),
            new NotFoundHandler(),
            loggerHandler,
            new EmbargoHandler(auths.getEmbargo()),
            auditFilterHandler,
            uriValidationHandler,
            monitoringHandler,
            serviceLogsHandlerFactory.create(swiftExceptionTranslator));
    }

    private CasperApiControl createCasperApiControl(WebServerClients clients,
                                                    WebServerAuths auths,
                                                    WebServerBackends backends,
                                                    MetricAnnotationHandlerFactory metricAnnotationHandlerFactory,
                                                    FailureHandlerFactory failureHandlerFactory,
                                                    CommonHandlerFactory commonHandlerFactory,
                                                    HealthChecker healthChecker,
                                                    ObjectMapper mapper,
                                                    MetricScopeWriter metricScopeWriter,
                                                    BoatConfiguration boatConfiguration,
                                                    MonitoringHandler monitoringHandler) {
        return new CasperApiControl(
                commonHandlerFactory,
                new V2ExceptionTranslator(mapper),
                metricAnnotationHandlerFactory,
                failureHandlerFactory,
                new HealthCheckHandler(healthChecker),
                new RefreshVolumeMetadataControlHandler(clients.getVolumeMetadataCache()),
                new GetVmdChangeSeqHandler(clients.getVolumeMetadataCache(), mapper),
                new EmbargoControllers(auths.getEmbargo().getRuleCollector()),
                new JmxHandler(),
                new ListBucketsControlHandler(backends.getBucketBackend(), mapper),
                new ListObjectsControlHandler(backends.getBackend(), mapper),
                new ListNamespacesControlHandler(backends.getBucketBackend(), mapper),
                new ArchiveObjectControlHandler(backends.getBackend()),
                new MetricsHandler(metricScopeWriter, 1000000L),
                new NotFoundHandler(),
                new CheckObjectHandler(
                        auths.getAsyncAuthenticator(),
                        boatConfiguration,
                        backends.getGetObjBackend()),
                monitoringHandler);
    }

    WebServerAPIs(WebServerFlavor flavor,
                  CasperConfig config,
                  WebServerClients clients,
                  WebServerAuths auths,
                  WebServerBackends backends,
                  ObjectMapper mapper,
                  JacksonSerDe jacksonSerDe,
                  Clock clock,
                  MetricScopeWriter metricScopeWriter,
                  Tracer tracer,
                  DecidingKeyManagementService kms,
                  TrafficController controller,
                  MonitoringMetricsReporter metricsReporter,
                  ConfigValue casper6145FixDisabled) {

        MetricAnnotationHandlerFactory metricAnnotationHandlerFactory = new MetricAnnotationHandlerFactory() {
            @Override
            public MetricAnnotationHandler create(Annotation annotation) {
                return new MetricAnnotationHandler(annotation);
            }

            @Override
            public MetricAnnotationHandler create(String key, Object value) {
                return new MetricAnnotationHandler(key, value);
            }
        };

        FailureHandlerFactory failureHandlerFactory =
                wsExceptionTranslator -> new FailureHandler(
                        wsExceptionTranslator,
                        auths.getEmbargo(),
                        config.getWebServerConfigurations().getApiConfiguration().getCloseTimeout()
                );

        CommonHandlerFactory commonHandlerFactory
                = (useBadRequestException, casperAPI) ->
                new CommonHandler(
                        useBadRequestException, tracer,
                        config.getWebServerConfigurations().getApiConfiguration().getEagleConfiguration().getPort(),
                        casperAPI);

        HealthChecker healthChecker = new SimpleHealthChecker();

        if (flavor == WebServerFlavor.INTEGRATION_TESTS) {
            meterWriter = new InMemoryMeterWriter();
        } else {
            meterWriter = new Slf4jMeterWriter(jacksonSerDe);
        }

        if (flavor == WebServerFlavor.INTEGRATION_TESTS || config.getRegion() == ConfigRegion.LOCAL ||
            config.getRegion() == ConfigRegion.LGL) {
            auditLogger = new InMemoryAuditLogger();
            serviceLogWriter = new InMemoryServiceLogWriter();
        } else {
            auditLogger = new Log4jAuditLogger(jacksonSerDe);
            serviceLogWriter = new ServiceLogWriterImpl(jacksonSerDe);
        }
        final AuditFilterHandler auditFilterHandler = new AuditFilterHandler(config.getEventServiceConfiguration(),
            auditLogger, auths.getLimits());
        CasperLoggerHandler casperLoggerHandler = new CasperLoggerHandler();
        URIValidationHandler uriValidationHandler = new URIValidationHandler();

        maximumContentLength = new CountingHandler.MaximumContentLength(
                config.getWebServerConfigurations().getApiConfiguration().getMaxContentLength()
        );
        MeteringHandler meteringHandler = new MeteringHandler(new MeteringHelperImpl(clock, meterWriter,
                config.getWebServerConfigurations().getServicePrincipalConfigurationList(),
                new TestTenants(config.getWebServerConfigurations().getTestTenancies())));

        ServiceLogsHandlerFactory serviceLogsHandlerFactory = wsExceptionTranslator -> new ServiceLogsHandler(
                serviceLogWriter,
                config.getWebServerConfigurations().getServiceLogConfiguration(),
                wsExceptionTranslator);

        casperApiV1 = createCasperApiV1(
                config.getWebServerConfigurations().getApiConfiguration(),
                backends,
                mapper,
                metricAnnotationHandlerFactory,
                failureHandlerFactory,
                commonHandlerFactory,
                healthChecker,
                meteringHandler,
                metricScopeWriter,
                casperLoggerHandler,
                kms,
                controller,
                auths.getEmbargoV3());

        casperApiV2 = createCasperApiV2(
                config.getWebServerConfigurations(),
                clients,
                auths,
                backends,
                mapper,
                jacksonSerDe,
                metricAnnotationHandlerFactory,
                failureHandlerFactory,
                auditFilterHandler,
                healthChecker,
                meteringHandler,
                metricScopeWriter,
                tracer,
                casperLoggerHandler,
                uriValidationHandler,
                kms,
                config.getBOATConfiguration(),
                controller,
                new MonitoringHandler(
                        new MonitoringHelper(clock, metricsReporter), config.getPublicTelemetryConfiguration()),
                serviceLogsHandlerFactory,
                auths.getEmbargoV3());

        s3Api = createS3Api(
                config.getWebServerConfigurations().getApiConfiguration(),
                auths,
                backends,
                metricAnnotationHandlerFactory,
                failureHandlerFactory,
                healthChecker,
                meteringHandler,
                metricScopeWriter,
                tracer,
                casperLoggerHandler,
                auditFilterHandler,
                kms,
                controller,
                new MonitoringHandler(
                        new MonitoringHelper(clock, metricsReporter), config.getPublicTelemetryConfiguration()),
                serviceLogsHandlerFactory,
                auths.getEmbargoV3());

        swiftApi = createSwiftApi(
                config.getWebServerConfigurations().getApiConfiguration(),
                auths,
                backends,
                metricAnnotationHandlerFactory,
                failureHandlerFactory,
                healthChecker,
                meteringHandler,
                mapper,
                metricScopeWriter,
                tracer,
                casperLoggerHandler,
                auditFilterHandler,
                uriValidationHandler,
                kms,
                controller,
                new MonitoringHandler(
                        new MonitoringHelper(clock, metricsReporter), config.getPublicTelemetryConfiguration()),
                auths.getEmbargoV3(),
                casper6145FixDisabled,
                serviceLogsHandlerFactory);

        casperApiControl = createCasperApiControl(
                clients,
                auths,
                backends,
                metricAnnotationHandlerFactory,
                failureHandlerFactory,
                commonHandlerFactory,
                healthChecker,
                mapper,
                metricScopeWriter,
                config.getBOATConfiguration(),
                new MonitoringHandler(
                        new MonitoringHelper(clock, metricsReporter), config.getPublicTelemetryConfiguration()));


        casperVIPRouter = new CasperVIPRouter(
                config.getWebServerConfigurations().getApiConfiguration().getEagleConfiguration(),
                this
        );

    }

    CasperApiV1 getCasperApiV1() {
        return casperApiV1;
    }

    CasperApiV2 getCasperApiV2() {
        return casperApiV2;
    }

    S3Api getS3Api() {
        return s3Api;
    }

    SwiftApi getSwiftApi() {
        return swiftApi;
    }

    CasperApiControl getCasperApiControl() {
        return casperApiControl;
    }

    CasperVIPRouter getCasperVIPRouter() {
        return casperVIPRouter;
    }

    public CountingHandler.MaximumContentLength getMaximumContentLength() {
        return maximumContentLength;
    }

    public MeterWriter getMeterWriter() {
        return meterWriter;
    }

    public AuditLogger getAuditLogger() {
        return auditLogger;
    }

    public ServiceLogWriter getServiceLogWriter() {
        return serviceLogWriter;
    }

}
