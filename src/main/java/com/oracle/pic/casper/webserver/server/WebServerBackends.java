package com.oracle.pic.casper.webserver.server;

import com.google.common.base.Ticker;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.json.JacksonSerDe;
import com.oracle.pic.casper.common.metrics.MetricScopeWriter;
import com.oracle.pic.casper.objectmeta.CasperTransactionIdFactory;
import com.oracle.pic.casper.objectmeta.CasperTransactionIdFactoryImpl;
import com.oracle.pic.casper.webserver.api.usage.QuotaEvaluator;
import com.oracle.pic.casper.webserver.api.auth.ParAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.ParAuthorizerDecorator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.backend.GetUsageBackend;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.backend.WorkRequestBackend;
import com.oracle.pic.casper.webserver.api.eventing.EventPublisher;
import com.oracle.pic.casper.webserver.api.pars.PreAuthenticatedRequestBackend;
import com.oracle.pic.casper.webserver.api.usage.UsageCache;

import java.time.Clock;


/**
 * A collection of backend classes used by the web server to handle API requests.
 */
public final class WebServerBackends {
    private final Backend backend;
    private final Backend parBackend;
    private final PutObjectBackend putObjBackend;
    private final PutObjectBackend parPutObjBackend;
    private final GetObjectBackend getObjBackend;
    private final GetObjectBackend parGetObjBackend;
    private final TenantBackend tenantBackend;
    private final BucketBackend bucketBackend;
    private final ParAuthenticator parAuthenticator;
    private final PreAuthenticatedRequestBackend preAuthenticatedRequestBackend;
    private final WorkRequestBackend workRequestBackend;
    private final GetUsageBackend getUsageBackend;

    WebServerBackends(
        CasperConfig config,
        WebServerClients clients,
        MdsClients mdsClients,
        DecidingKeyManagementService kms,
        WebServerAuths auths,
        JacksonSerDe jacksonSerDe,
        EventPublisher eventPublisher,
        Clock clock,
        Ticker ticker,
        MetricScopeWriter metricScopeWriter) {

        CasperTransactionIdFactory txnIdFactory = new CasperTransactionIdFactoryImpl();
        ParAuthorizerDecorator parAuthorizer = new ParAuthorizerDecorator(auths.getAuthorizer(), clock);

        tenantBackend = new TenantBackend(
                mdsClients,
                config.getMeteringConfigurations().getCompartmentCacheConfiguration(),
                config.getMdsClientConfiguration());

        final UsageCache usageCache = new UsageCache(
                config.getWebServerConfigurations().getUsageConfiguration(),
                mdsClients.getOperatorMdsExecutor(),
                mdsClients.getOperatorDeadline());

        // Create a quota evaluator with 100ms timeout to complete evaluation
        final QuotaEvaluator quotaEvaluator =
                new QuotaEvaluator(auths.getMetadataClient(), auths.getLimits(),
                        usageCache, config.getRegion().getFullName(),
                        config.getLimitsClientConfiguration().getQuotaConfiguration().getTimeout(),
                        config.getLimitsClientConfiguration().getQuotaConfiguration().getThreadCount(),
                        config.getLimitsClientConfiguration().getQuotaConfiguration().getQueueSize());

        bucketBackend = new BucketBackend(
            tenantBackend,
            auths.getAuthorizer(),
            jacksonSerDe,
            kms,
            config.getMeteringConfigurations().getCompartmentCacheConfiguration(),
            config.getCommonConfigurations().getServicePrincipalConfiguration(),
            auths.getLimits(),
            quotaEvaluator,
            config.getWebServerConfigurations().getUsageConfiguration().isQuotaEvaluationEnabled(),
            mdsClients,
            config.getWorkRequestServiceConfigurations().getMaxRequestsPerTenant(),
            config.getWebServerConfigurations().getBucketCacheConfiguration(),
            ticker,
            metricScopeWriter);

        backend = new Backend(
            mdsClients,
            auths.getAuthorizer(),
            config.getWebServerConfigurations().getApiConfiguration(),
            jacksonSerDe,
            kms,
            bucketBackend,
            config.getWebServerConfigurations().getArchiveConfiguration(),
            eventPublisher,
            auths.getMetadataClient(),
            config.getCommonConfigurations().getServicePrincipalConfiguration().getTenantId());

        parBackend = new Backend(
            mdsClients,
            parAuthorizer,
            config.getWebServerConfigurations().getApiConfiguration(),
            jacksonSerDe,
            kms,
            bucketBackend,
            config.getWebServerConfigurations().getArchiveConfiguration(),
            eventPublisher,
            auths.getMetadataClient(),
            config.getCommonConfigurations().getServicePrincipalConfiguration().getTenantId());

        getObjBackend = new GetObjectBackend(
            backend,
            clients.getAthenaVolumeStorageClient(),
            clients.getVolumeMetadataCache(),
            auths.getAuthorizer(),
            bucketBackend,
            kms);

        // override authorizer for PARs
        parGetObjBackend = new GetObjectBackend(
            backend,
            clients.getAthenaVolumeStorageClient(),
            clients.getVolumeMetadataCache(),
            parAuthorizer,
            bucketBackend,
            kms);

        putObjBackend = new PutObjectBackend(
            mdsClients,
            clients.getVolumeAndVonPicker(),
            clients.getVolumeStorageClient(),
            clients.getVolumeMetadataCache(),
            txnIdFactory,
            auths.getAuthorizer(),
            config.getWebServerConfigurations().getApiConfiguration(),
            config.getWebServerConfigurations().isMetersDebugEnabled(),
            jacksonSerDe,
            kms,
            bucketBackend,
            config.getWebServerConfigurations().getArchiveConfiguration(),
            eventPublisher,
            auths.getLimits(),
            usageCache,
            auths.getMetadataClient(),
            quotaEvaluator,
            config.getWebServerConfigurations().getUsageConfiguration().isQuotaEvaluationEnabled(),
            config.getWebServerConfigurations().isSingleChunkOptimizationEnabled());

        // override authorizer for PARs
        parPutObjBackend = new PutObjectBackend(
            mdsClients,
            clients.getVolumeAndVonPicker(),
            clients.getVolumeStorageClient(),
            clients.getVolumeMetadataCache(),
            txnIdFactory,
            parAuthorizer,
            config.getWebServerConfigurations().getApiConfiguration(),
            config.getWebServerConfigurations().isMetersDebugEnabled(),
            jacksonSerDe,
            kms,
            bucketBackend,
            config.getWebServerConfigurations().getArchiveConfiguration(),
            eventPublisher,
            auths.getLimits(),
            usageCache,
            auths.getMetadataClient(),
            quotaEvaluator,
            config.getWebServerConfigurations().getUsageConfiguration().isQuotaEvaluationEnabled(),
            config.getWebServerConfigurations().isSingleChunkOptimizationEnabled());

        preAuthenticatedRequestBackend = new PreAuthenticatedRequestBackend(
            tenantBackend,
            bucketBackend,
            backend,
            getObjBackend,
            putObjBackend,
            auths.getAuthorizer(),
            jacksonSerDe,
            kms);

        // even the default authz that the PAR backend does is to be overriden by the authenticator,
        // since in this case the PAR backend is being used to look up the PAR for authenticating
        // the user of the PAR
        parAuthenticator = new ParAuthenticator(new PreAuthenticatedRequestBackend(
            tenantBackend,
            bucketBackend,
            backend,
            getObjBackend,
            putObjBackend,
            auths.getAuthorizerPassThrough(),
            jacksonSerDe,
            kms));

        workRequestBackend = new WorkRequestBackend(
            jacksonSerDe,
            mdsClients,
            auths.getAuthorizer(),
            bucketBackend,
            backend,
            auths.getResourceControlledServicePrincipalClient(),
            kms,
            auths.getLimits());

        getUsageBackend = new GetUsageBackend(usageCache, auths.getAuthorizer(), auths.getMetadataClient());
    }

    public Backend getBackend() {
        return backend;
    }

    public Backend getParBackend() {
        return parBackend;
    }

    PutObjectBackend getPutObjBackend() {
        return putObjBackend;
    }

    PutObjectBackend getParPutObjBackend() {
        return parPutObjBackend;
    }

    GetObjectBackend getParGetObjBackend() {
        return parGetObjBackend;
    }

    GetObjectBackend getGetObjBackend() {
        return getObjBackend;
    }

    public TenantBackend getTenantBackend() {
        return tenantBackend;
    }

    public BucketBackend getBucketBackend() {
        return bucketBackend;
    }

    public ParAuthenticator getParAuthenticator() {
        return parAuthenticator;
    }

    public PreAuthenticatedRequestBackend getPreAuthenticatedRequestBackend() {
        return preAuthenticatedRequestBackend;
    }

    WorkRequestBackend getWorkRequestBackend() {
        return workRequestBackend;
    }

    public GetUsageBackend getGetUsageBackend() {
        return getUsageBackend;
    }
}
