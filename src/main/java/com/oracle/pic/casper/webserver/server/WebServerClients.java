package com.oracle.pic.casper.webserver.server;

import com.google.inject.Provider;
import com.oracle.bmc.ClientConfiguration;
import com.oracle.pic.casper.common.certs.CertificateStore;
import com.oracle.pic.casper.common.clients.CustomTlsConfigurator;
import com.oracle.pic.casper.common.clients.FakeAuthDetailsProvider;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.common.config.v2.MtlsClientConfiguration;
import com.oracle.pic.casper.common.config.v2.ServiceGatewayConfiguration;
import com.oracle.pic.casper.common.config.v2.VolumeStorageClientConfiguration;
import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.metrics.MetricScopeWriter;
import com.oracle.pic.casper.common.monitoring.FakeTelemetryClient;
import com.oracle.pic.casper.common.monitoring.TelemetryClientFactory;
import com.oracle.pic.casper.common.scheduling.vertx.PerHostVertxContextProvider;
import com.oracle.pic.casper.common.scheduling.vertx.PerHostVertxContextProviderImpl;
import com.oracle.pic.casper.lifecycle.FakeLifecycleEngineClient;
import com.oracle.pic.casper.lifecycle.LifecycleEngineClient;
import com.oracle.pic.casper.lifecycle.LifecycleEngineClientImpl;
import com.oracle.pic.casper.storageclient.AthenaVolumeStorageClient;
import com.oracle.pic.casper.storageclient.HttpClientProvider;
import com.oracle.pic.casper.storageclient.VolumeStorageClient;
import com.oracle.pic.casper.storageclient.core.AthenaRequestHandlerFactory;
import com.oracle.pic.casper.storageclient.core.RequestHandlerFactory;
import com.oracle.pic.casper.storageclient.core.replicated.AthenaReplicatedRequestHandlerFactory;
import com.oracle.pic.casper.storageclient.core.replicated.ReplicatedRequestHandlerFactory;
import com.oracle.pic.casper.storageclient.impl.AthenaInMemoryVolumeStorageClient;
import com.oracle.pic.casper.storageclient.impl.AthenaVolumeStorageClientImpl;
import com.oracle.pic.casper.storageclient.impl.InMemoryVolumeStorageClient;
import com.oracle.pic.casper.storageclient.impl.VolumeStorageClientImpl;
import com.oracle.pic.casper.volumemeta.VolumeMetadataClientCache;
import com.oracle.pic.casper.volumemeta.impl.CacheableVolumeMetadataProviderRemoteClient;
import com.oracle.pic.casper.volumemeta.impl.CacheableVolumeMetadataProviderRemoteClientImpl;
import com.oracle.pic.casper.volumemeta.impl.VolumeMetadataClientCacheImpl;
import com.oracle.pic.casper.volumemeta.util.VolumeMetadataTestHelpers;
import com.oracle.pic.casper.volumemgmt.VolumeAndVonPicker;
import com.oracle.pic.casper.volumemgmt.impl.InMemoryVolumeAndVonPickerImpl;
import com.oracle.pic.casper.volumemgmt.impl.RemoteVolumeAndVonPickerImpl;
import com.oracle.pic.casper.volumeservice.client.VolumeServiceClient;
import com.oracle.pic.casper.volumeservice.client.VolumeServiceClientModule;
import com.oracle.pic.casper.webserver.api.sg.VcnIdProvider;
import com.oracle.pic.casper.webserver.api.sg.VcnIdServiceGatewayClientProvider;
import com.oracle.pic.events.EventsIngestionClient;
import com.oracle.pic.sgw.api.InternalServiceGatewayClient;
import com.oracle.pic.telemetry.api.Telemetry;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;

/**
 * The service clients used by the web server to communicate with external dependencies.
 *
 * This includes the volume metadata cache and volume/VON picker clients, which communicate with the volume service
 * and maintain a local, in-memory cache, as well as clients for the auth data plane and storage servers.
 *
 * A single Apache HTTP client instance is created here, and can be used by any code that wants to make synchronous
 * HTTP requests to external dependencies (it is thread-safe and can handle multiple connections).
 */
public final class WebServerClients {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebServerClients.class);

    private final VolumeMetadataClientCache volumeMetadataCache;
    private final VolumeAndVonPicker volumeAndVonPicker;
    private final AthenaVolumeStorageClient athenaVolumeStorageClient;
    private final VolumeStorageClient volumeStorageClient;
    private final VolumeServiceClient volumeServiceClient;
    private final LifecycleEngineClient lifecycleEngineClient;
    private final EventsIngestionClient eventsClient;
    private final VcnIdProvider vcnIdProvider;
    private final Telemetry telemetryClient;

    public WebServerClients(WebServerFlavor flavor,
                            CasperConfig config,
                            MetricScopeWriter metricScopeWriter,
                            CertificateStore cstore,
                            Vertx vertx) {
        if (flavor == WebServerFlavor.INTEGRATION_TESTS) {
            telemetryClient = new FakeTelemetryClient();
            volumeServiceClient = null;

            volumeMetadataCache = VolumeMetadataTestHelpers.fakeVolumeMetadataClientCache();

            volumeAndVonPicker = new InMemoryVolumeAndVonPickerImpl(volumeMetadataCache);
            athenaVolumeStorageClient = new AthenaInMemoryVolumeStorageClient();
            volumeStorageClient = new InMemoryVolumeStorageClient();
            lifecycleEngineClient = new FakeLifecycleEngineClient();
            eventsClient = null;
            vcnIdProvider = VcnIdProvider.emptyProvider();
        } else {
            telemetryClient = TelemetryClientFactory.create(
                    config.getRegion(),
                    config.getAvailabilityDomain(),
                    config.getPublicTelemetryConfiguration(),
                    config.getCommonConfigurations().getServiceEndpointConfiguration(),
                    config.getCommonConfigurations().getServicePrincipalConfiguration(),
                    cstore);
            volumeServiceClient = VolumeServiceClientModule.getVolumeServiceClient(config.getCommonConfigurations());
            final Provider<VolumeServiceClient> volumeServiceClientProvider = () -> volumeServiceClient;
            final CacheableVolumeMetadataProviderRemoteClient cacheableVolumeMetadataProvider =
                    new CacheableVolumeMetadataProviderRemoteClientImpl(volumeServiceClientProvider);
            volumeMetadataCache = new VolumeMetadataClientCacheImpl(cacheableVolumeMetadataProvider, metricScopeWriter);
            volumeAndVonPicker = new RemoteVolumeAndVonPickerImpl(
                    volumeMetadataCache, volumeServiceClientProvider);

            final VolumeStorageClientConfiguration volumeClientConfig =
                    config.getWebServerConfigurations().getVolumeStorageClientConfiguration();
            final HttpClientProvider httpClientProvider =
                    new HttpClientProvider(vertx, volumeClientConfig.getVertxClientConfiguration());
            final PerHostVertxContextProvider perHostVertxContextProvider =
                    new PerHostVertxContextProviderImpl(vertx);
            perHostVertxContextProvider.initializeVertxContexts();
            final AthenaRequestHandlerFactory athenaRequestHandlerFactory = new AthenaReplicatedRequestHandlerFactory(
                    vertx,  volumeClientConfig, httpClientProvider, perHostVertxContextProvider);
            final RequestHandlerFactory requestHandlerFactory = new ReplicatedRequestHandlerFactory(
                    vertx,  volumeClientConfig, httpClientProvider, perHostVertxContextProvider);
            athenaVolumeStorageClient = new AthenaVolumeStorageClientImpl(athenaRequestHandlerFactory);
            volumeStorageClient = new VolumeStorageClientImpl(requestHandlerFactory);

            final String lifecycleUrl =
                    config.getLifecycleEngineConfiguration().getLifecycleEngineServerEndpoint().getUrl();
            final int lifecyclePort = config.getLifecycleEngineConfiguration().getPort();
            lifecycleEngineClient = new LifecycleEngineClientImpl(lifecycleUrl, lifecyclePort);

            // events client for eventing service, this exists for the demo on 04/20/2018
            final ClientConfiguration clientConfig = ClientConfiguration
                    .builder()
                    .connectionTimeoutMillis(10000)
                    .readTimeoutMillis(90000)
                    .build();
            MtlsClientConfiguration mtlsClientConfiguration =
                    config.getCommonConfigurations().getMtlsClientConfiguration();
            CustomTlsConfigurator configurator = new CustomTlsConfigurator(
                    mtlsClientConfiguration.getClientCertFolder(),
                    mtlsClientConfiguration.getTrustAnchorsFolder(),
                    "");

            eventsClient = config.getEventServiceConfiguration().isEnabled() ?
                    new EventsIngestionClient(FakeAuthDetailsProvider.Instance, clientConfig, configurator) :
                    null;
            if (eventsClient != null) {
                String eventClientEndpoint = config.getEventServiceConfiguration().getEndpoint().getUrl();
                LOGGER.info("Setting event client endpoint to {}", eventClientEndpoint);
                eventsClient.setEndpoint(eventClientEndpoint);
            }

            final ServiceGatewayConfiguration sgwConfig =
                    config.getWebServerConfigurations().getServiceGatewayConfiguration();
            if (sgwConfig.isEnabled()) {
                ClientBuilder builder = ClientBuilder.newBuilder();
                configurator.customizeBuilder(builder);
                vcnIdProvider = new VcnIdServiceGatewayClientProvider(
                        sgwConfig.getRefreshInterval(), new InternalServiceGatewayClient(builder.build(),
                        sgwConfig.getEndpoint().getUri()));
            } else {
                vcnIdProvider = VcnIdProvider.emptyProvider();
            }
        }
    }

    /**
     * Start the background processes that populate caches for the clients. It is not safe to call this method more
     * than once.
     */
    public void start(WebServerConfiguration webServerConfig) {
        volumeMetadataCache.start(webServerConfig.getVolumeMetadataCachePollInterval());
        volumeAndVonPicker.start();
        vcnIdProvider.start();
    }

    /**
     * Stop the background processes and close all open clients. It is safe to call this method without calling start,
     * and to call it more than once. This method will never throw an exception.
     */
    public void stop() {
        try {
            volumeMetadataCache.close();
        } catch (Exception ex) {
            LOGGER.warn("Failed to close the volume metadata cache", ex);
        }

        try {
            volumeAndVonPicker.stop();
        } catch (Exception ex) {
            LOGGER.warn("Failed to stop the volume and VON picker", ex);
        }

        if (volumeServiceClient != null) {
            try {
                volumeServiceClient.close();
            } catch (Exception ex) {
                LOGGER.warn("Failed to close the volume service client", ex);
            }
        }

        try {
            vcnIdProvider.stop();
        } catch (Exception ex) {
            LOGGER.warn("Failed to stop the VCN ID provider", ex);
        }
    }

    public Telemetry getTelemetryClient() {
        return telemetryClient;
    }

    public VolumeMetadataClientCache getVolumeMetadataCache() {
        return volumeMetadataCache;
    }

    public VolumeAndVonPicker getVolumeAndVonPicker() {
        return volumeAndVonPicker;
    }

    public VolumeStorageClient getVolumeStorageClient() {
        return volumeStorageClient;
    }
    public AthenaVolumeStorageClient getAthenaVolumeStorageClient() {
        return athenaVolumeStorageClient;
    }

    public LifecycleEngineClient getLifecycleEngineClient() {
        return lifecycleEngineClient;
    }

    public EventsIngestionClient getEventsClient() {
        return eventsClient;
    }

    public VcnIdProvider getVcnIdProvider() {
        return vcnIdProvider;
    }
}
