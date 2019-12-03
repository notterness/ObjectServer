package com.oracle.athena.webserver.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.pic.casper.common.certs.CertificateStore;
import com.oracle.pic.casper.common.certs.CertificateStoreFactory;
import com.oracle.pic.casper.common.config.ConfigConstants;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.common.config.v2.ResourceLimitConfiguration;
import com.oracle.pic.casper.common.config.v2.TrafficControllerConfiguration;
import com.oracle.pic.casper.common.encryption.store.InMemorySecretStore;
import com.oracle.pic.casper.common.encryption.store.Secret;
import com.oracle.pic.casper.common.encryption.store.SecretStore;
import com.oracle.pic.casper.common.encryption.store.SecretStoreFactory;
import com.oracle.pic.casper.common.encryption.store.Secrets;
import com.oracle.pic.casper.common.guice.TracingModule;
import com.oracle.pic.casper.common.host.StaticHostInfoProvider;
import com.oracle.pic.casper.common.host.impl.DefaultStaticHostInfoProvider;
import com.oracle.pic.casper.common.json.JacksonSerDe;
import com.oracle.pic.casper.common.metrics.MetricScopeWriter;
import com.oracle.pic.casper.common.metrics.MetricsInitializer;
import com.oracle.pic.casper.common.metrics.Slf4jMetricScopeWriter;
import com.oracle.pic.casper.common.metrics.T2MetricsInitializer;
import com.oracle.pic.casper.common.monitoring.MonitoringMetricsReporter;
import com.oracle.pic.casper.common.util.Log4jUtil;
import com.oracle.pic.casper.objectmeta.oracle.OracleMetrics;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.eventing.DiskEventPublisherImpl;
import com.oracle.pic.casper.webserver.api.eventing.EventPublisher;
import com.oracle.pic.casper.webserver.api.eventing.NoOpEventPublisher;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.server.MdsClients;
import com.oracle.pic.casper.webserver.server.WebServerAuths;
import com.oracle.pic.casper.webserver.server.WebServerClients;
import com.oracle.pic.casper.webserver.server.WebServerEncryption;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import com.oracle.pic.casper.webserver.traffic.KeyValueStoreUpdater;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficControllerUpdater;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.util.ConfigValueImpl;
import com.oracle.pic.casper.webserver.util.MaximumContentLengthUpdater;
import com.oracle.pic.casper.webserver.util.ObjectMappers;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.events.EventsIngestionClient;
import com.uber.jaeger.Tracer;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// FIXME: We need to be completely independent of the casper.webserver package
public class WebServer {

    private static final Logger LOG = LoggerFactory.getLogger(WebServer.class);
    /*
     ** This needs to be large enough to account for all the connections associated with either the
     **   SSL server or the non-SSL server. Currently, the expectation is to support thousands of
     **   simultaneous connections, so using 10,000 for now.
     */
    private static final int WEB_SERVER_ID_OFFSET = 10000;

    private final MetricsInitializer metricsInitializer;
    private final CasperConfig config;
    private final WebServerClients clients;
    private final WebServerAuths auths;
    private final EventPublisher eventPublisher;
    private final KeyValueStoreUpdater keyValueStoreUpdater;
    private final MdsClients mdsClients;
    private final MonitoringMetricsReporter metricsReporter;
    private final Tracer tracer;
    private final ServerChannelLayer http;
    private final ServerChannelLayer https;
    private final MemoryManager memoryManager;
    private final int webServerClientIdBase;


    public WebServer(final WebServerFlavor flavor, final CasperConfig config) {
        this(flavor, ServerChannelLayer.DEFAULT_CLIENT_ID, config);
    }

    public WebServer(final WebServerFlavor flavor, final int serverClientId, final CasperConfig config) {
        this(flavor, ServerChannelLayer.BASE_TCP_PORT, serverClientId, config);
    }

    public WebServer(
            final WebServerFlavor flavor,
            final int listenPort,
            final int serverClientId,
            final CasperConfig config) {
        this.config = config;
        this.webServerClientIdBase = serverClientId * WEB_SERVER_ID_OFFSET;
        int sslWebServerClientIdBase = (serverClientId + 1) * WEB_SERVER_ID_OFFSET;

        /*
            In order to allow the -Dregion=XXX system property to be optional, we need to set the system property if it
            is not present to whatever this WebServer instance is configured to be so that the metrics can be
            initialized appropriately.
         */
        if (!ConfigRegion.tryFromSystemProperty().isPresent()) {
            System.setProperty(ConfigConstants.REGION, config.getRegion().name());
        }

        // FIXME: Update the app-name and hostType as appropriate
        final StaticHostInfoProvider staticHostInfoProvider = new DefaultStaticHostInfoProvider("web-server", "docker");
        tracer = new TracingModule().tracer(config.getCommonConfigurations().getTracingConfiguration(),
                staticHostInfoProvider);

        // TODO CA: The T2 metrics initialize to the casper-common config files which report as casper metrics, determine if this is desired behavior
        metricsInitializer = new T2MetricsInitializer(
                config.getCommonConfigurations().getT2Configuration().isEnabled(),
                config.getCommonConfigurations().getT2Configuration().getProject(),
                config.getCommonConfigurations().getT2Configuration().getEndpoint().getUrl(),
                staticHostInfoProvider);

        //Use the real secret store if we are either STANDARD or we are using real auth
        SecretStore secretStore;
        if (flavor == WebServerFlavor.STANDARD || !WebServerAuths.skipAuth(flavor, config.getRegion())) {
            secretStore = SecretStoreFactory.getSecretStore(
                    config.getRegion(),
                    config.getAvailabilityDomain(),
                    config.getSecretStoreConfiguration(),
                    config.getSecretServiceV2Configuration());
        } else {
            InMemorySecretStore inMemorySecretStore = new InMemorySecretStore();
            inMemorySecretStore.addSecret(new Secret(RandomStringUtils.randomAlphanumeric(32),
                    Secrets.ENCRYPTION_MASTER_KEY, "1"));
            secretStore = inMemorySecretStore;
        }

        final ResourceLimitConfiguration rlConfig =
                config.getWebServerConfigurations().getResourceLimitConfiguration();
        final ResourceLimiter resourceLimiter = new ResourceLimiter(rlConfig);
        final ObjectMapper mapper = ObjectMappers.createCasperObjectMapper();
        final JacksonSerDe jacksonSerDe = new JacksonSerDe(mapper);
        final MetricScopeWriter metricScopeWriter = new Slf4jMetricScopeWriter(jacksonSerDe);
        mdsClients = new MdsClients(flavor, config);

        // construct the certStore used for authentication
        final CertificateStore certStore = CertificateStoreFactory.create(config.getRegion(), secretStore);
        WebServerEncryption encryption = new WebServerEncryption(flavor, config, secretStore, certStore, mdsClients);
        /*
            FIXME: The last value is the vertx context which is a bugbear of malfeasance in our webserver.

            The WebServerClients right now assume that the Vert.x context is setup and used. This means that which
            endpoints are being called, and all manner of ReST/HTTP headers. The WebServerClients will need to be
            rewritten for our server.
         */
        clients = new WebServerClients(flavor, config, metricScopeWriter, certStore, null);
        metricsReporter = new MonitoringMetricsReporter(
                clients.getTelemetryClient(), config.getPublicTelemetryConfiguration(), true);
        this.auths = new WebServerAuths(flavor, config, clients,
                mdsClients, mapper, certStore,
                encryption.getDecidingManagementServiceProvider(), resourceLimiter);

        // construct the TrafficController
        // FIXME: this likely needs to be passed into the http and https ServerChannelLayers
        final TrafficControllerConfiguration tcConfig =
                config.getWebServerConfigurations().getTrafficControllerConfiguration();
        TrafficController controller = new TrafficController(
                tcConfig.isShadowMode(),
                tcConfig.isTenantLevelShadowMode(),
                tcConfig.getMaxBandwidth(),
                new TrafficRecorder.Configuration(
                        tcConfig.getAverageBandwidth(),
                        tcConfig.getAverageOverhead(),
                        tcConfig.getAlpha(),
                        tcConfig.getSmallRequestOverheadPercentage(),
                        tcConfig.getUpdateInterval(),
                        tcConfig.getRequestSamplePercentage()
                )
        );

        final ConfigValueImpl casper6145FixDisabled = new ConfigValueImpl("Casper6145FixDisabled");
        keyValueStoreUpdater = new KeyValueStoreUpdater(
                mdsClients.getOperatorMdsExecutor(),
                mdsClients.getOperatorDeadline(),
                ImmutableList.of(
                        new TrafficControllerUpdater(controller),
                        // TODO: Track down how the CasperApiX handles changing this value (prev: WebServerApis.getMaximumContentLength())
                        new MaximumContentLengthUpdater(new CountingHandler.MaximumContentLength(
                                config.getWebServerConfigurations().getApiConfiguration().getMaxContentLength())),
                        casper6145FixDisabled
                ),
                config.getWebServerConfigurations().getApiConfiguration().getKeyValueStorePollInterval());

        // FIXME: This needs to be piped into whatever handlers do the actual PUT
        final EventsIngestionClient eventsClient = clients.getEventsClient();
        eventPublisher = eventsClient != null ?
                new DiskEventPublisherImpl(config.getEventServiceConfiguration(), eventsClient, jacksonSerDe,
                        auths.getLimits()) :
                new NoOpEventPublisher();

        memoryManager = new MemoryManager(flavor);

        /*
         ** For INTEGRATION_TESTS the number of worker threads is set to 1.
         **
         ** For INTEGRATION_TESTS the number of connections per worker thread is set to 2 to insure that the
         **   system runs out of connections and can be tested for the out of connections handling.
         **
         ** TODO: Need to add some statistics so the thread usage and number of connections can be tracked for
         **   later performance tunning.
         */
        int numConnectionsPerWorkerThread;
        int numWorkerThreads;
        if (flavor == WebServerFlavor.STANDARD) {
            numConnectionsPerWorkerThread = 100;
            numWorkerThreads = 10;
        } else {
            numConnectionsPerWorkerThread = 2;
            numWorkerThreads = 1;
        }
        ServerLoadBalancer serverWorkHandler = new ServerLoadBalancer(flavor, auths, numConnectionsPerWorkerThread, numWorkerThreads,
                memoryManager, webServerClientIdBase);
        ServerSSLLoadBalancer sslServerWorkHandler = new ServerSSLLoadBalancer(flavor, auths, numConnectionsPerWorkerThread, numWorkerThreads,
                memoryManager, sslWebServerClientIdBase);

        http = new ServerChannelLayer(serverWorkHandler, listenPort,
                webServerClientIdBase);
        https = new ServerChannelLayer(sslServerWorkHandler, listenPort + ServerChannelLayer.HTTPS_PORT_OFFSET,
                sslWebServerClientIdBase);
    }

    public void start() {
        http.start();
        https.start();

        WebServerMetrics.init();
        OracleMetrics.init();

        metricsInitializer.start();
        metricsReporter.start();
        clients.start(config.getWebServerConfigurations().getApiConfiguration());
        mdsClients.start();
        eventPublisher.start();
        keyValueStoreUpdater.start();
    }

    public void stop() {
        try {
            metricsInitializer.stop();
        } catch (Exception ex) {
            LOG.warn("Failed to stop the metrics initializer", ex);
        }

        try {
            metricsReporter.stop();
        } catch (Exception ex) {
            LOG.warn("Failed to stop the MonitoringMetricsReporter", ex);
        }

        tracer.close();
        clients.stop();
        auths.close();
        mdsClients.stop();

        // stop the servers
        http.stop();
        https.stop();

        /*
         ** Verify that the MemoryManger has all of its memory back in the free pools
         */
        if (memoryManager.verifyMemoryPools("ServerChannelLayer")) {
            System.out.println("ServerChannelLayer[" + webServerClientIdBase + "] Memory Verification All Passed");
        }
        try {
            Log4jUtil.shutdown();
        } catch (Exception ex) {
            LOG.warn("Failed to shutdown Log4j", ex);
        }

        eventPublisher.stopRunning();
        try {
            eventPublisher.join(2000);
        } catch (InterruptedException ex) {
            LOG.warn("Failed to shutdown event publisher", ex);
        }

        try {
            keyValueStoreUpdater.stop();
        } catch (InterruptedException ex) {
            LOG.warn("Failed to shutdown KeyValueStoreUpdater", ex);
        }
    }

}

