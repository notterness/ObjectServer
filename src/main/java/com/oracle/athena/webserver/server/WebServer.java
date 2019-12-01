package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.pic.casper.common.certs.CertificateStore;
import com.oracle.pic.casper.common.certs.CertificateStoreFactory;
import com.oracle.pic.casper.common.config.v2.ResourceLimitConfiguration;
import com.oracle.pic.casper.common.config.v2.TrafficControllerConfiguration;
import com.oracle.pic.casper.common.encryption.store.InMemorySecretStore;
import com.oracle.pic.casper.common.encryption.store.Secret;
import com.oracle.pic.casper.common.encryption.store.SecretStoreFactory;
import com.oracle.pic.casper.common.encryption.store.Secrets;
import com.oracle.pic.casper.common.guice.TracingModule;
import com.oracle.pic.casper.common.host.StaticHostInfoProvider;
import com.oracle.pic.casper.common.host.impl.DefaultStaticHostInfoProvider;
import com.oracle.pic.casper.common.json.JacksonSerDe;
import com.oracle.pic.casper.common.metrics.T2MetricsInitializer;
import com.oracle.pic.casper.common.util.AdjustableClock;
import com.oracle.pic.casper.webserver.api.eventing.DiskEventPublisherImpl;
import com.oracle.pic.casper.webserver.api.eventing.NoOpEventPublisher;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.server.*;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.util.ObjectMappers;
import com.oracle.pic.events.EventsIngestionClient;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Clock;

public class WebServer {

    /*
    ** This needs to be large enough to account for all the connections associated with either the
    **   SSL server or the non-SSL server. Currently, the expectation is to support thousands of
    **   simultaneous connections, so using 10,000 for now.
     */
    public static final int WEB_SERVER_ID_OFFSET = 10000;

    /*
    private final MetricsInitializer metricsInitializer;

    private final ObjectMapper mapper;
    private final AdjustableClock clock;

    private final CasperConfig config;
    private final WebServerClients clients;
    private final WebServerEncryption encryption;
    private final WebServerAuths auths;
    private final WebServerBackends backends;
    private final WebServerAPIs apis;
    private final EventPublisher eventPublisher;
    private final SecretStore secretStore;
    private final KeyValueStoreUpdater keyValueStoreUpdater;
    private final TrafficController controller;
    private final MdsClients mdsClients;
    private final ResourceLimiter resourceLimiter;
    private final MonitoringMetricsReporter metricsReporter;

    private final Vertx vertx;
    private final Tracer tracer;

     */

    private ServerChannelLayer http_server;
    private ServerChannelLayer https_server;
    private ServerLoadBalancer serverWorkHandler;
    private ServerSSLLoadBalancer sslServerWorkHandler;
    private MemoryManager memoryManager;
    private final int webServerClientIdBase;
    private final int sslWebServerClientIdBase;


    public WebServer(WebServerFlavor flavor) {
        this(flavor, ServerChannelLayer.DEFAULT_CLIENT_ID);
    }

    public WebServer(WebServerFlavor flavor, int serverClientId) {
        this(flavor, ServerChannelLayer.BASE_TCP_PORT, serverClientId);
    }

    public WebServer(WebServerFlavor flavor, int listenPort, int serverClientId) {
        this.webServerClientIdBase = serverClientId * WEB_SERVER_ID_OFFSET;
        this.sslWebServerClientIdBase = (serverClientId + 1) * WEB_SERVER_ID_OFFSET;

        /*
        ** The following are required to allow the WebServer to perform required operations
        *
        GlobalSystemPropertiesConfigurer.configure();

        final ConfigRegion region = ConfigRegion.fromSystemProperty();
        final ConfigAvailabilityDomain ad = ConfigAvailabilityDomain.tryFromSystemProperty()
                .orElse(ConfigAvailabilityDomain.GLOBAL);

        final CasperConfig config = CasperConfig.create(region, ad);

         mapper = ObjectMappers.createCasperObjectMapper();
        final JacksonSerDe jacksonSerDe = new JacksonSerDe(mapper);

        final StaticHostInfoProvider staticHostInfoProvider = new DefaultStaticHostInfoProvider("web-server", "docker");

        clock = new AdjustableClock(Clock.systemUTC());
        tracer = new TracingModule().tracer(config.getCommonConfigurations().getTracingConfiguration(),
                staticHostInfoProvider);

        vertx = configureVertx(config.getWebServerConfigurations().getVertxConfiguration());

        metricsInitializer = new T2MetricsInitializer(
                config.getCommonConfigurations().getT2Configuration().isEnabled(),
                config.getCommonConfigurations().getT2Configuration().getProject(),
                config.getCommonConfigurations().getT2Configuration().getEndpoint().getUrl(),
                staticHostInfoProvider);

        //Use the real secret store if we are either STANDARD or we are using real auth
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

        final CertificateStore certStore = CertificateStoreFactory.create(config.getRegion(), secretStore);

        mdsClients = new MdsClients(flavor, config);
        encryption = new WebServerEncryption(flavor, config, secretStore, certStore, mdsClients);
        clients = new WebServerClients(flavor, config, metricScopeWriter, certStore, vertx);

        final TrafficControllerConfiguration tcConfig =
                config.getWebServerConfigurations().getTrafficControllerConfiguration();
        controller = new TrafficController(
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

        final ResourceLimitConfiguration rlConfig =
                config.getWebServerConfigurations().getResourceLimitConfiguration();
        resourceLimiter = new ResourceLimiter(rlConfig);

        auths = new WebServerAuths(flavor, config, clients,
                mdsClients, mapper, certStore,
                encryption.getDecidingManagementServiceProvider(), resourceLimiter);
        final EventsIngestionClient eventsClient = clients.getEventsClient();
        eventPublisher = eventsClient != null ?
                new DiskEventPublisherImpl(config.getEventServiceConfiguration(), eventsClient, jacksonSerDe,
                        auths.getLimits()) :
                new NoOpEventPublisher();
        */

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
        if (flavor == WebServerFlavor.INTEGRATION_TESTS) {
            numConnectionsPerWorkerThread = 2;
            numWorkerThreads = 1;
        } else {
            numConnectionsPerWorkerThread = 100;
            numWorkerThreads = 10;
        }
        serverWorkHandler = new ServerLoadBalancer(flavor, numConnectionsPerWorkerThread, numWorkerThreads,
                memoryManager, webServerClientIdBase);
        sslServerWorkHandler = new ServerSSLLoadBalancer(flavor, numConnectionsPerWorkerThread, numWorkerThreads,
                memoryManager, sslWebServerClientIdBase);

        http_server = new ServerChannelLayer(serverWorkHandler, listenPort,
                webServerClientIdBase);
        https_server = new ServerChannelLayer(sslServerWorkHandler, listenPort + ServerChannelLayer.HTTPS_PORT_OFFSET,
                sslWebServerClientIdBase);
    }

    public void start() {
        http_server.start();
        https_server.start();
    }

    public void stop() {
        http_server.stop();
        https_server.stop();

        /*
         ** Verify that the MemoryManger has all of its memory back in the free pools
         */
        if (memoryManager.verifyMemoryPools("ServerChannelLayer")) {
            System.out.println("ServerChannelLayer[" + webServerClientIdBase + "] Memory Verification All Passed");
        }
    }

}

