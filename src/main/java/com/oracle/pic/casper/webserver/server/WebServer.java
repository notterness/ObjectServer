package com.oracle.pic.casper.webserver.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.certs.CertificateStore;
import com.oracle.pic.casper.common.certs.CertificateStoreFactory;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.common.config.v2.ResourceLimitConfiguration;
import com.oracle.pic.casper.common.config.v2.TrafficControllerConfiguration;
import com.oracle.pic.casper.common.config.v2.VertxConfiguration;
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
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.metrics.MetricsInitializer;
import com.oracle.pic.casper.common.metrics.Slf4jMetricScopeWriter;
import com.oracle.pic.casper.common.metrics.T2MetricsInitializer;
import com.oracle.pic.casper.common.monitoring.MonitoringMetricsReporter;
import com.oracle.pic.casper.common.resourcecontrol.ResourceControlTestHook;
import com.oracle.pic.casper.common.util.Log4jUtil;
import com.oracle.pic.casper.objectmeta.oracle.OracleMetrics;
import com.oracle.pic.casper.webserver.api.eventing.DiskEventPublisherImpl;
import com.oracle.pic.casper.webserver.api.eventing.EventPublisher;
import com.oracle.pic.casper.webserver.api.eventing.NoOpEventPublisher;
import com.oracle.pic.casper.webserver.api.v2.ObjectResource;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.traffic.KeyValueStoreUpdater;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficControllerUpdater;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.common.util.AdjustableClock;
import com.oracle.pic.casper.webserver.util.CommonUtils;
import com.oracle.pic.casper.webserver.util.ConfigValueImpl;
import com.oracle.pic.casper.webserver.util.MaximumContentLengthUpdater;
import com.oracle.pic.casper.webserver.util.ObjectMappers;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.events.EventsIngestionClient;
import com.oracle.pic.telemetry.commons.metrics.model.MetricName;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.uber.jaeger.Tracer;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.dns.AddressResolverOptions;
import org.apache.commons.lang3.RandomStringUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ext.RuntimeDelegate;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The Casper web server, including all APIs (v1, v2, Swift, S3, Archive, Control).
 */
public final class WebServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebServer.class);

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

    private String deploymentId;

    public WebServer(WebServerFlavor flavor, CasperConfig config) {
        this(flavor, config, new Slf4jMetricScopeWriter(new JacksonSerDe(ObjectMappers.createCasperObjectMapper())));
    }

    public WebServer(WebServerFlavor flavor, CasperConfig config, MetricScopeWriter metricScopeWriter) {
        this.config = config;

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
        backends = new WebServerBackends(config, clients, mdsClients,
                encryption.getDecidingManagementServiceProvider(), auths, jacksonSerDe,
                eventPublisher, clock, CommonUtils.getTicker(flavor), metricScopeWriter);

        // Config values that will be read from the Key-Value store in the
        // operator DB.
        final ConfigValueImpl casper6145FixDisabled = new ConfigValueImpl("Casper6145FixDisabled");

        metricsReporter = new MonitoringMetricsReporter(
                clients.getTelemetryClient(), config.getPublicTelemetryConfiguration(), true);

        apis = new WebServerAPIs(
                flavor,
                config,
                clients,
                auths,
                backends,
                mapper,
                jacksonSerDe,
                clock,
                metricScopeWriter,
                tracer,
                encryption.getDecidingManagementServiceProvider(),
                controller,
                metricsReporter,
                casper6145FixDisabled);

        keyValueStoreUpdater = new KeyValueStoreUpdater(
                mdsClients.getOperatorMdsExecutor(),
                mdsClients.getOperatorDeadline(),
                ImmutableList.of(
                        new TrafficControllerUpdater(controller),
                        new MaximumContentLengthUpdater(apis.getMaximumContentLength()),
                        casper6145FixDisabled
                ),
                config.getWebServerConfigurations().getApiConfiguration().getKeyValueStorePollInterval());

        vertx.registerVerticleFactory(new WebServerVerticle.WebServerVerticleFactory(
                config.getWebServerConfigurations().getApiConfiguration(), apis, controller, secretStore));
        deploymentId = null;
    }

    /**
     * Starts the web server, metrics reporting, database migrations and all the APIs and their ports.
     */
    public void start() {
        WebServerMetrics.init();
        OracleMetrics.init();

        metricsInitializer.start();
        metricsReporter.start();
        clients.start(config.getWebServerConfigurations().getApiConfiguration());
        mdsClients.start();


        // START JERSEY HERE
        final ResourceConfig rc = new ResourceConfig();
        final TrafficRecorder recorder = new TrafficRecorder(controller.getTrafficRecorderConfiguration());
        recorder.setBandwidthAndConcurrencyGagues();

        rc.register(new ObjectResource(config, controller, new JacksonSerDe(mapper), auths, recorder,
                backends.getBucketBackend(), mdsClients, clients.getVolumeAndVonPicker(), encryption.getDecidingManagementServiceProvider(),
                clients.getVolumeMetadataCache(), clients.getAthenaVolumeStorageClient()));

        HttpHandler h = RuntimeDelegate.getInstance().createEndpoint(rc, HttpHandler.class);
        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress(9090), 0);
        } catch (IOException e) {
            System.out.println("Failed to create http server");
            return;
        }

        HttpContext httpContext = server.createContext("/", h);
        new Thread(() -> {
            server.start();
        }).start();
        final DeploymentOptions options = new DeploymentOptions()
                .setInstances(Runtime.getRuntime().availableProcessors() * 2);
        final CompletableFuture<String> cf = new CompletableFuture<>();
        vertx.deployVerticle(WebServerVerticle.VERTICLE_NAME, options, result -> {
            if (result.failed()) {
                cf.completeExceptionally(result.cause());
            } else {
                cf.complete(result.result());
            }
        });

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }

        eventPublisher.start();
        keyValueStoreUpdater.start();
    }

    /**
     * Stop the web server, close all client connections and un-deploy all verticles. It is safe to call this method
     * multiple times, and to call it without having first called start. It is not safe to do anything else with this
     * class once stop has been called.
     * <p>
     * This method will not throw any exceptions, all internal exceptions are logged and ignored.
     */
    public void stop() {
        try {
            metricsInitializer.stop();
        } catch (Exception ex) {
            LOGGER.warn("Failed to stop the metrics initializer", ex);
        }

        try {
            metricsReporter.stop();
        } catch (Exception ex) {
            LOGGER.warn("Failed to stop the MonitoringMetricsReporter", ex);
        }

        tracer.close();
        clients.stop();
        auths.close();
        mdsClients.stop();

        try {
            if (deploymentId != null) {
                CompletableFuture<Void> cf = new CompletableFuture<>();
                vertx.undeploy(deploymentId, result -> {
                    if (result.failed()) {
                        cf.completeExceptionally(result.cause());
                    } else {
                        cf.complete(null);
                    }
                });

                cf.join();
            }
        } catch (Exception ex) {
            LOGGER.warn("Failed to un-deploy the web server verticles", ex);
        }

        try {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            vertx.close(result -> {
                if (result.failed()) {
                    cf.completeExceptionally(result.cause());
                } else {
                    cf.complete(null);
                }
            });
            cf.join();
        } catch (Exception ex) {
            LOGGER.warn("Failed to close the web server Vertx instance", ex);
        }

        try {
            Log4jUtil.shutdown();
        } catch (Exception ex) {
            LOGGER.warn("Failed to shutdown Log4j", ex);
        }

        eventPublisher.stopRunning();
        try {
            eventPublisher.join(2000);
        } catch (InterruptedException ex) {
            LOGGER.warn("Failed to shutdown event publisher", ex);
        }

        try {
            keyValueStoreUpdater.stop();
        } catch (InterruptedException ex) {
            LOGGER.warn("Failed to shutdown KeyValueStoreUpdater", ex);
        }
    }

    public CasperConfig getConfigs() {
        return config;
    }

    public WebServerEncryption getEncryption() {
        return encryption;
    }

    public WebServerAuths getAuths() {
        return auths;
    }

    public WebServerClients getClients() {
        return clients;
    }

    public ObjectMapper getObjectMapper() {
        return mapper;
    }

    public WebServerBackends getBackends() {
        return backends;
    }

    public WebServerAPIs getApis() {
        return apis;
    }

    public TrafficController getTrafficController() {
        return controller;
    }

    public AdjustableClock getClock() {
        return clock;
    }

    public MdsClients getMdsClients() {
        return mdsClients;
    }

    private Vertx configureVertx(VertxConfiguration vertxConfig) {
        final VertxOptions vertxOptions = new VertxOptions();
        if (vertxConfig.getWorkerThreadPoolSize() != 0) {
            vertxOptions.setWorkerPoolSize(vertxConfig.getWorkerThreadPoolSize());
        }

        if (vertxConfig.getEventLoopPoolSize() != 0) {
            vertxOptions.setEventLoopPoolSize(vertxConfig.getEventLoopPoolSize());
        }

        final AddressResolverOptions addressResolverOptions = new AddressResolverOptions();
        addressResolverOptions.setCacheMaxTimeToLive((int) vertxConfig.getDnsCacheMaxTtl().getSeconds());
        addressResolverOptions.setCacheMinTimeToLive((int) vertxConfig.getDnsCacheMinTtl().getSeconds());
        addressResolverOptions.setQueryTimeout((int) vertxConfig.getDnsQueryTimeout().toMillis());
        vertxOptions.setAddressResolverOptions(addressResolverOptions);

        return Vertx.vertx(vertxOptions);
    }

    public SecretStore getSecretStore() {
        return secretStore;
    }

    @VisibleForTesting
    public ResourceControlTestHook getIdentityTestHook() {
        return resourceLimiter.getIdentityTestHook();
    }
}
