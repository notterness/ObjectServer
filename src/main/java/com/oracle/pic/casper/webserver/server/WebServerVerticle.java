package com.oracle.pic.casper.webserver.server;

import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.v2.EagleConfiguration;
import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.encryption.store.SecretStore;
import com.oracle.pic.casper.common.vertx.ssl.VertxSslUtil;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import io.netty.handler.ssl.OpenSsl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.JdkSSLEngineOptions;
import io.vertx.core.net.KeyCertOptions;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.SSLEngineOptions;
import io.vertx.core.spi.VerticleFactory;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.measure.unit.SI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * The Vert.x Verticle that manages HTTP servers for all Casper web server APIs (v1, v2, Swift, S3, Archive, Control).
 */
public class WebServerVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(WebServerVerticle.class);

    static final String VERTX_FACTORY_PREFIX = "ws";
    static final String VERTICLE_NAME = VERTX_FACTORY_PREFIX + ":" + WebServerVerticle.class.getName();

    // ObjectStorage can have very long URIs
    private static final int MAX_HTTP_REQUEST_LINE_LENGTH = 16 * 1024;

    static final class WebServerVerticleFactory implements VerticleFactory {
        private final WebServerConfiguration wsConfig;
        private final WebServerAPIs apis;
        private final TrafficController controller;
        private final SecretStore secretStore;

        WebServerVerticleFactory(WebServerConfiguration wsConfig,
                                 WebServerAPIs apis,
                                 TrafficController controller,
                                 SecretStore secretStore) {
            this.wsConfig = wsConfig;
            this.apis = apis;
            this.controller = controller;
            this.secretStore = secretStore;
        }

        @Override
        public String prefix() {
            return VERTX_FACTORY_PREFIX;
        }

        @Override
        public Verticle createVerticle(String s, ClassLoader classLoader) throws Exception {
            return new WebServerVerticle(wsConfig, apis, controller, secretStore);
        }
    }

    private final WebServerConfiguration wsConfig;
    private final WebServerAPIs apis;
    private final TrafficController controller;
    private final SecretStore secretStore;

    public WebServerVerticle(WebServerConfiguration wsConfig,
                             WebServerAPIs apis,
                             TrafficController controller,
                             SecretStore secretStore) {
        this.wsConfig = wsConfig;
        this.apis = apis;
        this.controller = controller;
        this.secretStore = secretStore;
    }

    @Override
    public void start(Future<Void> startFuture) {
        // Create a traffic recorder for this verticle, stash it in the Vert.x context, add it to the global traffic
        // controller and create a periodic task to update it.
        final TrafficRecorder recorder = new TrafficRecorder(controller.getTrafficRecorderConfiguration());
        controller.addRecorder(recorder);
        TrafficRecorder.setVertxRecorder(recorder);
        Vertx.currentContext().owner().setPeriodic(
                controller.getTrafficRecorderConfiguration().getUpdateInterval().longValue(SI.MILLI(SI.SECOND)),
                v -> recorder.update());

        CompletableFuture.allOf(
                startCasperV1WebServer(),
                startCasperV2WebServer(),
                startSwiftWebServer(),
                startS3WebServer(),
                startControlWebServer(),
                startEagleWebServer())
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        startFuture.fail(throwable);
                    } else {
                        startFuture.complete();
                    }
                });
    }

    @Override
    public void stop() {
    }

    private CompletableFuture<Void> startControlWebServer() {
        final int port = wsConfig.getControlPort();
        final HttpServerOptions options = new HttpServerOptions()
                .setIdleTimeout((int) wsConfig.getControlPlaneIdleTimeout().toMillis() / 1000)
                .setMaxChunkSize(wsConfig.getHttpServerMaxChunkSize());

        logger.info("Creating the Casper control web server on port {}", port);
        CompletableFuture<Void> cf = startWebServer(port, options,
                apis.getCasperApiControl().createRouter(vertx), false);

        logger.info("Creating the Casper control SSL web server on port {}", port);
        return CompletableFuture.allOf(cf,
                startWebServer(wsConfig.getControlSslPort(), options,
                        apis.getCasperApiControl().createRouter(vertx), true));
    }

    private CompletableFuture<Void> startCasperV1WebServer() {
        final int port = wsConfig.getApiV1Port();
        final HttpServerOptions options = new HttpServerOptions()
                .setIdleTimeout((int) wsConfig.getHttpServerIdleTimeout().toMillis() / 1000)
                .setMaxChunkSize(wsConfig.getHttpServerMaxChunkSize());

        logger.info("Creating the Casper V1 web server on port {}", port);
        CompletableFuture<Void> cf = startWebServer(port, options, apis.getCasperApiV1().createRouter(vertx), false);

        logger.info("Creating the Casper V1 SSL web server on port {}", wsConfig.getApiV1SslPort());
        return CompletableFuture.allOf(cf,
                startWebServer(wsConfig.getApiV1SslPort(), options, apis.getCasperApiV1().createRouter(vertx), true));
    }

    private CompletableFuture<Void> startCasperV2WebServer() {
        final int port = wsConfig.getApiV2Port();
        final HttpServerOptions options = new HttpServerOptions()
                .setMaxInitialLineLength(MAX_HTTP_REQUEST_LINE_LENGTH)
                .setIdleTimeout((int) wsConfig.getHttpServerIdleTimeout().toMillis() / 1000)
                .setMaxChunkSize(wsConfig.getHttpServerMaxChunkSize());

        logger.info("Creating the Casper V2 web server on port {}", port);
        CompletableFuture<Void> cf = startWebServer(port, options, apis.getCasperApiV2().createRouter(vertx), false);

        logger.info("Creating the Casper V2 SSL web server on port {}", wsConfig.getApiV2SslPort());
        return CompletableFuture.allOf(cf,
                startWebServer(wsConfig.getApiV2SslPort(), options, apis.getCasperApiV2().createRouter(vertx), true));
    }

    private CompletableFuture<Void> startSwiftWebServer() {
        final int port = wsConfig.getSwiftPort();
        final HttpServerOptions options = new HttpServerOptions()
                .setIdleTimeout((int) wsConfig.getHttpServerIdleTimeout().toMillis() / 1000)
                .setMaxChunkSize(wsConfig.getHttpServerMaxChunkSize());

        logger.info("Creating the Swift web server on port {}", port);
        CompletableFuture<Void> cf = startWebServer(port, options, apis.getSwiftApi().createRouter(vertx), false);

        logger.info("Creating the Swift SSL web server on port {}", wsConfig.getSwiftSslPort());
        return CompletableFuture.allOf(cf,
                startWebServer(wsConfig.getSwiftSslPort(), options, apis.getSwiftApi().createRouter(vertx), true));
    }

    private CompletableFuture<Void> startS3WebServer() {
        final int port = wsConfig.getS3Port();
        final HttpServerOptions options = new HttpServerOptions()
                .setIdleTimeout((int) wsConfig.getHttpServerIdleTimeout().toMillis() / 1000)
                .setMaxChunkSize(wsConfig.getHttpServerMaxChunkSize());

        logger.info("Creating the S3 web server on port {}", port);
        CompletableFuture<Void> cf = startWebServer(port, options, apis.getS3Api().createRouter(vertx), false);

        logger.info("Creating the S3 SSL web server on port {}", wsConfig.getS3SslPort());
        return CompletableFuture.allOf(cf,
                startWebServer(wsConfig.getS3SslPort(), options, apis.getS3Api().createRouter(vertx), true));
    }

    /**
     * Starts the Eagle web server. We have created a new web server on a separate port to handle all traffic
     * that comes to us without going through Flamingo L7 LBs as part of project EAGLE.
     * @see <a href=https://teamcity.oci.oraclecorp.com/repository/download/Casper_Build_CasperEBPFTunnel_BuildRelease/.lastSuccessful/index.html>Project Eagle</a>
     * */
    private CompletableFuture<Void> startEagleWebServer() {
        final HttpServerOptions options = new HttpServerOptions()
                .setMaxInitialLineLength(MAX_HTTP_REQUEST_LINE_LENGTH)
                .setIdleTimeout((int) wsConfig.getHttpServerIdleTimeout().toMillis() / 1000)
                .setMaxChunkSize(wsConfig.getHttpServerMaxChunkSize());
        final EagleConfiguration eagleConfig = wsConfig.getEagleConfiguration();

        if (!eagleConfig.isEnabled()) {
            logger.info("Eagle is disabled; not binding port 443 on any interface.");
            final CompletableFuture<Void> done = new CompletableFuture<>();
            done.complete(null);
            return done;
        }

        final BindableAddresses addresses = BindableAddresses.fromNetworkInterfaces(wsConfig.getEagleConfiguration());

        List<CompletableFuture<Void>> publicListeners = addresses.getPublicVIPs().stream().map(address ->
                startWebServer(
                        eagleConfig.getPort(),
                        options,
                        apis.getCasperVIPRouter().createRouter(vertx),
                        address.getHostAddress(),
                        true,
                        true,
                        eagleConfig.getPublic())
        ).collect(Collectors.toList());
        List<CompletableFuture<Void>> privateListeners = addresses.getPrivateVIPs().stream().map(address ->
                CompletableFuture.allOf(
                        startWebServer(
                                eagleConfig.getPort(),
                                options,
                                apis.getCasperVIPRouter().createRouter(vertx),
                                address.getHostAddress(),
                                true,
                                true,
                                eagleConfig.getPrivate()),
                        // Bind the "Verizon" (VCN Flow logs) port to the private VIP
                        startWebServer(
                                eagleConfig.getVerizonPort(),
                                options,
                                apis.getCasperApiV2().createRouter(vertx),
                                address.getHostAddress(),
                                true,
                                true,
                                eagleConfig.getPrivate()))
        ).collect(Collectors.toList());
        List<CompletableFuture<Void>> substrateListeners = addresses.getSubstrateIPs().stream().map(address ->
                CompletableFuture.allOf(
                        startWebServer(
                                eagleConfig.getPort(),
                                options,
                                apis.getCasperVIPRouter().createRouter(vertx),
                                address.getHostAddress(),
                                true,
                                true,
                                eagleConfig.getPrivate()),
                        // Bind the "Verizon" (VCN Flow logs) port to the substrate IP
                        startWebServer(
                                eagleConfig.getVerizonPort(),
                                options,
                                apis.getCasperApiV2().createRouter(vertx),
                                address.getHostAddress(),
                                true,
                                true,
                                eagleConfig.getPrivate()))
        ).collect(Collectors.toList());

        List<CompletableFuture<Void>> listeners = new ArrayList<>();
        listeners.addAll(publicListeners);
        listeners.addAll(privateListeners);
        listeners.addAll(substrateListeners);

        logger.info("Creating the Casper Eagle SSL web server on port {}", eagleConfig.getPort());
        return CompletableFuture.allOf(listeners.toArray(new CompletableFuture[0]));
    }

    /**
     * An overloaded method to support non-Eagle web servers. Calling this method will make the web server listener
     * accept incoming connections on any IP address assigned to the server (0.0.0.0).
     * Code to start Eagle web servers should not use this method but use:
     * {@link #startWebServer(int, HttpServerOptions, Router, String, boolean, boolean, EagleConfiguration.CasperCIDRConfiguration)}
     * to ensure separate listeners for private and public VIP.
     * @param port port to start the web server on.
     * @param options HttpServerOptions
     * @param router Router
     * @param ssl enable SSL on the server
     * @return A CompletableFuture indicating the if the server started successfully
     */
    private CompletableFuture<Void> startWebServer(int port, HttpServerOptions options, Router router, boolean ssl) {
        return startWebServer(port, options, router, "0.0.0.0", ssl, false, null);
    }

    private CompletableFuture<Void> startWebServer(
            int port,
            HttpServerOptions options,
            Router router,
            String host,
            boolean ssl,
            boolean isEagleWebServer,
            @Nullable EagleConfiguration.CasperCIDRConfiguration vipConfiguration
    ) {

        //if a non-ssl server is asked to be started and plaintext is disabled
        //just return a completed future
        if (!wsConfig.isPlainTextEnabled() && !ssl) {
            return CompletableFuture.completedFuture(null);
        }

        //if a ssl server is asked to be started and ssl is disabled
        //just return a completed future
        if (!wsConfig.isSslEnabled() && ssl) {
            return CompletableFuture.completedFuture(null);
        }

        /*
         * If SSL is enabled for the web-server use tcnative (https://netty.io/wiki/forked-tomcat-native.html)
         * which will dynamically link to the OpenSSL library.
         */
        if (ssl) {
            logger.info("Enabling SSL on port {}", port);
            options = serverOptionsForSSLEngine(options, host, isEagleWebServer, vipConfiguration);
        }

        CompletableFuture<Void> cf = new CompletableFuture<>();

        // Here we invoke the listen method that includes a host parameter to convince Vertx not to
        // bind to 0.0.0.0 always.
        // For non Eagle web servers, we bind to 0.0.0.0
        // For Eagle web servers, we bind to the public or private VIP
        vertx.createHttpServer(options).requestHandler(router).listen(port, host, result -> {
            if (result.failed()) {
                logger.warn("Failed to start web server on port {}:{}", host, port, result.cause());
                cf.completeExceptionally(result.cause());
            } else {
                logger.info("Successfully started web server on {}:{}", host, port);
                cf.complete(null);
            }
        });
        return cf;
    }

    /**
     * Construct an {@link HttpServerOptions} instance that uses the TLS implementation appropriate to this environment:
     *
     * <ul>
     * <li>On Oracle Linux (i.e., production) this will cause netty-tcnative to be used with the OpenSSL
     * cipher suite from the web server configuration;</li>
     * <li>On macOS (i.e., most developer's laptops) this will cause JSSE to be used.</li>
     * </ul>
     *
     * @param options                     the options to clone and adapt for TLS termination
     * @param host
     * @param isEagleWebServer            whether these options are for an Eagle TLS listener
     * @return the TLS ready options.
     */
    private HttpServerOptions serverOptionsForSSLEngine(
            final HttpServerOptions options,
            final String host,
            final boolean isEagleWebServer,
            final EagleConfiguration.CasperCIDRConfiguration vipConfig) {
        // Clone the options to avoid mutating the non SSL options
        HttpServerOptions sslServerOptions = new HttpServerOptions(options);
        sslServerOptions.setSsl(true);
        ConfigRegion region = ConfigRegion.tryFromSystemProperty().orElse(ConfigRegion.LOCAL);

        // Only available on Oracle Linux, but *not* available on TeamCity
        // https://jira-sd.mc1.oracleiaas.com/browse/CICD-3242
        if (OpenSsl.isAvailable() && region != ConfigRegion.LGL) {
            logger.info("netty-tcnative available; using OpenSSL engine with session caching");
            SSLEngineOptions sslEngineOptions = new OpenSSLEngineOptions().setSessionCacheEnabled(true);
            sslServerOptions.setSslEngineOptions(sslEngineOptions);
            logger.info("Applying OpenSSL cipher suite");
            VertxSslUtil.setOpenSSLCipherSuite(sslServerOptions, wsConfig.getCipherSuite());
        } else {
            logger.info("netty-tcnative not available; using JDK SSL engine");
            sslServerOptions.setSslEngineOptions(new JdkSSLEngineOptions());
        }

        if (isEagleWebServer) {
            KeyCertOptions keyCertOptions = VertxSslUtil.getKeyOption("eagle:" + host,
                    Collections.singletonList(vipConfig.getSniCertParentFolder()),
                    secretStore,
                    vipConfig.getAllCerts(),
                    vipConfig.getDefaultCertName());
            sslServerOptions.setKeyCertOptions(keyCertOptions)
                    .setSni(true);
        } else {
            // In the case of non-eagle, we start web-servers potentially with TLS for FedRamp compliance.
            // These web-servers are "TLS Backends" to their upstream Flamingo loadbalancers and use the
            // node-server-cert PKI material on the machines.
            Path serverCertPath = Paths.get(wsConfig.getServerCertFolder());
            sslServerOptions.setKeyCertOptions(VertxSslUtil.getKeyOption(serverCertPath));
        }

        return sslServerOptions;
    }
}
