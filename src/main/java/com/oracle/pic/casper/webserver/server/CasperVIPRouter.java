package com.oracle.pic.casper.webserver.server;

import com.google.common.net.InetAddresses;
import com.oracle.pic.casper.common.config.v2.EagleConfiguration;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.Api;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.VirtualHostHandler;

import java.net.InetAddress;

/**
 * CasperVIPRouter is a <b>composite</b> API router.
 * Prior to Project Eagle, the web-server would open a port <b>per</b> API (i.e. v1, swift, v2, s3).
 * With the introduction of Eagle, all the APIs will be hosted on a single TCP port -- 443.
 * Therefore the logic to select the correct downstream router must now be done here with the following caveats:
 * Let PubVIP be the public VIP of ObjectStorage (i.e. objectstorage.us-phoenix-1.oraclecloud.com)
 * Let PrivVIP be the private VIP of ObjectStorage (i.e. casperv2.svc.ad1.r2)
 *
 * 1. The V1 API <b>will not</b> be accessible if the web-server was accessed through the PubVIP
 * 2. The API will be be selected based on the combination of Host header
 * (Those familiar with Flamingo will releaize the above is similar in functionality to what was provided)
 *
 * @see <a href=https://teamcity.oci.oraclecorp.com/repository/download/Casper_Build_CasperEBPFTunnel_BuildRelease/.lastSuccessful/index.html>Project Eagle</a>
 */
public class CasperVIPRouter implements Api {

    private final EagleConfiguration config;
    private final WebServerAPIs apis;

    public CasperVIPRouter(EagleConfiguration config, WebServerAPIs apis) {
        this.config = config;
        this.apis = apis;
    }


    /**
     * Register the api with the router so that when the Host header in the request matches the hostnames provided
     * the api is called.
     */
    private void addRouterForHostnames(Router router, Handler<HttpServerRequest> api, String... hostnames) {
        // Register the Virtual Host header provided
        for (String hostname : hostnames) {
            if (hostname != null) {
                router.route().handler(
                        VirtualHostHandler.create(hostname, routingContext -> api.handle(routingContext.request())));
            }
        }
    }

    @Override
    public Router createRouter(Vertx vertx) {
        // by this point TLS is already negotiated and therefore SNI (Server Name Indication)
        // was used to select the correct TLS certificate

        // 1. create the main composite router
        final Router router = Router.router(vertx);
        // 2. Create each API router
        Router s3ApiRouter = apis.getS3Api().createRouter(vertx);
        Router swiftApiRouter = apis.getSwiftApi().createRouter(vertx);
        Router casperV1ApiRouter = apis.getCasperApiV1().createRouter(vertx);
        Router casperV2ApiRouter = apis.getCasperApiV2().createRouter(vertx);

        // V1
        addRouterForHostnames(router, request -> {
            // we do not want to make the V1 API accessible through the public VIP
            // so check the local address and fail the request if that's the case.
            final InetAddress address = InetAddresses.forString(request.localAddress().host());
            if (config.isPublicVIP(address)) {
                request.response().setStatusCode(HttpResponseStatus.NOT_FOUND).end();
                return;
            }
            casperV1ApiRouter.handle(request);
        },
        config.getPrivate().getV1().getDns());

        // V2
        addRouterForHostnames(
                router,
                casperV2ApiRouter,
                config.getPrivate().getV2().getDns(),
                config.getPublic().getV2().getDns()
        );

        // Swift
        addRouterForHostnames(
                router,
                swiftApiRouter,
                config.getPrivate().getSwift().getDns(),
                config.getPublic().getSwift().getDns()
        );

        // S3
        addRouterForHostnames(
                router,
                s3ApiRouter,
                // we don't care to demonstrate we own the non-wildcard
                // s3 within the substrate, so exclude it from the VirtualHost
                // config.getPrivateVIP().getS3().getDns(),
                config.getPublic().getS3().getDns(),
                config.getPrivate().getS3Star().getDns(),
                config.getPublic().getS3Star().getDns()
        );

        return router;
    }
}
