package com.oracle.pic.casper.storageclient.core.replicated;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.oracle.pic.casper.common.config.v2.VolumeStorageClientConfiguration;
import com.oracle.pic.casper.common.scheduling.vertx.PerHostVertxContextProvider;
import com.oracle.pic.casper.storageclient.HttpClientProvider;
import com.oracle.pic.casper.storageclient.core.AthenaPutRequestHandler;
import com.oracle.pic.casper.storageclient.core.AthenaRequestHandlerFactory;
import com.oracle.pic.casper.storageclient.core.DeleteRequestHandler;
import com.oracle.pic.casper.storageclient.core.GetRequestHandler;
import com.oracle.pic.casper.storageclient.core.PutRequestHandler;
import com.oracle.pic.casper.storageclient.core.RequestHandlerFactory;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;

/**
 * A factory class to help instantiate Get/Put/Delete RequestHandler directly instead of relying on Guice to avoid
 * paying overhead of Guice.
 */
@Singleton
public class AthenaReplicatedRequestHandlerFactory implements AthenaRequestHandlerFactory {

    private final Vertx vertx;
    private final VolumeStorageClientConfiguration configuration;
    private final HttpClientProvider httpClientProvider;
    private final PerHostVertxContextProvider perHostVertxContextProvider;

    /**
     * Used to track failures/successes when talking to a host and to rank the
     * different hosts we can talk to.
     */
    private final HostTracker hostTracker;

    /**
     * Instantiates a new Replicated request handler factory.
     *
     * @param vertx             the vertx
     * @param configuration     the configuration
     */
    @Inject
    public AthenaReplicatedRequestHandlerFactory(Vertx vertx,
                                           VolumeStorageClientConfiguration configuration,
                                           HttpClientProvider httpClientProvider,
                                           PerHostVertxContextProvider perHostVertxContextProvider) {
        this.vertx = vertx;
        this.configuration = configuration;
        this.httpClientProvider = httpClientProvider;
        this.perHostVertxContextProvider = perHostVertxContextProvider;
        this.hostTracker = new HostTracker(configuration.getFailureCountSaturation());
    }

    /**
     * Creates a new GET request handler.
     *
     * @return the GET request handler
     */
    @Override
    public GetRequestHandler newGetRequestHandler() {
        HttpClient client = httpClientProvider.getDataClient();
        return new ReplicatedGetRequestHandler(vertx, client, configuration, hostTracker, hostTracker);
    }


    /**
     * Creates a new PUT request handler.
     *
     * @return the PUT request handler
     */
    @Override
    public AthenaPutRequestHandler newPutRequestHandlerAthena() {
        HttpClient client = httpClientProvider.getDataClient();
        return new AthenaReplicatedPutRequestHandler(vertx, client, configuration, hostTracker);
    }

    @Override
    public PutRequestHandler newPutRequestHandler() {
        HttpClient client = httpClientProvider.getDataClient();
        return new ReplicatedPutRequestHandler(vertx, client, configuration, hostTracker);
    }

    /**
     * Creates a new DELETE request handler.
     *
     * @return the DELETE request handler
     */
    @Override
    public DeleteRequestHandler newDeleteRequestHandler() {
        HttpClient client = httpClientProvider.getHttpsDataClient();
        return new ReplicatedDeleteRequestHandler(vertx, client, configuration,
                hostTracker, httpClientProvider, perHostVertxContextProvider);
    }
}
