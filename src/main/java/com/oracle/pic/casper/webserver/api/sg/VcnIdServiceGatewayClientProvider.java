package com.oracle.pic.casper.webserver.api.sg;

import com.codahale.metrics.Counter;
import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.executor.RefreshTask;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.util.GuavaCollectors;
import com.oracle.pic.casper.common.util.ListUtils;
import com.oracle.pic.casper.common.util.ListableResponse;
import com.oracle.pic.commons.client.exceptions.RestClientException;
import com.oracle.pic.commons.client.model.Page;
import com.oracle.pic.sgw.api.InternalServiceGateway;
import com.oracle.pic.sgw.model.InternalVcnHeaderMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An implementation of {@link VcnIdProvider} that fetches the mappings from the service gateway client.
 * The mappings will be cached depending on the interval provided.
 */
public class VcnIdServiceGatewayClientProvider implements VcnIdProvider {

    private static final Logger LOG = LoggerFactory.getLogger(VcnIdServiceGatewayClientProvider.class);

    private static final int DEFAULT_PAGE_LIMIT = 100;

    private static final Counter SUCCESS_CLIENT_COUNTER = Metrics.counter("sgw.client.success");
    private static final Counter FAILURE_CLIENT_COUNTER = Metrics.counter("sgw.client.failure");
    private static final Metrics.LongGauge CACHE_SIZE_CLIENT_GAUGE = Metrics.gauge("sgw.cache.size");

    /**
     * A read-only cache of the vcn-id to ocid mappings.
     * The reference held here will be swapped out every refresh interval.
     */
    private final AtomicReference<ImmutableMap<Long, String>> cache = new AtomicReference<>(ImmutableMap.of());

    /**
     * The high level refresh task that will take care of calling the service gateway client for the new
     * mappings.
     */
    private final RefreshTask refreshTask;

    private InternalServiceGateway client;

    public VcnIdServiceGatewayClientProvider(Duration refreshInterval, InternalServiceGateway client) {

        this.client = client;

        //Create the refresh task that simplify swaps out the cache atomic reference
        this.refreshTask = new RefreshTask(() -> {

            try {
                //create a stream of every mapping
                ImmutableMap<Long, String> newCache = ListUtils.listEntities(pageToken -> {
                    Page<InternalVcnHeaderMapping> page = client.listVcnHeaderMappings(DEFAULT_PAGE_LIMIT, pageToken);

                    String nextPage = page.getNextPageToken().orElse(null);
                    return ListableResponse.create(page::getItems, () -> nextPage);
                }).collect(GuavaCollectors.toMap(InternalVcnHeaderMapping::getVcnId,
                        InternalVcnHeaderMapping::getVcnOcid));

                cache.set(newCache);

                SUCCESS_CLIENT_COUNTER.inc();
                CACHE_SIZE_CLIENT_GAUGE.set(newCache.size());
            } catch (RestClientException e) {
                LOG.warn("Service Gateway call failed with error {} and message {}", e.getHttpCode(), e.getMessage());
            } catch (Throwable t) {
                LOG.warn("Failure when using service gateway client.", t);
                FAILURE_CLIENT_COUNTER.inc();
            }

        }, refreshInterval);
    }

    @Override
    public Optional<String> getMapping(long vcnId) {
        return Optional.ofNullable(cache.get().get(vcnId));
    }

    @Override
    public void start() {
        refreshTask.start();
    }

    @Override
    public void stop() {
        try {
            refreshTask.stop();
        } catch (InterruptedException ex) {
            LOG.warn("Interrupted while stopping the refresh task for the service gateway client", ex);
        }

        try {
            client.close();
        } catch (Exception e) {
            LOG.warn("Failed to close service gateway client.", e);
        }
    }
}
