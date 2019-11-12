package com.oracle.pic.casper.webserver.traffic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.oracle.pic.casper.common.executor.RefreshTask;
import com.oracle.pic.casper.mds.loadbalanced.MdsExecutor;
import com.oracle.pic.casper.mds.operator.ListConfigKeysRequest;
import com.oracle.pic.casper.mds.operator.ListConfigKeysResponse;
import com.oracle.pic.casper.mds.operator.MdsConfigEntry;
import com.oracle.pic.casper.mds.operator.OperatorMetadataServiceGrpc.OperatorMetadataServiceBlockingStub;
import com.oracle.pic.casper.webserver.api.backend.MdsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * KeyValueStoreUpdater notifies listeners of changes to a KeyValueStore.
 * <p>
 * The KeyValueStoreUpdater polls a KeyValueStore (at a fixed delay) and notifies listeners of any changes to the set
 * of key/value pairs in the store. The first update to the listeners will always contain every key/value pair in the
 * store. Subsequent updates will notify the listener of any changed key/value pairs (added or modified) and any
 * removed keys.
 * <p>
 * The full set of listeners is specified in the constructor, and cannot be modified. The KeyValueStoreUpdater must be
 * started with the start() method, which will schedule an immediate update of the listeners (with the full set of
 * key/value pairs in the store). That update will occur asynchronously, on the polling thread, so listeners must have
 * a "default" state between the time they are created and the time the first update arrives. Subsequent updates to
 * listeners will only be sent when there are changes to report, and only at fixed delay intervals (as specified in the
 * constructor). Note that the initial update could be empty, but subsequent updates will never be empty (at least one
 * key will be changed or removed).
 */
public final class KeyValueStoreUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueStoreUpdater.class);

    @FunctionalInterface
    public interface Listener {
        /**
         * Receives updates to the set of key/value pairs.
         * <p>
         * The first time this method is called on a listener the changed map will contain the full set of key/value
         * pairs in the KeyValueStore. On subsequent calls the changed map will contain keys that have been added or
         * modified since the last call, and the removed set will contain keys that have been removed since the last
         * call. In the first call, both changed and removed may be empty (if there are no key/value pairs in the store)
         * but on subsequent calls at least one key will have been changed or removed. Neither changed nor removed will
         * ever be null.
         * <p>
         * It is safe to keep references to changed and removed (the updater guarantees that they will not be modified).
         * <p>
         * Implementations of this method must not throw exceptions and must be thread safe.
         */
        void update(Map<String, String> changed, Set<String> removed);
    }

    private MdsExecutor<OperatorMetadataServiceBlockingStub> client;
    private final List<Listener> listeners;
    private final Duration requestDeadline;
    private final RefreshTask refreshTask;
    private volatile Map<String, String> state;

    public KeyValueStoreUpdater(
            MdsExecutor<OperatorMetadataServiceBlockingStub> client,
            List<Listener> listeners,
            Duration delay) {
        this(client, Duration.ofSeconds(10), listeners, delay);
    }

    /**
     * Constructor
     *
     * @param client    Operator MDS blocking client.
     * @param listeners the listeners that are notified of changes. The listeners are always notified in this order.
     * @param delay     the polling delay.
     */
    public KeyValueStoreUpdater(MdsExecutor<OperatorMetadataServiceBlockingStub> client,
                                Duration requestDeadline, List<Listener> listeners, Duration delay) {
        this.client = client;
        this.requestDeadline = requestDeadline;
        this.listeners = listeners;
        this.refreshTask = new RefreshTask(this::update, delay, "kvstore-updater", null);
        this.state = null;
    }

    /**
     * Starts the poller.
     * <p>
     * This method must only be called once for each instance of this class.
     * <p>
     * The first update to the listeners will contain the full list of key/value pairs, and subsequent updates will
     * contain only the updated and removed key/value pairs. The first call to update for each listener will occur
     * asynchronously as soon as possible after start is called. The subsequent calls will occur when there are changes
     * and only after the polling delay.
     */
    public void start() {
        refreshTask.start();
    }

    /**
     * Stops the poller.
     */
    public void stop() throws InterruptedException {
        refreshTask.stop();
    }

    @VisibleForTesting
    void update() {
        final Map<String, String> newState;
        try {
            newState = getConfigKeys();
        } catch (Exception ex) {
            LOG.error("Failed to list the keys and values in the KeyValueStore", ex);
            return;
        }

        final Map<String, String> changed;
        final Set<String> removed;
        if (state == null) {
            // This is the initial update, so we just send the entire state.
            changed = Collections.unmodifiableMap(newState);
            removed = ImmutableSet.of();
        } else {
            // This is a subsequent update, so we need to diff with the previous state.
            changed = new HashMap<>();
            newState.forEach((k, v) -> {
                if (!state.containsKey(k) || !Objects.equals(state.get(k), v)) {
                    changed.put(k, v);
                }
            });

            removed = new HashSet<>();
            state.forEach((k, v) -> {
                if (!newState.containsKey(k)) {
                    removed.add(k);
                }
            });
        }

        // Update listeners on the first update, or anytime the diff is not empty.
        if (state == null || !changed.isEmpty() || !removed.isEmpty()) {
            final Map<String, String> uchanged = Collections.unmodifiableMap(changed);
            final Set<String> uremoved = Collections.unmodifiableSet(removed);
            for (Listener listener : listeners) {
                try {
                    listener.update(uchanged, uremoved);
                } catch (Exception ex) {
                    LOG.debug("A listener threw an unexpected exception", ex);
                }
            }
        }

        // Update to the new state.
        state = newState;
    }

    private Map<String, String> getConfigKeys() {
        List<MdsConfigEntry> results = new ArrayList<>();
        ListConfigKeysResponse response = null;
        AtomicReference<String> pageToken = new AtomicReference<>("");
        do {
            response = MdsMetrics.executeWithMetrics(
                    MdsMetrics.OPERATOR_MDS_BUNDLE,
                    MdsMetrics.OPERATOR_LIST_CONFIG,
                    false,
                    () -> client.execute(c -> c.withDeadlineAfter(requestDeadline.toMillis(), TimeUnit.MILLISECONDS)
                            .listConfigKeys(ListConfigKeysRequest.newBuilder()
                                    .setPageSize(1000)
                                    .setPageToken(pageToken.get())
                                    .build())));
            results.addAll(response.getConfigEntriesList());
            pageToken.set(response.getNextPageToken());
        } while (!response.getNextPageToken().isEmpty());
        Map<String, String> map = new HashMap<>();
        for (MdsConfigEntry entry : results) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
}
