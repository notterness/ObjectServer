package com.oracle.pic.casper.webserver.util;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.webserver.traffic.KeyValueStoreUpdater;

import java.util.Map;
import java.util.Set;

/**
 * Provide a simple way to update a config value when a change is seen by
 * the {@link KeyValueStoreUpdater}. This class takes a key and track the
 * value associated with the key.
 */
public class ConfigValueImpl implements ConfigValue, KeyValueStoreUpdater.Listener {

    /**
     * The key we will look for.
     */
    private final String key;

    /**
     * The value associated with the key.
     */
    private String value;

    public ConfigValueImpl(String key) {
        this.key = Preconditions.checkNotNull(key);
        this.value = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getValue() {
        return value;
    }

    /**
     * @InheritDoc
     */
    @Override
    public void update(Map<String, String> changed, Set<String> removed) {
        Preconditions.checkNotNull(changed);
        Preconditions.checkNotNull(removed);
        if (removed.contains(key)) {
            value = null;
        } else {
            value = changed.getOrDefault(key, value);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("key", key)
            .add("value", value)
            .toString();
    }
}
