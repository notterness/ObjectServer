package com.oracle.pic.casper.webserver.util;

/**
 * A configuration value.
 */
public interface ConfigValue {

    /**
     * Gets the value of the configuration.
     */
    String getValue();

    default boolean getValueAsBoolean() {
        return Boolean.parseBoolean(getValue());
    }
}
