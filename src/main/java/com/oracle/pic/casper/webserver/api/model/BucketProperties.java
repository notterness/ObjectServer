package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidPropertyException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public enum BucketProperties {

    APPROXIMATE_SIZE("approximateSize"),
    APPROXIMATE_COUNT("approximateCount"),
    TAGS("tags"),
    OBJECT_LIFECYCLE_POLICY_ETAG("objectLifecyclePolicyEtag");

    private static final String URI_REQUEST_VALUE_SEPARATOR = ",";

    private final String name;

    BucketProperties(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getName();
    }

    @JsonCreator
    public static BucketProperties createFrom(String value) {
        for (BucketProperties v : values()) {
            if (v.getName().equalsIgnoreCase(value)) {
                return v;
            }
        }
        throw new IllegalArgumentException("Could not find a bucket property named " + value);
    }


    /**
     * Parse CSV (comma separated) string and return all {@link BucketProperties} that are found.
     * @param string the value to parse
     * @param validPropertiesArr the list of {@link BucketProperties} that are acceptable
     * @throws IllegalArgumentException if it finds an bad value
     */
    public static Set<BucketProperties> parseConcatenatedList(String string,
                                                              BucketProperties... validPropertiesArr) {

        if (Strings.isNullOrEmpty(string)) {
            return Collections.emptySet();
        }

        List<BucketProperties> validProperties = Arrays.asList(validPropertiesArr);

        List<String> values = Splitter.on(URI_REQUEST_VALUE_SEPARATOR)
                .trimResults()
                .splitToList(string);

        return values.stream().map(String::toUpperCase)
                .map(value -> {
                    try {
                        BucketProperties property = BucketProperties.createFrom(value);
                        if (!validProperties.contains(property)) {
                            throw new InvalidPropertyException("Property " + value + " is not valid", null);
                        }
                        return property;
                    } catch (IllegalArgumentException ex) {
                        throw new InvalidPropertyException("Property " + value + " is not valid", ex);
                    }
                }).collect(Collectors.toSet());
    }
}
