package com.oracle.pic.casper.webserver.api.model;

import com.google.common.collect.ImmutableList;
import com.oracle.pic.casper.common.rest.CommonHeaders;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidPropertyException;

import java.util.Locale;

/**
 * This class enumerates externally visible properties of Object, mostly contained in ObjectMetadata
 * Those properties can be requested to be returned as part of Object LIST operations.
 * Value of certain properties are only computed in a context of a particular query (such as those dependent on the
 * value of delimiter character.
 */
public enum ObjectProperties {

    NAME,
    SIZE,
    TIMECREATED,
    TIMEMODIFIED,
    MD5,
    ARCHIVED,
    METADATA,
    ETAG,
    ARCHIVALSTATE;

    // Utility method to parse a comma separated list of properties, by name
    public static ImmutableList<ObjectProperties> parseConcatenatedList(String string) {

        ImmutableList.Builder<ObjectProperties> builder = ImmutableList.<ObjectProperties>builder();

        if (string == null || string.isEmpty()) {
            return builder.build();
        }

        String[] split = string.split(CommonHeaders.HEADER_VALUE_SEPARATOR);

        for (String h : split) {
            try {
                ObjectProperties op = ObjectProperties.valueOf(h.toUpperCase(Locale.ENGLISH).trim());
                builder.add(op);
            } catch (IllegalArgumentException e) {
                throw new InvalidPropertyException("Property " + h + " is not valid", null);
            }
        }

        return builder.build();
    }
}
