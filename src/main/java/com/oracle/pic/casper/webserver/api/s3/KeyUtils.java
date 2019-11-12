package com.oracle.pic.casper.webserver.api.s3;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.io.Charsets;

import javax.annotation.Nullable;

final class KeyUtils {

    private KeyUtils() {
    }

    /**
     * The S3 list objects/versions methods supports a "start after" parameter,
     * but our internal listObjects call takes a "start with" argument. This
     * method calculates the next key.
     */
    public static String startWith(@Nullable String startAfter, @Nullable String prefix,
                                   @Nullable Character delimiter) {
        if (startAfter == null) {
            return null;
        }

        final int firstDelimiter;
        if (delimiter != null) {
            if (prefix != null) {
                firstDelimiter = startAfter.indexOf(delimiter, prefix.length());
            } else {
                firstDelimiter = startAfter.indexOf(delimiter);
            }
        } else {
            firstDelimiter = -1;
        }

        if (firstDelimiter >= 0) {
            // Suppose we have a key like "project/src/main.c" and a delimiter of '/'
            // What key should we start listing with? We want to avoid listing all
            // objects that start with "project/" because "project/" should NOT be
            // returned as a common prefix. This means we want to start with the
            // first key that comes after "project/" but that does not start with
            // "project/". To do that we will truncate the key to the delimiter
            // and then pad the key with '\uffff'. We will make sure the padded
            // key is longer than any valid object name so that the next valid
            // object name will be the one we want.
            //
            // As an example, suppose we only support uppercase ASCII letters plus '/'
            // as the delimiter and the maximum key length is 6 characters.
            //  - Given "FOO/A" as startAfter we should use "FOO/ZZZ" as startWith
            //  - Given "FOO/ZZ" as startAfter we should also use "FOO/ZZZ" as startWith
            //  - Given "A/B/" as startAfter with a prefix of "A/" we should use "A/B/ZZZ" as startWith
            final String truncated = startAfter.substring(0, firstDelimiter + 1);
            final int length = truncated.getBytes(Charsets.UTF_8).length;
            if (length > 1024) {
                // The start-with argument is already longer than the maximum key length.
                // We can use the key as-is because no object can match it.
                return truncated;
            }
            // The maximum UTF-8 encoded object name length is 1024 bytes so we will produce a
            // key that is at least 1025 bytes. Our padding character ('\uffff') encodes to 3 bytes.
            final int paddingChars = ((1025 - length) + 2) / 3;
            Preconditions.checkState(paddingChars > 1);
            return truncated + Strings.repeat("\uffff", paddingChars);
        } else {
            return startAfter + "\u0000";
        }
    }
}
