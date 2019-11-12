package com.oracle.pic.casper.webserver.api.v2;

import org.apache.commons.io.Charsets;

import javax.annotation.Nullable;

/**
 * When we have both startWith and the delimiter present in the list objects request then we want to skip all the
 * objectNames where the name has the startWith and the delimiter to speed up the list operation.
 *
 * This is useful when there are object names > 1000 with the same prefix containing the delimiter.
 *
 * The startWith is modified such that:
 * a) Anything after the first delimiter is removed
 * b) The first delimiter is replaced with the character `delimiter+1`.
 *
 * This is to ensure that the objects and prefixes returned in the intermediate result is always non null.
 */
final class KeyUtils {

    private KeyUtils() {

    }

    public static String modifyStartWith(@Nullable String startWith, @Nullable Character delimiter,
                                         @Nullable String prefix) {
        if (startWith == null) {
            return null;
        }
        /*
         * If there is no delimiter or if the delimiter is at the end of startWith then we are done
         */
        if (delimiter == null || startWith.charAt(startWith.length() - 1) == delimiter) {
            return startWith;
        }
        int firstDelimiter;
        if (prefix != null) {
            firstDelimiter = startWith.indexOf(delimiter, prefix.length());
        } else {
            firstDelimiter = startWith.indexOf(delimiter);
        }



        if (firstDelimiter > 0) {
            String truncated = startWith.substring(0, firstDelimiter + 1);
            final int length = truncated.getBytes(Charsets.UTF_8).length;
            if (length > 1024) {
                // The start-with argument is already longer than the maximum key length.
                // We can use the key as-is because no object can match it.
                return truncated;
            }
            //replace the last character (the delimiter) with character+1;
            if (delimiter != '\uffff') {
                truncated = truncated.substring(0, truncated.length() - 1) + (char) (delimiter + 1);
                return truncated;
            } else {
                //remove the delimiter and then add one to the last char
                truncated = truncated.substring(0, truncated.length() - 1);
                char lastChar = truncated.charAt(truncated.length() - 1);
                return truncated.substring(0, truncated.length() - 1) + (char) (lastChar + 1);
            }

        } else {
            return startWith;
        }
    }
}
