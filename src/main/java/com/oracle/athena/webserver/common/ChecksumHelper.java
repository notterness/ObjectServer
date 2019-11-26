package com.oracle.athena.webserver.common;

import com.oracle.pic.casper.common.exceptions.InvalidMd5Exception;

import java.util.Base64;
import java.util.Map;
import java.util.Optional;

public class ChecksumHelper {

    // TODO: We need to determine whether we'll use JETTY or rewrite the header enum classes.
    private static final String CONTENT_MD5_HEADER = "content-md5";

    /**
     * This method extracts the `Content-MD5` header from the map of request headers and return an optional that may
     * contain the decoded md5 header.
     *
     * @param headers a {@link Map} representing all of the headers associated with a request.
     * @return an {@link Optional} string value that, if present, represents the decoded MD5 Checksum.
     */
    public static Optional<String> getContentMD5Header(Map<String, String> headers) {
        String contentMD5 = headers.get(CONTENT_MD5_HEADER);
        if (contentMD5 == null || contentMD5.isEmpty()) {
            return Optional.empty();
        }
        return getContentMD5Header(contentMD5);
    }

    /**
     * This method returns an optional that, when present, contains the decoded MD5 Checksum.
     * @param contentMD5 the request header value for 'content-md5'
     * @return an {@link Optional} string value that, if present, represents the decoded MD5 Checksum.
     */
    public static Optional<String> getContentMD5Header(String contentMD5) {
        byte[] bytes = Base64.getDecoder().decode(contentMD5);
        if (bytes.length != 16) {
            throw new InvalidMd5Exception("The value of the Content-MD5 header '" + contentMD5 +
                    "' was not the correct length after base-64 decoding");
        }
        return Optional.of(contentMD5);
    }
}
