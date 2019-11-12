package com.oracle.pic.casper.webserver.api.common;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.oracle.pic.casper.common.exceptions.ContentMD5UnmatchedException;
import com.oracle.pic.casper.common.exceptions.InvalidMd5Exception;
import io.vertx.core.http.HttpServerRequest;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.util.Base64;
import java.util.Optional;

/**
 * A common code to format the md5 depending on the API and the object (normal object vs multipart object).
 *
 * There are so many variations of the way that we send out the header so we put all the variations in the common
 * place so that we can use the right one as appropriate.
*/
public final class ChecksumHelper {
    private ChecksumHelper() {
    }

    /**
     * Reads the MD5 information from {@code Content-Md5} header from a client request.
     *
     * @param request the request
     * @return the md5 header in base64 format
     */
    public static Optional<String> getContentMD5Header(HttpServerRequest request) {
        String contentMD5 = request.getHeader(io.vertx.core.http.HttpHeaders.CONTENT_MD5);
        if (contentMD5 == null || contentMD5.isEmpty()) {
            return Optional.empty();
        }

        try {
            byte[] bytes = BaseEncoding.base64().decode(contentMD5);
            if (bytes.length != 16) {
                throw new InvalidMd5Exception("The value of the Content-MD5 header '" + contentMD5 +
                    "' was not the correct length after base-64 decoding");
            }
            return Optional.of(contentMD5);
        } catch (IllegalArgumentException iaex) {
            throw new InvalidMd5Exception("The value of the Content-MD5 header '" + contentMD5 +
                "' was not the correct length after base-64 decoding");
        }
    }

    public static String computeBase64MD5(byte[] bytes) {
        return BaseEncoding.base64().encode(Hashing.md5().newHasher().putBytes(bytes).hash().asBytes());
    }

    public static String computeHexMD5(byte[] bytes) {
        return Hex.encodeHexString(Hashing.md5().newHasher().putBytes(bytes).hash().asBytes());
    }

    public static String computeHexSHA256(byte[] bytes) {
        return Hex.encodeHexString(Hashing.sha256().newHasher().putBytes(bytes).hash().asBytes());
    }

    /**
     * Performs an integrity check on the body of an HTTP request if the Content-MD5 header is available.
     *
     * If Content-MD5 is not present, this function does nothing, otherwise it computes the MD5 value for the body and
     * compares it to the value from the header.
     *
     * @param request the HTTP request used to find the Content-MD5 header.
     * @param bytes   the raw bytes of the HTTP request body.
     */
    public static void checkContentMD5(HttpServerRequest request, byte[] bytes) {
        Optional<String> contentMD5 = getContentMD5Header(request);
        if (contentMD5.isPresent()) {
            String actualMD5 = computeBase64MD5(bytes);
            if (!contentMD5.get().equals(actualMD5)) {
                throw new ContentMD5UnmatchedException("The Content-MD5 you specified did not match what we received.");
            }
        }
    }

    /**
     * Convert a base64-encoded string to hexadecimal.
     *
     * Swift doesn't use standard base64-encoded Content-MD5 headers, but it does implement the same feature using
     * hex-encoded MD5s as object ETags.  This helper method lets us convert from our storage representation (base64)
     * to Swift's (hex).
     */
    public static String base64ToHex(String base64String) {
        return Hex.encodeHexString(Base64.getDecoder().decode(base64String));
    }

    /**
     * Convert a hex-encoded string to base64.
     *
     * Swift doesn't use standard base64-encoded Content-MD5 headers, but it does implement the same feature using
     * hex-encoded MD5s as object ETags.  This helper method lets us convert from Swift's representation (hex) to our
     * storage representation (base64).
     */
    public static String hexToBase64(String hexString) throws DecoderException {
        return Base64.getEncoder().encodeToString(Hex.decodeHex(hexString.toCharArray()));
    }
}
