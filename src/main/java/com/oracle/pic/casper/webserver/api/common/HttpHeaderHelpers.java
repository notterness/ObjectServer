package com.oracle.pic.casper.webserver.api.common;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.encryption.Obfuscate;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.common.exceptions.RangeNotSatisfiableException;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.rest.CommonHeaders;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import org.apache.commons.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Helper methods for dealing with HTTP headers, both reading them and generating them.
 */
public final class HttpHeaderHelpers {

    private static final Logger LOG = LoggerFactory.getLogger(HttpHeaderHelpers.class);

    public enum UserMetadataPrefix {
        V1("x-cspr-meta-"),
        V2("opc-meta-"),
        S3("x-amz-meta-"),
        SWIFT_CONTAINER("x-container-meta-"),
        SWIFT_OBJECT("x-object-meta-");

        private final String prefix;
        private final int length;

        UserMetadataPrefix(String prefix) {
            this.prefix = prefix;
            this.length = prefix.length();
        }

        public String getPrefix() {
            return prefix;
        }

        public int getLength() {
            return length;
        }

        @Override public String toString() {
            return prefix;
        }
    }

    public static final String OPC_CONTENT_MD5_HEADER = "opc-content-md5";

    // Content headers are standard HTTP headers, like "Content-Type" or
    // "Content-Encoding" that do NOT start with the user metadata prefix
    // of "opc-meta-" but are stored as part of the object metadata. This
    // means they are included in the Map<String,String> we use to represent
    // metadata.
    //
    // We store the content headers by prefixing them with characters that
    // are not allowed in metadata keys. This is done to allow us to
    // differentiate between "opc-meta-content-language" and "content-language"
    // headers. To save space we strip off the "opc-meta-" prefix from custom
    // headers so "opc-meta-content-language" is stored as "content-language".
    // We store the HTTP "content-language" with a key of "@^L".
    //
    // The two maps below let us translate between the HTTP headers and the
    // key we use in our metadata maps.

    /**
     * This maps a standard HTTP header (e.g. "Content-Disposition") to the
     * key that is used to store the header in a metadata map (e.g. "@^D").
     *
     * This is used when an object is written.
     *
     * The inverse of this map is {@link #PRESERVED_CONTENT_HEADERS_LOOKUP}.
     */
    public static final Map<String, String> PRESERVED_CONTENT_HEADERS;

    /**
     * This maps a key in a metadata map (e.g "@^T") to a standard HTTP header
     * (e.g. "Content-Type").
     *
     * This is used when an object is retrieved.
     *
     * The inverse of this map is {@link #PRESERVED_CONTENT_HEADERS}.
     */
    public static final Map<String, String> PRESERVED_CONTENT_HEADERS_LOOKUP;

    static {
        PRESERVED_CONTENT_HEADERS_LOOKUP
            = ImmutableMap.<String, String>builder()
            .put(ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX + "^T", HttpHeaders.CONTENT_TYPE.toString())
            .put(ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX + "^L", HttpHeaders.CONTENT_LANGUAGE.toString())
            .put(ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX + "^C", HttpHeaders.CACHE_CONTROL.toString())
            .put(ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX + "^D", HttpHeaders.CONTENT_DISPOSITION.toString())
            .put(ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX + "^E", HttpHeaders.CONTENT_ENCODING.toString())
            .build();

        final ImmutableMap.Builder builder = ImmutableMap.<String, String>builder();
        for (Map.Entry<String, String> entry : PRESERVED_CONTENT_HEADERS_LOOKUP.entrySet()) {
            builder.put(entry.getValue().toLowerCase(Locale.ENGLISH), entry.getKey());
        }
        PRESERVED_CONTENT_HEADERS = builder.build();
    }

    private HttpHeaderHelpers() {

    }

    /**
     * Get the names and values of user-defined metadata that start with the user-defined
     * metadata prefix.
     * @param request the HTTP server request from which to read headers.
     * @param prefix api specific user metadata header prefix.
     * @param entries the entries of key value pair to look at
     * @param throwWhenInvalid whether to throw an exception when metadata key does not start with prefix
     * @return a map of user-defined metadata, which may be empty (but won't be null).
     */
    public static Map<String, String> getUserMetadata(HttpServerRequest request, UserMetadataPrefix prefix,
                                                      List<Map.Entry<String, String>> entries,
                                                      boolean throwWhenInvalid,
                                                      boolean metadataMerge) {
        HashMap<String, String> userMetadataMap = new HashMap<>();

        for (Map.Entry<String, String> entry : entries) {

            String caseInsensitiveName = entry.getKey().toLowerCase(Locale.ENGLISH);

            if (caseInsensitiveName.startsWith(prefix.getPrefix())) {

                String userMetadataKey = caseInsensitiveName.substring(prefix.getLength());

                // Sanity check.   TODO - validate that no other improper characters for HTTP headers are present
                if (userMetadataKey.startsWith(ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX)) {
                    throw new HttpException(V2ErrorCode.INVALID_ATTRIBUTE_NAME,
                        "Illegal user attribute name starting with " + ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX,
                        request.path());
                }

                // Concatenate values for the same key (case-insensitive).
                String existingValue = userMetadataMap.putIfAbsent(userMetadataKey, entry.getValue());
                if (existingValue != null) {
                    userMetadataMap.put(userMetadataKey,
                            existingValue + CommonHeaders.HEADER_VALUE_SEPARATOR + entry.getValue());
                }

            } else {
                String contentHeaderKey  = PRESERVED_CONTENT_HEADERS.get(caseInsensitiveName);
                if (contentHeaderKey != null) {
                    userMetadataMap.put(contentHeaderKey, entry.getValue());
                } else if (throwWhenInvalid) {
                    //Key is neither prefixed nor content header
                    throw new HttpException(V2ErrorCode.INVALID_METADATA, "The metadata key \"" + entry.getKey() +
                        "\" must be prefixed with " + prefix, request.path());
                }
            }
        }
        // Immutable map will complain about value being null for a key. we need to return hashmap if
        // we want map for merging. key who have null values will be removed from source metadata.
        if (metadataMerge) {
            return userMetadataMap;
        } else {
            return ImmutableMap.copyOf(userMetadataMap);
        }
    }

    /**
     * Get the names and values of user-defined metadata that start with the user-defined
     * metadata prefix.
     * @param path the HTTP path of the request
     * @param prefix api specific user metadata header prefix.
     * @param entries the entries of key value pair to look at
     * @param throwWhenInvalid whether to throw an exception when metadata key does not start with prefix
     * @return a map of user-defined metadata, which may be empty (but won't be null).
     */
    public static Map<String, String> getUserMetadata(String path, UserMetadataPrefix prefix,
                                                      List<Map.Entry<String, List<String>>> entries,
                                                      boolean throwWhenInvalid,
                                                      boolean metadataMerge) {
        HashMap<String, String> userMetadataMap = new HashMap<>();

        for (Map.Entry<String, List<String>> entry : entries) {

            if (entry.getValue().isEmpty()) {
                continue;
            }

            String caseInsensitiveName = entry.getKey().toLowerCase(Locale.ENGLISH);

            if (caseInsensitiveName.startsWith(prefix.getPrefix())) {

                String userMetadataKey = caseInsensitiveName.substring(prefix.getLength());

                // Sanity check.   TODO - validate that no other improper characters for HTTP headers are present
                if (userMetadataKey.startsWith(ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX)) {
                    throw new HttpException(V2ErrorCode.INVALID_ATTRIBUTE_NAME,
                            "Illegal user attribute name starting with " + ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX,
                            path);
                }

                // Concatenate values for the same key (case-insensitive).
                String existingValue = userMetadataMap.putIfAbsent(userMetadataKey, entry.getValue().get(0));
                if (existingValue != null) {
                    userMetadataMap.put(userMetadataKey,
                            existingValue + CommonHeaders.HEADER_VALUE_SEPARATOR + entry.getValue().get(0));
                }

            } else {
                String contentHeaderKey  = PRESERVED_CONTENT_HEADERS.get(caseInsensitiveName);
                if (contentHeaderKey != null) {
                    userMetadataMap.put(contentHeaderKey, entry.getValue().get(0));
                } else if (throwWhenInvalid) {
                    //Key is neither prefixed nor content header
                    throw new HttpException(V2ErrorCode.INVALID_METADATA, "The metadata key \"" + entry.getKey() +
                            "\" must be prefixed with " + prefix, path);
                }
            }
        }
        // Immutable map will complain about value being null for a key. we need to return hashmap if
        // we want map for merging. key who have null values will be removed from source metadata.
        if (metadataMerge) {
            return userMetadataMap;
        } else {
            return ImmutableMap.copyOf(userMetadataMap);
        }
    }


    public static Map<String, String> getUserMetadataHeaders(HttpServerRequest request, UserMetadataPrefix prefix) {
        return getUserMetadata(request, prefix, request.headers().entries(), false, false);
    }


    public static Map<String, String> getUserMetadataHeaders(HttpServerRequest request) {
        return getUserMetadataHeaders(request, UserMetadataPrefix.V2);
    }

    public static Map<String, String> getUserMetadataHeaders(javax.ws.rs.core.HttpHeaders headers, String path) {
        return getUserMetadataHeaders(headers, UserMetadataPrefix.V2, path);
    }

    public static Map<String, String> getUserMetadataHeaders(javax.ws.rs.core.HttpHeaders headers, UserMetadataPrefix prefix, String path) {
        return getUserMetadata(path, prefix, new ArrayList<>(headers.getRequestHeaders().entrySet()), false, false);
    }

    // Utility method to populate Http header with object metadata
    public static void addUserMetadataToHttpHeaders(HttpServerResponse response,
                                                    ObjectMetadata objectMetadata) {
        addUserMetadataToHttpHeaders(response, objectMetadata, UserMetadataPrefix.V2);
    }

    // Utility method to populate Http header with object metadata
    public static void addUserMetadataToHttpHeaders(HttpServerResponse response,
                                                    ObjectMetadata objectMetadata,
                                                    UserMetadataPrefix metaHeaderPrefix) {
        addUserMetadataToHttpHeaders(response, objectMetadata.getMetadata(), metaHeaderPrefix);
    }

    // Utility method to populate Http header with object metadata
    public static void addUserMetadataToHttpHeaders(HttpServerResponse response,
                                                    Map<String, String> objectMetadata,
                                                    UserMetadataPrefix metaHeaderPrefix) {

        for (Map.Entry<String, String> entry : objectMetadata.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX)) {
                String preservedHeader = HttpHeaderHelpers.PRESERVED_CONTENT_HEADERS_LOOKUP.get(key);
                if (preservedHeader != null) {
                    response.putHeader(preservedHeader, entry.getValue());
                } else {
                    // Backwards-compatibility: a newer web-server has encoded
                    // a header that we do not understand. We will not return
                    // anything in this case.
                    LOG.warn("Unknown system header found in object metadata {}:{}", entry.getKey(), entry.getValue());
                }
            } else {
                response.putHeader(
                        metaHeaderPrefix + key, entry.getValue());
            }
        }

        //At this point, based on our spec we will default Content-Type to application/octet-stream if none provided
        if (!response.headers().contains(HttpHeaders.CONTENT_TYPE)) {
            response.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_OCTET_STREAM);
        }
    }

    /**
     * Title a word, specifically header keys, such that the first letter of the word is upper cased.
     * eg. content-length -> Content-Length
     *     content-type -> Content-Type
     *     location -> Location
     */
    public static String title(CharSequence val) {
        return WordUtils.capitalize(val.toString(), '-');
    }

    /**
     * Represents an HTTP header as a name and value.
     */
    public static final class Header {
        private final CharSequence name;
        private final String value;

        public Header(CharSequence name, String value) {
            this.name = name;
            this.value = value;
        }

        public CharSequence getName() {
            return name;
        }

        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Header header = (Header) o;
            return Objects.equals(getName(), header.getName()) &&
                    Objects.equals(getValue(), header.getValue());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getName(), getValue());
        }

        @Override
        public String toString() {
            return "Header{" +
                    "name='" + name + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }

    /**
     * An empty header which is ignored by methods like writeJsonResponse.
     */
    public static final Header NULL_HEADER = new Header(null, null);

    /**
     * Convenience method to create {@link Header} objects.
     * @param name the header name.
     * @param value the header value.
     * @return a {@link Header} object.
     */
    public static Header header(CharSequence name, String value) {
        return new Header(name, value);
    }

    /**
     * A convenience for creating a Location header using the absolute URI from an HTTP request.
     * @param request the HTTP request from which to get the absolute URI.
     * @param components extra components to add to the end of the Location header.
     * @return a {@link Header} with name "Location" and the full URI as the value.
     */
    public static Header locationHeader(HttpServerRequest request, String... components) {
        String baseURI = request.absoluteURI();
        // I actually couldn't find a standard library way of appending slashes only when necessary. --jfriedly.
        baseURI = baseURI.endsWith("/") ? baseURI : baseURI + "/";
        return header(HttpHeaders.LOCATION, baseURI + Joiner.on('/').join(components));
    }

    /**
     * A convenience for creating an ETag header.
     * @param etag the entity tag.
     * @return a {@link Header} object with "ETag" as the name and the entity tag as the value.
     */
    public static Header etagHeader(String etag) {
        return header(HttpHeaders.ETAG, etag);
    }

    /**
     * A convenience for creating an CacheControl header.
     * @param cacheControl the cachecontrol header tag.
     * @return a {@link Header} object with "CACHE_CONTROL" as the name and the cachecontrol tag as the value.
     */
    public static Header cacheControlHeader(String cacheControl) {
        return header(HttpHeaders.CACHE_CONTROL, cacheControl);
    }


    /**
     * A convenience for creating an last modified header.
     * @param lastModified the entity tag.
     * @return a {@link Header} object with "lastModified" as the name and the entity tag as the value.
     */
    public static Header lastModifiedHeader(String lastModified) {
        return header(HttpHeaders.LAST_MODIFIED, lastModified);
    }

    /**
     * A convenience for creating an "opc-next-page" header from a paginated list.
     * @param paginatedList the list from which to create the header.
     * @param keyFun the function to use to extract the cursor from type T.
     * @param <T> the type of object in the paginated list.
     * @return a {@link Header} that is either NULL_HEADER (if paginatedList.isTruncated() is true) or has name
     *         "opc-next-page" with the next cursor (the key from the last element in the list) as the value.
     */
    public static <T> Header opcNextPageHeader(PaginatedList<T> paginatedList, Function<T, String> keyFun) {
        if (!paginatedList.isTruncated() || paginatedList.getItems().isEmpty()) {
            return NULL_HEADER;
        }

        return nextPageHeader(keyFun.apply(paginatedList.getItems().get(paginatedList.getItems().size() - 1)));
    }

    /**
     * A convenience for creating an "opc-next-page" header from a nextPageToken.
     * @param nextPageToken the next page token.
     * @return a {@link Header} that is either NULL_HEADER or has name "opc-next-page".
     */
    public static Header nextPageHeader(@Nullable String nextPageToken) {
        return nextPageToken == null ? NULL_HEADER :
                header(CommonHeaders.OPC_NEXT_PAGE_HEADER, Obfuscate.obfuscate(nextPageToken));
    }

    /**
     * Helper method to validate byte range information.
     *
     * @param request the HTTP server request from which to read headers.
     */
    public static void validateRangeHeader(HttpServerRequest request) {
        Optional.ofNullable(request.getHeader(org.apache.http.HttpHeaders.RANGE))
                .ifPresent(rangeHeader -> {
                    boolean validate = ByteRange.isValidHeader(rangeHeader);
                    if (!validate) {
                        throw new RangeNotSatisfiableException("Invalid byte range passed");
                    }
                });
    }

    /**
     * Helper method to validate host header is not malformed.
     *
     * @param request the HTTP server request from which to read headers.
     */
    public static void ensureHostHeaderParseable(HttpServerRequest request) {
        try {
            String hostHeader = request.host();
            if (hostHeader != null) {
                // Check if scheme (any word which ends with ://) is present
                if (!hostHeader.toLowerCase().matches("^\\w+://.*")) {
                    // Scheme is mandatory in url string for creating the java.net.URL object.
                    String schemeToPrefix = request.scheme() != null ? request.scheme() : "http";
                    hostHeader = schemeToPrefix + "://" + hostHeader;
                }
                new java.net.URL(hostHeader);
            }
        } catch (MalformedURLException ex) {
            throw new BadRequestException("Invalid host header");
        }
    }

    @Nullable
    public static ByteRange tryParseByteRange(HttpServerRequest request, long totalSizeInBytes) {
        return HttpContentHelpers.tryParseByteRange(
                request.getHeader(org.apache.http.HttpHeaders.RANGE), totalSizeInBytes);
    }
}
