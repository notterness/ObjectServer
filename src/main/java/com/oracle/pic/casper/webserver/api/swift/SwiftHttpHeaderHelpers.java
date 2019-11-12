package com.oracle.pic.casper.webserver.api.swift;

import com.google.common.net.MediaType;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers.UserMetadataPrefix;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public final class SwiftHttpHeaderHelpers {

    private static final Logger logger = LoggerFactory.getLogger(SwiftHttpHeaderHelpers.class);

    private SwiftHttpHeaderHelpers() {
    }

    public static void negotiateUTF8Content(HttpServerRequest request, long maxSizeBytes) {
        String contentType = request.getHeader(HttpHeaders.CONTENT_TYPE);
        if (contentType != null) {
            MediaType mediaType = MediaType.parse(contentType);
            if (mediaType.charset().isPresent() && !mediaType.charset().get().equals(StandardCharsets.UTF_8)) {
                throw new HttpException(V2ErrorCode.INVALID_CONTENT_TYPE,
                    "The Content-Type must not specify charset, or have it set to 'UTF-8' (it was '" +
                        contentType + "')", request.path());
            }
        }

        String contentLenStr = request.getHeader(HttpHeaders.CONTENT_LENGTH);
        if (contentLenStr == null) {
            throw new HttpException(V2ErrorCode.CONTENT_LEN_REQUIRED, "The Content-Length header is required",
                request.path());
        }

        try {
            long contentLen = Long.parseLong(contentLenStr);
            if (contentLen > maxSizeBytes) {
                throw new HttpException(V2ErrorCode.ENTITY_TOO_LARGE,
                    "The Content-Length must be less than " + maxSizeBytes + " bytes (it was '" + contentLenStr + "')",
                    request.path());
            }

            if (contentLen < 0) {
                throw new HttpException(V2ErrorCode.INVALID_CONTENT_LEN,
                    "The Content-Length must be greater than or equal to zero (it was '" + contentLenStr + "'",
                    request.path());
            }
        } catch (NumberFormatException nex) {
            throw new HttpException(V2ErrorCode.INVALID_CONTENT_LEN,
                "The Content-Length header must contain a valid integer (it was '" + contentLenStr + "')",
                request.path(), nex);
        }
    }

    @Nullable
    public static String getContentMD5(HttpServerRequest request) {
        // Swift ignores the Content-MD5 header and uses the ETag header instead.
        return Optional.ofNullable(request.getHeader(HttpHeaders.ETAG))
                .map(etag -> {
                    try {
                        return ChecksumHelper.hexToBase64(etag);
                    } catch (DecoderException ex) {
                        throw new HttpException(V2ErrorCode.UNMATCHED_ETAG_MD5,
                            "Failed to decode hex-encoded string '" + etag + "'", request.path(), ex);
                    }
                }).orElse(null);
    }

    /**
     * Reads the metadata headers on a request into the update format understood by the {@link BucketBackend}.
     *
     * The Swift spec dictates that:
     *  * metadata headers that have an empty value should be deleted
     *  * metadata headers with the "X-Remove-Container-Meta-" prefix should be deleted
     *  * metadata headers that have non-empty values should be updated or created
     *  * metadata headers that exist but are not mentioned in a request should remain unchanged
     *
     * (See http://developer.openstack.org/api-ref-objectstorage-v1.html#updateContainerMeta)
     *
     * The {@link BucketBackend} spec dictates that:
     *  * metadata keys that have null values will be deleted
     *  * metadata keys that have non-null values will be updated or created
     *  * metadata keys that exist but are not mentioned in a request will remain unchanged
     *
     * (See {@link BucketBackend#updateBucketPartially}
     *
     * @param request request to update a container
     * @return A metadata map whose diff against the existing metadata will be used to update it.  Keys with null
     * values will be deleted, keys with values will be added / updated, and missing keys will be left unchanged.
     */
    @Nonnull
    public static Map<String, String> getUserContainerMetadataHeaders(HttpServerRequest request) {
        Map<String, String> updates = new HashMap<>();
        for (Map.Entry<String, String> headerPair : request.headers()) {
            String lowerCaseName = headerPair.getKey().toLowerCase(Locale.ENGLISH);
            String headerValue = headerPair.getValue();
            if (lowerCaseName.startsWith(UserMetadataPrefix.SWIFT_CONTAINER.getPrefix())) {
                String metadataKey = lowerCaseName.substring(UserMetadataPrefix.SWIFT_CONTAINER.getLength());
                if (headerValue.isEmpty()) {
                    updates.put(metadataKey, null);
                } else {
                    updates.put(metadataKey, headerValue);
                }
            } else if (lowerCaseName.startsWith(SwiftHeaders.RM_CONTAINER_META_PREFIX.toLowerCase(Locale.ENGLISH))) {
                String metadataKey = lowerCaseName.substring(SwiftHeaders.RM_CONTAINER_META_PREFIX.length());
                updates.put(metadataKey, null);
            }
        }
        return updates;
    }

    /**
     * Reads the metadata headers on a request into the format understood by {@link PutContainerHandler}
     *
     * The Swift spec for a PUT dictates that:
     *  * metadata headers that have an empty value should be deleted
     *  * metadata headers with the "X-Remove-Container-Meta-" prefix should be deleted
     *  * metadata headers that have non-empty values should be updated or created
     *  * metadata headers that exist but are not mentioned in a request should remain unchanged
     *
     * (See https://developer.openstack.org/api-ref/object-store/index.html#create-container)
     *
     * The {@link BucketBackend#createBucket} spec dictates that:
     *  * * All keys with empty values or the compartment-id key will be ignored, all others will be created.
     * The {@link BucketBackend#updateBucketPartially} spec dictates that:
     *  * * metadata keys that have empty values will be deleted
     *  * * metadata keys that have non-empty values will be updated or created
     *  * * metadata keys that exist but are not mentioned in a request will remain unchanged
     *
     * So this method examines the header key and :
     * * if prefixed by "X-Container-Meta-", treats it as a "create" and adds it to the map after stripping the prefix
     * * if prefixed by "X-Remove-Container-Meta-", treats it as a delete and adds it to the map with a blank
     * * for all other headers it just adds it as is
     *
     * @param request request to create/update a container
     * @return A metadata map used to create a new container, or update an existing container.
     */
    @Nonnull
    public static Map<String, String> getContainerMetadataHeadersForPut(HttpServerRequest request) {
        Map<String, String> metadata = new HashMap<>();
        for (Map.Entry<String, String> headerPair : request.headers()) {
            String lowerCaseName = headerPair.getKey().toLowerCase(Locale.ENGLISH);
            String headerValue = headerPair.getValue();

            if (lowerCaseName.startsWith(UserMetadataPrefix.SWIFT_CONTAINER.getPrefix())) {

                // ignore the compartment-id, as we do not want to set that to the container metadata
                if (lowerCaseName.equalsIgnoreCase(SwiftHeaders.COMPARTMENT_ID_HEADER)) {
                    continue;
                }

                String metadataKey = lowerCaseName.substring(UserMetadataPrefix.SWIFT_CONTAINER.getLength());
                metadata.put(metadataKey, headerValue);
            } else if (lowerCaseName.startsWith(SwiftHeaders.RM_CONTAINER_META_PREFIX.toLowerCase(Locale.ENGLISH))) {
                String metadataKey = lowerCaseName.substring(SwiftHeaders.RM_CONTAINER_META_PREFIX.length());
                metadata.put(metadataKey, "");
            }
        }
        return metadata;
    }

    /**
     * Get the names and values of user-defined metadata from HTTP request headers that start with the user-defined
     * metadata prefix.
     *
     * Swift details:
     * This method differs from its counterpart in {@link com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers}
     * in two ways:  1) the header prefix used by swift for metadata is different, and 2) our swift implementation
     * has special handling for the {@link SwiftHeaders#LARGE_OBJECT_HEADER}
     *
     * @param request the HTTP server request from which to read headers.
     * @return a map of user-defined metadata, which may be empty (but won't be null).
     */
    @Nonnull
    public static Map<String, String> getObjectMetadataHeaders(HttpServerRequest request) {

        final String largeObjectHeader = request.headers().get(SwiftHeaders.LARGE_OBJECT_HEADER);
        if (largeObjectHeader != null && largeObjectHeader.indexOf('/') <= 0) {
            throw new HttpException(V2ErrorCode.INVALID_MANIFEST_HEADER,
                    SwiftHeaders.LARGE_OBJECT_HEADER + " must be in <container>/<prefix> format.", request.path());
        }

        // remove the large object header so HttpHeaderHelpers does not complain it is not
        // prefixed
        request.headers().remove(SwiftHeaders.LARGE_OBJECT_HEADER);

        final Map<String, String> metadata = new HashMap<>(
                HttpHeaderHelpers.getUserMetadataHeaders(request, UserMetadataPrefix.SWIFT_OBJECT)
        );

        // rehydrate the request headers & metadata if its not null
        if (largeObjectHeader != null) {
            request.headers().add(SwiftHeaders.LARGE_OBJECT_HEADER, largeObjectHeader);
            metadata.put(SwiftHeaders.LARGE_OBJECT_HEADER, largeObjectHeader);
        }

        return Collections.unmodifiableMap(metadata);
    }

    /**
     * Utility method to populate the response with user metadata headers
     *
     * Swift details:
     * This method differs from its counterpart in {@link com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers}
     * in two ways:  1) the header prefix used by swift for metadata is different, and 2) our swift implementation
     * removes redundant header for large object header
     */
    public static void addUserObjectMetadataToHttpHeaders(HttpServerResponse response,
                                                          ObjectMetadata objectMetadata) {
        Map<String, String> metadata = new HashMap<>(objectMetadata.getMetadata());
        String largeObjectHeader = metadata.remove(SwiftHeaders.LARGE_OBJECT_HEADER.toLowerCase());

        HttpHeaderHelpers.addUserMetadataToHttpHeaders(response, metadata, UserMetadataPrefix.SWIFT_OBJECT);

        if (largeObjectHeader != null) {
            response.headers().add(SwiftHeaders.LARGE_OBJECT_HEADER, largeObjectHeader);
        }
    }

    public static void addUserContainerMetadataToHttpHeaders(HttpServerResponse response,
                                                          Map<String, String> objectMetadata) {
        HttpHeaderHelpers.addUserMetadataToHttpHeaders(response, objectMetadata, UserMetadataPrefix.SWIFT_CONTAINER);

        //special handling for large object header -- remove it from the user map
        response.headers().remove(UserMetadataPrefix.SWIFT_CONTAINER + SwiftHeaders.LARGE_OBJECT_HEADER);
    }

    /**
     * Add object-specific headers to a response.
     *
     * Swift returns all object-specific headers (ETag, Last-Modified, Content-Type, X-Timestamp, and custom user
     * metadata) on GET and HEAD, even when the response is an error or 304 (but not 404, of course).
     *
     * This method is also responsible for decoding base64-encoded MD5 hashes from the Backend into swift-style 32
     * character hex strings.
     */
    // TODO(jfriedly):  document the three different timestamp headers
    public static void writeObjectHeaders(HttpServerResponse response, ObjectMetadata objectMetadata) {
        objectMetadata.getChecksum().addHeaderToResponseSwift(response);
        response.putHeader(HttpHeaders.LAST_MODIFIED, DateUtil.httpFormattedDate(objectMetadata.getModificationTime()));
        addUserObjectMetadataToHttpHeaders(response, objectMetadata);
    }

    /**
     * Headers that should be sent back on every single response, even errors.
     */
    public static void writeCommonHeaders(RoutingContext routingContext) {
        HttpServerResponse response = routingContext.response();

        String transactionId = WSRequestContext.getCommonRequestContext(routingContext).getOpcRequestId();
        response.putHeader(SwiftHeaders.TRANS_ID, transactionId);
        // Swift formats the X-Timestamp header as a floating point seconds-since-the-epoch number with 10ns precision.
        // We don't actually have that level of precision on our clocks (but nevermind that, neither do they)
        // Our timestamps will always have the last two digits as zeroes.
        String xTimestamp = String.format("%.5f", ((double) System.currentTimeMillis()) / 1000);
        response.putHeader(SwiftHeaders.TIMESTAMP, xTimestamp);

        response.putHeader(HttpHeaders.DATE, DateUtil.httpFormattedDate(new Date()));
    }
}
