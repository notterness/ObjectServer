package com.oracle.pic.casper.webserver.api.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.net.MediaType;
import com.oracle.bmc.objectstorage.model.CopyPartDetails;
import com.oracle.bmc.objectstorage.model.CreateReplicationPolicyDetails;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.MultipartLimit;
import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.ContentLengthRequiredException;
import com.oracle.pic.casper.common.exceptions.InvalidContentLengthException;
import com.oracle.pic.casper.common.exceptions.InvalidContentTypeException;
import com.oracle.pic.casper.common.exceptions.ObjectTooLargeException;
import com.oracle.pic.casper.common.exceptions.RangeNotSatisfiableException;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.rest.CommonHeaders;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.CrossRegionUtils;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.common.vertx.stream.AbortableReadStream;
import com.oracle.pic.casper.common.vertx.stream.MeasuringReadStream;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.ETagType;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.ParSigningHelper;
import com.oracle.pic.casper.webserver.api.model.Bucket;
import com.oracle.pic.casper.webserver.api.model.BucketCreate;
import com.oracle.pic.casper.webserver.api.model.BucketFilter;
import com.oracle.pic.casper.webserver.api.model.BucketUpdate;
import com.oracle.pic.casper.webserver.api.model.BulkRestoreRequestJson;
import com.oracle.pic.casper.webserver.api.model.CopyRequestJson;
import com.oracle.pic.casper.webserver.api.model.CreatePreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.CreateUploadRequest;
import com.oracle.pic.casper.webserver.api.model.FinishUploadRequest;
import com.oracle.pic.casper.webserver.api.model.FinishUploadRequest.PartAndETag;
import com.oracle.pic.casper.webserver.api.model.NamespaceMetadata;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.PreAuthenticatedRequestMetadata;
import com.oracle.pic.casper.webserver.api.model.PutObjectLifecyclePolicyDetails;
import com.oracle.pic.casper.webserver.api.model.ReencryptObjectDetails;
import com.oracle.pic.casper.webserver.api.model.RenameRequest;
import com.oracle.pic.casper.webserver.api.model.RestoreObjectsDetails;
import com.oracle.pic.casper.webserver.api.model.UpdateBucketOptionsRequestJson;
import com.oracle.pic.casper.webserver.api.model.UpdateObjectMetadataDetails;
import com.oracle.pic.casper.webserver.api.model.UploadIdentifier;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidNamespaceMetadataException;
import com.oracle.pic.casper.webserver.api.model.exceptions.MissingBucketNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.MissingCompartmentIdException;
import com.oracle.pic.casper.webserver.api.model.serde.BucketCreateJson;
import com.oracle.pic.casper.webserver.api.model.serde.BucketUpdateJson;
import com.oracle.pic.casper.webserver.api.model.serde.InvalidBucketJsonException;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorderReadStream;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetricsBundle;
import com.oracle.pic.casper.webserver.vertx.HttpServerRequestReadStream;
import com.oracle.pic.commons.util.Region;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.streams.Pump;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.util.URIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static javax.measure.unit.NonSI.BYTE;

/**
 * Helper methods for dealing with HTTP content negotation, MD5 checking and entity serialization and deserialization.
 */
public final class HttpContentHelpers {

    private static final Logger LOG = LoggerFactory.getLogger(HttpContentHelpers.class);

    public static final String CONTENT_TYPE = "contentType";
    public static final String CONTENT_LANGUAGE = "contentLanguage";
    public static final String CONTENT_ENCODING = "contentEncoding";
    public static final String CACHE_CONTROL = "cacheControl";
    public static final String CONTENT_DISPOSITION = "contentDisposition";

    public static final Map<String, String> PRESERVED_CONTENT_HEADERS = ImmutableMap.of(
        CONTENT_TYPE, ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX + "^T",
        CONTENT_LANGUAGE, ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX + "^L",
        CONTENT_ENCODING, ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX + "^E",
        CACHE_CONTROL, ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX + "^C",
        CONTENT_DISPOSITION, ObjectMetadata.EXTENDED_USER_DATA_KEY_PREFIX + "^D");

    private HttpContentHelpers() {

    }

    /**
     * Perform simple HTTP content negotiation for application/json content in an HTTP request.
     *
     * The relevant documentation for HTTP content negotiation is primarily at these links:
     *
     *  https://www.w3.org/Protocols/rfc2616/rfc2616-sec7.html#sec7
     *  https://www.w3.org/Protocols/rfc2616/rfc2616-sec12.html#sec12
     *  https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14
     *
     * This method does the following:
     *
     *  1. If Content-Type is not set, it is assumed to be "application/json".
     *  2. If Content-Type is set to something other than "application/json" it is an HTTP 415 (Unsupported Media Type)
     *     error.
     *  3. If Content-Encoding is set and is anything other than "identity" it is an HTTP 415 error.
     *  4. If Content-Length is not set it is an HTTP 411 (Length Required) error.
     *  5. If Content-Length is greater than the maximum it is an HTTP 413 (Payload Too Large) error.
     *  6. If Content-Length is set to something that isn't numeric it is an HTTP 400 error.
     *
     * Errors are signalled using subclasses of {@link HttpException}.
     *
     * @param request the HTTP request used to inspect header values.
     */
    public static void negotiateApplicationJsonContent(HttpServerRequest request, long maxSizeBytes) {
        String contentType = request.getHeader(HttpHeaders.CONTENT_TYPE);
        if (contentType != null) {
            MediaType mediaType = MediaType.parse(contentType);
            Charset charset = mediaType.charset().or(StandardCharsets.UTF_8);

            if (!charset.equals(StandardCharsets.UTF_8)) {
                throw new HttpException(V2ErrorCode.INVALID_CONTENT_TYPE,
                    "The Content-Type must not specify charset, or have it set to 'UTF-8' (it was '" + contentType +
                        "')", request.path());
            }

            if (!isApplicationOrText(mediaType) || !mediaType.subtype().equalsIgnoreCase("json")) {
                throw new HttpException(V2ErrorCode.INVALID_CONTENT_TYPE,
                    "The Content-Type must be \"application/json\" (it was '" + contentType + "')", request.path());
            }
        }

        String contentEncoding = request.getHeader(HttpHeaders.CONTENT_ENCODING);
        if (contentEncoding != null && !contentEncoding.equalsIgnoreCase("identity")) {
            throw new HttpException(V2ErrorCode.INVALID_CONTENT_ENC,
                "The Content-Encoding must be \"identity\" or omitted from the request (it was '" + contentEncoding +
                    "')", request.path());
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
                    request.path()
                );
            }
        } catch (NumberFormatException nex) {
            throw new HttpException(V2ErrorCode.INVALID_CONTENT_LEN,
                "The Content-Length header must contain a valid integer (it was '" + contentLenStr + "')",
                request.path());
        }
    }

    public static void negotiateApplicationXmlContent(HttpServerRequest request) {
        String contentType = request.getHeader(HttpHeaders.CONTENT_TYPE);
        if (contentType != null) {
            MediaType mediaType = MediaType.parse(contentType);
            Charset charset = mediaType.charset().or(StandardCharsets.UTF_8);

            if (!charset.equals(StandardCharsets.UTF_8)) {
                throw new InvalidContentTypeException("The Content-Type must not specify charset, " +
                    "or have it set to 'UTF-8' (it was '" + contentType + "')");
            }

            if (!isApplicationOrText(mediaType) || !mediaType.subtype().equalsIgnoreCase("xml")) {
                throw new InvalidContentTypeException("The Content-Type must be \"application/xml\" (it was '" +
                    contentType + "')");
            }
        }
        validateContentLengthHeader(request);

    }

    public static void validateContentLengthHeader(HttpServerRequest request) {
        String contentLenStr = request.getHeader(HttpHeaders.CONTENT_LENGTH);
        if (contentLenStr == null) {
            throw new ContentLengthRequiredException("The Content-Length header is required");
        }
    }

    /**
     * Perform simple HTTP content negotiation for storage object content in an HTTP request.
     *
     * See {@link #negotiateApplicationJsonContent(HttpServerRequest, long)} for links to content negotiation docs.
     *
     * This method does the following:
     *
     *  1. The Content-Type is ignored (see {@link com.oracle.pic.casper.webserver.api.v2.PutObjectHandler} for the
     *     default value if none is provided by the client).
     *  2. If the Content-Length is not provided a 411 (Length Required) error is thrown.
     *  3. If the Content-Length is too large a 413 (Payload Too Large) error is thrown.
     *  4. If the Content-Length is set to something that is less than the minimum byte size,
     *     a 400 (Bad Request) error is thrown.
     *
     * Errors are {@link ContentLengthRequiredException}, {@link ObjectTooLargeException}
     * or {@link InvalidContentLengthException}.
     */
    public static void negotiateStorageObjectContent(HttpServerRequest request, long minSizeByte, long maxSizeBytes) {
        String contentLenStr = request.getHeader(HttpHeaders.CONTENT_LENGTH);
        if (contentLenStr == null) {
            throw new ContentLengthRequiredException("The Content-Length header is required");
        }

        try {
            long contentLen = Long.parseLong(contentLenStr);
            if (contentLen > maxSizeBytes) {
                throw new ObjectTooLargeException("The Content-Length must be less than " + maxSizeBytes +
                    " bytes (it was '" + contentLenStr + "')");
            }

            if (contentLen < minSizeByte) {
                throw new InvalidContentLengthException("The Content-Length must be greater than or equal to " +
                    minSizeByte + " (it was '" + contentLenStr + "')");
            }
        } catch (NumberFormatException nfex) {
            throw new InvalidContentLengthException("The Content-Length must be a valid integer (it was '" +
                contentLenStr + "')");
        }
    }

    /**
     * If the request includes "Expect: 100-continue", send a 100 continue to the client, otherwise do nothing.
     *
     * @param request  the HTTP request from which to get the header.
     * @param response the HTTP response on which to send the 100 continue.
     */
    public static void write100Continue(HttpServerRequest request, HttpServerResponse response) {
        String expect = request.getHeader(HttpHeaders.EXPECT);
        if (expect != null && expect.equalsIgnoreCase(HttpHeaders.CONTINUE.toString())) {
            response.writeContinue();
        }
    }

    public static <T> void writeJsonResponse(
        HttpServerRequest request,
        HttpServerResponse response,
        T content,
        ObjectMapper mapper,
        HttpHeaderHelpers.Header... headers
    ) {
        writeJsonResponse(request, response, content, mapper, null, headers);
    }

    /**
     * Writes an HTTP response with a serialized JSON object in the body.
     *
     * This write out an HTTP response with the content object serialized as JSON in the body. The content may have some
     * field(s) filtered, if there's a filter provided.
     * The HTTP response has the following headers:
     *  - Content-Type: "application/json".
     *  - Content-Length: body length.
     *
     * Note that this method ends the response, so you must not call end() on the response after this method returns.
     *
     * @param response the HTTP response object used to write the response.
     * @param content  the object to serialize to the body of the response.
     * @param mapper   the Jackson ObjectMapper used to perform the serialization.
     * @param filter   the JSON filter used to include only specific fields.
     * @param <T>      the class of the content object.
     */
    public static <T> void writeJsonResponse(
            HttpServerRequest request,
            HttpServerResponse response,
            T content,
            ObjectMapper mapper,
            @Nullable FilterProvider filter,
            HttpHeaderHelpers.Header... headers
    ) {
        try {
            String body;
            if (filter != null) {
                ObjectMapper copyMapper = mapper.copy();
                copyMapper.addMixIn(Bucket.class, BucketFilter.class);
                body = copyMapper.writer(filter).writeValueAsString(content);
            } else {
                body = mapper.writeValueAsString(content);
            }
            response.setStatusCode(HttpResponseStatus.OK);
            response.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON);
            for (HttpHeaderHelpers.Header header : headers) {
                if (header != HttpHeaderHelpers.NULL_HEADER) {
                    response.putHeader(header.getName(), header.getValue());
                }
            }
            response.end(body);
        } catch (IOException ioex) {
            throw new HttpException(V2ErrorCode.INTERNAL_SERVER_ERROR, "Internal server error", request.path(), ioex);
        }
    }

    /**
     * Read the body of an HTTP request, parse it as a UTF-8 encoded JSON string into a {@link BucketCreate} object with
     * the given namespace and createdBy fields.
     *
     * This method checks the Content-MD5 header if it is present.
     *
     * When any of the following conditions are true, this method throws an appropriate HttpException:
     *
     *  - The bytes cannot be parsed as a valid JSON object.
     *  - The JSON object is missing a "name" property.
     *  - The JSON object is missing a "compartmentId" property.
     *  - The JSON object has a "namespace" property that is not identical to the namespace passed to this method.
     *  - The bucket name is invalid (as per our spec for naming buckets).
     *  - The metadata for the bucket is too arge (as per our spec for total user-defined metadata size).
     *
     * @param request the HTTP request from which the body was read, used for accessing headers.
     * @param mapper the Jackson object mapper to convert the JSON data to a BucketCreate object.
     * @param namespace the namespace, typically read from the URL in the request.
     * @param createdBy the user who is creating the bucket, read from authorization headers.
     * @param bytes the raw bytes of the HTTP request.
     * @return a well-formed BucketCreate.
     */
    public static BucketCreate readBucketCreateContent(
            HttpServerRequest request, ObjectMapper mapper, String namespace, String createdBy, byte[] bytes) {
        ChecksumHelper.checkContentMD5(request, bytes);

        final BucketCreate bucketCreate;
        try {
            bucketCreate = BucketCreateJson.bucketCreateFromJson(namespace, createdBy, bytes, mapper);
        } catch (InvalidBucketJsonException ibuex) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, request.path(), ibuex);
        } catch (MissingBucketNameException mbnex) {
            throw new HttpException(V2ErrorCode.MISSING_BUCKET_NAME,
                "The JSON request entity must contain a \"name\" property with the name of the bucket", request.path(),
                mbnex);
        } catch (MissingCompartmentIdException mciex) {
            throw new HttpException(V2ErrorCode.MISSING_COMPARTMENT,
                "The JSON request entity must contain a \"compartmentId\" property with the compartment OCID",
                request.path(), mciex);
        } catch (IllegalArgumentException iae) {
            throw new HttpException(V2ErrorCode.INVALID_BUCKET_PROPERTY, request.path(), iae);
        }

        if (!bucketCreate.getNamespaceName().equals(namespace)) {
            throw new HttpException(V2ErrorCode.MISMATCHED_NAMESPACE_NAMES,
                "The JSON request entity contains a \"namespace\" property (" +
                    bucketCreate.getNamespaceName() + ") that does not match the namespace in the URL (" + namespace +
                    ")", request.path());
        }

        return bucketCreate;
    }

    /**
     * Parse a JSON object of metadata into map of String keys to String values
     */
    private static Map<String, String> parseMetadata(HttpServerRequest request, JsonNode jsonNode) {
        if (jsonNode.getNodeType() == JsonNodeType.NULL) {
            return new HashMap<>();
        } else if (jsonNode.getNodeType() != JsonNodeType.OBJECT) {
            throw new HttpException(V2ErrorCode.INVALID_METADATA, "Object metadata must be a JSON object",
                request.path());
        }
        List<Map.Entry<String, String>> metadataList = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> metaIterator = jsonNode.fields();
        while (metaIterator.hasNext()) {
            Map.Entry<String, JsonNode> metaField = metaIterator.next();
            String metaKey = metaField.getKey();
            JsonNode metaValue = metaField.getValue();
            if (metaValue.getNodeType() != JsonNodeType.STRING) {
                throw new HttpException(V2ErrorCode.INVALID_METADATA,
                    "The metadata value of \"" + metaKey + "\" must be a string", request.path());
            }
            metadataList.add(new AbstractMap.SimpleImmutableEntry<>(metaKey, metaValue.asText()));
        }
        return HttpHeaderHelpers.getUserMetadata(request, HttpHeaderHelpers.UserMetadataPrefix.V2, metadataList,
                true, false);
    }

    /**
     * Parse a JSON object of metadata into map of String keys to String values.
     * if value for key is not present then add null. Immutable map will complain about null
     * so we need a separate function to segregate normal object metadata vs metadata for merge.
     */
    private static Map<String, String> parseMetadataForMerge(HttpServerRequest request, JsonNode jsonNode) {
        if (jsonNode.getNodeType() == JsonNodeType.NULL) {
            return new HashMap<>();
        } else if (jsonNode.getNodeType() != JsonNodeType.OBJECT) {
            throw new HttpException(V2ErrorCode.INVALID_METADATA, "Object metadata must be a JSON object",
                request.path());
        }
        List<HashMap.Entry<String, String>> metadataList = new ArrayList<>();
        Iterator<HashMap.Entry<String, JsonNode>> metaIterator = jsonNode.fields();
        while (metaIterator.hasNext()) {
            HashMap.Entry<String, JsonNode> metaField = metaIterator.next();
            String metaKey = metaField.getKey();
            String metaValueText;
            JsonNode metaValue = metaField.getValue();
            if (metaValue == NullNode.getInstance()) {
                metaValueText = null;
            } else if (metaValue.getNodeType() != JsonNodeType.STRING) {
                throw new HttpException(V2ErrorCode.INVALID_METADATA,
                    "The metadata value of \"" + metaKey + "\" must be a string", request.path());
            } else {
                metaValueText = metaValue.asText();
            }
            metadataList.add(new HashMap.SimpleImmutableEntry<>(metaKey, metaValueText));
        }
        return HttpHeaderHelpers.getUserMetadata(request, HttpHeaderHelpers.UserMetadataPrefix.V2, metadataList,
                true, true);
    }

    /**
     * Read the body of an HTTP request as a UTF-8 encoded JSON string with object field.
     *
     * This method checks the Content-MD5 header if it is present.
     */
    public static CreateUploadRequest readCreateUploadContent(
            HttpServerRequest request, ObjectMapper mapper, String namespace, String bucket,
            String ifMatch, String ifNoneMatch, byte[] bytes) {
        ChecksumHelper.checkContentMD5(request, bytes);
        String object = null;
        Map<String, String> metadata = Maps.newHashMap();
        JsonNode node;
        try {
            node = mapper.readTree(bytes);
            if (node == null) {
                throw new IOException();
            }
        } catch (IOException e) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Could not parse body as a JSON object.", request.path(),
                e);
        }
        if (!node.isObject()) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "The string must be a JSON object", request.path());
        }
        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = node.fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldsIterator.next();
            String key = field.getKey();
            JsonNode value = field.getValue();
            switch (key) {
                case "object":
                    if (value.getNodeType() != JsonNodeType.STRING) {
                        throw new HttpException(V2ErrorCode.INVALID_JSON, "Object name must be a string",
                            request.path());
                    }
                    object = value.asText();
                    break;
                case "contentType":
                case "contentLanguage":
                case "cacheControl":
                case "contentDisposition":
                case "contentEncoding":
                    if (value.getNodeType() == JsonNodeType.STRING) {
                        metadata.put(PRESERVED_CONTENT_HEADERS.get(key), value.asText());
                    } else if (value.getNodeType() != JsonNodeType.NULL) {
                        throw new HttpException(V2ErrorCode.INVALID_JSON, "Object " + key + " must be a string",
                            request.path());
                    }
                    break;
                case "metadata":
                    metadata.putAll(parseMetadata(request, value));
                    break;
                default:
                    throw new HttpException(V2ErrorCode.INVALID_JSON, "Unknown Json field: " + field.getKey(),
                        request.path());
            }
        }
        if (object == null) {
            throw new HttpException(V2ErrorCode.MISSING_OBJECT_NAME, "Missing object field in JSON", request.path());
        }
        return new CreateUploadRequest(namespace, bucket, object, metadata, ifMatch, ifNoneMatch);
    }

    public static FinishUploadRequest readCommitUploadContent(
            HttpServerRequest request,
            UploadIdentifier uploadIdentifier,
            ObjectMapper mapper, byte[] bytes, String ifMatch, String ifNoneMatch) {
        ChecksumHelper.checkContentMD5(request, bytes);

        JsonNode node;
        try {
            node = mapper.readTree(bytes);
        } catch (IOException e) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Could not parse body as a JSON object.", request.path(),
                e);
        }
        if (!node.isObject()) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "The string must be a JSON object", request.path());
        }
        boolean partsToCommitExists = false;
        boolean partsToExcludeExists = false;
        final TreeMap<Integer, PartAndETag> partsToCommit = new TreeMap<>();
        final TreeSet<Integer> partsToExclude = new TreeSet<>();

        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = node.fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldsIterator.next();
            if (field.getKey().equals("partsToCommit")) {
                partsToCommitExists = true;
                if (!field.getValue().isArray()) {
                    throw new HttpException(V2ErrorCode.INVALID_JSON, "JSON \"partsToCommit\" value must be an array",
                        request.path());
                }
                Iterator<JsonNode> partsIterator = field.getValue().elements();
                while (partsIterator.hasNext()) {
                    FinishUploadRequest.PartAndETag partAndETag;
                    try {
                        partAndETag = mapper.treeToValue(partsIterator.next(), FinishUploadRequest.PartAndETag.class);
                    } catch (JsonProcessingException e) {
                        throw new HttpException(V2ErrorCode.INVALID_JSON, "Could not parse partsToCommit from JSON",
                            request.path(), e);
                    }
                    int partNum = partAndETag.getUploadPartNum();
                    if (!MultipartLimit.isWithinRange(partNum)) {
                        throw new HttpException(V2ErrorCode.INVALID_UPLOAD_PART, MultipartLimit.PART_NUM_ERROR_MESSAGE,
                            request.path());
                    }
                    if (partsToCommit.containsKey(partNum)) {
                        throw new HttpException(V2ErrorCode.INVALID_UPLOAD_PART,
                            "Part " + partNum + "is duplicated in partsToCommit", request.path());
                    }
                    partsToCommit.put(partAndETag.getUploadPartNum(), partAndETag);
                }
            } else if (field.getKey().equals("partsToExclude")) {
                if (field.getValue().isArray()) {
                    partsToExcludeExists = true;
                    Iterator<JsonNode> partsIterator = field.getValue().elements();
                    while (partsIterator.hasNext()) {
                        JsonNode partNumNode = partsIterator.next();
                        if (!partNumNode.isInt()) {
                            throw new HttpException(V2ErrorCode.INVALID_JSON,
                                "JSON \"partsToExclude\" array elements must be integers", request.path());
                        }
                        int partNum = partNumNode.asInt();
                        if (!MultipartLimit.isWithinRange(partNum)) {
                            throw new HttpException(V2ErrorCode.INVALID_UPLOAD_PART,
                                MultipartLimit.PART_NUM_ERROR_MESSAGE, request.path());
                        }
                        if (partsToExclude.contains(partNum)) {
                            throw new HttpException(V2ErrorCode.INVALID_UPLOAD_PART,
                                "Part " + partNum + "is duplicated in partsToExclude", request.path());
                        }
                        partsToExclude.add(partNum);
                    }
                } else if (!field.getValue().isNull()) {
                    throw new HttpException(V2ErrorCode.INVALID_JSON, "JSON \"partsToExclude\" value must be an array",
                        request.path());
                }
            } else {
                throw new HttpException(V2ErrorCode.INVALID_JSON, "Unknown Json field: " + field.getKey(),
                    request.path());
            }
        }

        // partsToCommit is required
        if (!partsToCommitExists) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "partsToCommit is expected", request.path());
        }
        if (partsToCommit.isEmpty()) {
            throw new HttpException(V2ErrorCode.INVALID_UPLOAD_PART, "There are no parts to commit", request.path());
        }

        // partsToExclude is optional and default to be empty
        if (!partsToExcludeExists) {
            Preconditions.checkState(partsToExclude.isEmpty());
        }

        for (int partNum : partsToExclude) {
            if (partsToCommit.containsKey(partNum)) {
                throw new HttpException(V2ErrorCode.INVALID_UPLOAD_PART,
                    "Part " + partNum + " is in both partsToCommit and partsToExclude", request.path());
            }
        }

        return new FinishUploadRequest(
                uploadIdentifier,
                ETagType.ETAG,
                ImmutableSortedSet.copyOf(partsToCommit.values()),
                ImmutableSortedSet.copyOf(partsToExclude),
                ifMatch, ifNoneMatch);
    }

    /**
     * Read the body of an HTTP request, parse it as a UTF-8 encoded JSON string as a {@link BucketUpdate} with the
     * given namespace and bucket names.
     *
     * This method checks the Content-MD5 header if it is present.
     *
     * When any of the following conditions are true, this method throws an appropriate HttpException:
     *
     *  - The bytes cannot be parsed as valid JSON.
     *  - The JSON object contains a "namespace" property which is not the same as the namespace argument.
     *  - The JSON object contains a "name" property which is not the same as the bucketName argument.
     *  - The "metadata" property in the JSON object is too large.
     *  - The bucket name is invalid (as per the rules for bucket names)
     *
     * @param request    the HTTP request from which the body was read, used to get headers.
     * @param mapper     the Jackson object mapper to convert the JSON to a BucketUpdate.
     * @param namespace  the namespace, typically read from the URL in the request.
     * @param bucketName the bucket name, typically read from the URL in the request.
     * @param bytes      the raw bytes of the HTTP request body which must be a JSON object encoded in UTF-8.
     * @return a well-formed BucketUpdate.
     */
    public static BucketUpdate readBucketUpdateContent(
            HttpServerRequest request, ObjectMapper mapper, String namespace, String bucketName, byte[] bytes) {
        ChecksumHelper.checkContentMD5(request, bytes);

        final BucketUpdate bucketUpdate;
        try {
            bucketUpdate = BucketUpdateJson.bucketUpdateFromJson(namespace, bucketName, bytes, mapper
            );
        } catch (InvalidBucketJsonException ibuex) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, request.path(), ibuex);
        } catch (MissingBucketNameException mbnex) {
            throw new HttpException(V2ErrorCode.MISSING_BUCKET_NAME,
                "The JSON request entity must contain a \"name\" property with the name of the bucket", request.path(),
                mbnex);
        } catch (IllegalArgumentException iae) {
            throw new HttpException(V2ErrorCode.INVALID_BUCKET_PROPERTY, request.path(), iae);
        }

        if (!bucketUpdate.getNamespaceName().equals(namespace)) {
            throw new HttpException(V2ErrorCode.MISMATCHED_NAMESPACE_NAMES,
                "The JSON request entity contains a \"namespace\" property (" +
                    bucketUpdate.getNamespaceName() + ") that does not match the namespace in the URL (" +
                    namespace + ")", request.path());
        }

        if (!bucketUpdate.getBucketName().equals(bucketName)) {
            throw new HttpException(V2ErrorCode.MISMATCHED_BUCKET_NAMES,
                "The JSON request entity contains a \"name\" property (" + bucketUpdate.getBucketName() +
                    ") that does not match the bucket name in the URL (" + bucketName + ")", request.path());
        }

        return bucketUpdate;
    }

    /**
     * Read the body of an HTTP request, parse it as a UTF-8 encoded JSON string as a {@link RenameRequest} with the
     * given namespace object names and bucket name.
     *
     * @param request    the HTTP request from which the body was read, used to get headers.
     * @param mapper     the Jackson object mapper to convert the JSON to a RenameRequest.
     * @param bytes      the raw bytes of the HTTP request body which must be a JSON object encoded in UTF-8.
     * @return a well-formed RenameRequest.
     */
    public static RenameRequest readRenameContent(HttpServerRequest request, ObjectMapper mapper,
                                                  byte[] bytes)  throws Exception {
        ChecksumHelper.checkContentMD5(request, bytes);
        RenameRequest renameRequest;
        try {
            renameRequest = mapper.readValue(bytes, RenameRequest.class);
        } catch (Exception ex) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Could not parse body as valid rename object request.",
                request.path(), ex);
        }
        if (renameRequest != null && renameRequest.getSourceName().equals(renameRequest.getNewName())) {
            throw new HttpException(V2ErrorCode.SOURCE_SAME_AS_NEW, "Source name should not be the same as new name",
                request.path());
        }
        return renameRequest;
    }

    public static UpdateObjectMetadataDetails getUpdateObjectMetadataDetails(HttpServerRequest request,
                                                                             ObjectMapper mapper,
                                                                             byte[] bytes,
                                                                             boolean mergeMetadata) {
        ChecksumHelper.checkContentMD5(request, bytes);

        Map<String, String> metadata = Maps.newHashMap();
        boolean isMissingMetadata = true;

        JsonNode node;
        try {
            node = mapper.readTree(bytes);
        } catch (IOException e) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Could not parse body as a JSON object.", request.path(),
                e);
        }
        if (!node.isObject()) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "The string must be a JSON object", request.path());
        }
        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = node.fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldsIterator.next();
            String key = field.getKey();
            JsonNode value = field.getValue();
            switch (key) {
                case "metadata":
                    if (value != NullNode.getInstance()) {
                        if (mergeMetadata) {
                            metadata.putAll(parseMetadataForMerge(request, value));
                        } else {
                            metadata.putAll(parseMetadata(request, value));
                        }
                        isMissingMetadata = false;
                    }
                    break;
                default:
                    throw new HttpException(V2ErrorCode.INVALID_JSON, "Unknown Json field: " + field.getKey(),
                        request.path());
            }
        }

        if (isMissingMetadata) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "metadata param is missing", request.path());
        }

        final UpdateObjectMetadataDetails updateObjectMetadataDetails = new UpdateObjectMetadataDetails(metadata);

        return updateObjectMetadataDetails;
    }

    public static BulkRestoreRequestJson readBulkRestoreRequestJson(HttpServerRequest request, ObjectMapper mapper,
                                                                    byte[] bytes) {
        ChecksumHelper.checkContentMD5(request, bytes);
        final BulkRestoreRequestJson bulkRestoreRequestJson;
        try {
            bulkRestoreRequestJson = mapper.readValue(bytes, BulkRestoreRequestJson.class);
        } catch (Exception e) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Could not parse body as valid bulk restore request.",
                    request.path(), e);
        }
        if (bulkRestoreRequestJson.getInclusionPatterns() == null) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Inclusion patterns list is missing.", request.path());
        }
        return bulkRestoreRequestJson;
    }

    public static CopyRequestJson readCopyRequestJson(HttpServerRequest request, ObjectMapper mapper, byte[] bytes) {
        ChecksumHelper.checkContentMD5(request, bytes);
        final CopyRequestJson copyRequestJson;
        try {
            copyRequestJson = mapper.readValue(bytes, CopyRequestJson.class);
        } catch (Exception e) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Could not parse body as valid copy object request.",
                request.path(), e);
        }
        if (copyRequestJson.getSourceObjectName() == null) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Source object name is missing.", request.path());
        } else if (copyRequestJson.getDestinationRegion() == null) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Destination region is missing.", request.path());
        } else if (copyRequestJson.getDestinationNamespace() == null) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Destination namespace is missing.", request.path());
        } else if (copyRequestJson.getDestinationBucket() == null) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Destination bucket is missing.", request.path());
        } else if (copyRequestJson.getDestinationObjectName() == null) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Destination object name is missing.", request.path());
        }
        HttpMatchHelpers.validateConditionalHeaders(request,
            copyRequestJson.getSourceObjectIfMatchETag(),
            null,
            HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
            HttpMatchHelpers.IfNoneMatchAllowed.STAR_ONLY);
        HttpMatchHelpers.validateConditionalHeaders(request,
            copyRequestJson.getDestinationObjectIfMatchETag(),
            copyRequestJson.getDestinationObjectIfNoneMatchETag(),
            HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
            HttpMatchHelpers.IfNoneMatchAllowed.STAR_ONLY);
        return copyRequestJson;
    }

    public static UpdateBucketOptionsRequestJson readUpdateBucketOptionsRequestJson(HttpServerRequest request,
                                                                                    ObjectMapper mapper,
                                                                                    byte[] bytes) {
        ChecksumHelper.checkContentMD5(request, bytes);
        final UpdateBucketOptionsRequestJson updateBucketOptionsRequestJson;
        try {
            updateBucketOptionsRequestJson = mapper.readValue(bytes, UpdateBucketOptionsRequestJson.class);
        } catch (Exception e) {
            throw new HttpException(V2ErrorCode.INVALID_BUCKET_OPTIONS_JSON,
                    "Could not parse body as a valid update bucket options request.",
                    request.path(), e);
        }
        if (!updateBucketOptionsRequestJson.getOptions().isPresent()) {
            throw new HttpException(V2ErrorCode.INVALID_BUCKET_OPTIONS_JSON,
                    "Required fields are missing from the update bucket options request.",
                    request.path());
        }
        return updateBucketOptionsRequestJson;
    }

    public static Map<String, String> checkAndRemoveMetadataPrefix(HttpServerRequest request,
                                                                   Map<String, String> metadataWithPrefix) {
        if (metadataWithPrefix == null) {
            return null;
        } else {
            final Map metadataWithoutPrefix = new HashMap();
            final String prefix = HttpHeaderHelpers.UserMetadataPrefix.V2.getPrefix();
            final int length = HttpHeaderHelpers.UserMetadataPrefix.V2.getLength();
            for (Map.Entry<String, String> entry : metadataWithPrefix.entrySet()) {
                if (!entry.getKey().startsWith(prefix)) {
                    throw new HttpException(V2ErrorCode.INVALID_METADATA,
                        "The metadata key \"" + entry.getKey() + "\" must be prefixed with " + prefix,
                        request.path());
                } else {
                    metadataWithoutPrefix.put(entry.getKey().substring(length), entry.getValue());
                }
            }
            return metadataWithoutPrefix;
        }
    }

    public static Pair<CreatePreAuthenticatedRequestRequest, String> readCreateParDetails(
            RoutingContext context, AuthenticationInfo authInfo, String namespace, String bucketName, byte[] bytes,
            ObjectMapper mapper) {

        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(bucketName);
        Preconditions.checkNotNull(bytes);
        Preconditions.checkNotNull(mapper);

        HttpServerRequest request = context.request();
        String name = null;
        PreAuthenticatedRequestMetadata.AccessType accessType = null;
        Instant timeExpires = null;
        String objectName = null;

        final JsonNode root;
        try {
            root = mapper.readTree(bytes);
        } catch (IOException ioe) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Could not parse body as a JSON object.", request.path(),
                ioe);
        }

        if (!root.isObject()) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Request body must be a JSON object.", request.path());
        }

        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = root.fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldsIterator.next();
            String key = field.getKey();
            JsonNode value = field.getValue();

            if (!key.equals("objectName")) {
                if (value.getNodeType() == JsonNodeType.NULL) {
                    throw new HttpException(V2ErrorCode.MISSING_PAR_CREATE_DETAILS,
                        "PAR name, operation, and expiration are required in create PAR request", request.path());
                }
                if (value.getNodeType() != JsonNodeType.STRING) {
                    throw new HttpException(V2ErrorCode.INVALID_JSON,
                        "PAR name, operation, and expiration in request body JSON must be of type string, but is " +
                            value.getNodeType(), request.path());
                }
            }

            switch (key) {
                case "name":
                    name = value.asText();
                    break;
                case "accessType":
                    String accessTypeText = value.asText();
                    try {
                        accessType = PreAuthenticatedRequestMetadata.AccessType.valueOf(accessTypeText);
                    } catch (IllegalArgumentException iae) {
                        throw new HttpException(V2ErrorCode.INVALID_JSON,
                            "Invalid or unsupported PAR operation: " + accessTypeText, request.path(), iae);
                    }
                    break;
                case "timeExpires":
                    String timeExpiresText = value.asText();
                    // expect string conforming to RFC 3339
                    try {
                        timeExpires = DateUtil.rfc3339StringToInstant(timeExpiresText);
                    } catch (IllegalArgumentException iae) {
                        throw new HttpException(V2ErrorCode.INVALID_JSON,
                            "PAR expiration must conform to RFC 3339: " + timeExpiresText, request.path(), iae);
                    }
                    break;
                case "objectName":
                    if (value.getNodeType() != JsonNodeType.NULL && value.getNodeType() != JsonNodeType.STRING) {
                        throw new HttpException(V2ErrorCode.INVALID_JSON,
                            "PAR objectName in request body JSON, if present, must be of type string, but is " +
                                value.getNodeType(), request.path());
                    }
                    objectName = value.getNodeType() != JsonNodeType.NULL ? value.asText() : null;
                    break;
                default:
                    throw new HttpException(V2ErrorCode.INVALID_JSON, "Invalid Json field: " + key, request.path());
            }
        }

        if (name == null || accessType == null || timeExpires == null) {
            throw new HttpException(V2ErrorCode.MISSING_PAR_CREATE_DETAILS,
                    "PAR name, operation, and expiration are required in create PAR request", request.path());
        }

        final Map<String, String> stringToSign = ParSigningHelper.getSTS(namespace, bucketName, objectName);
        final String nonce = ParSigningHelper.getNonce();
        final String verifierId = ParSigningHelper.getVerifierId(stringToSign, nonce);

        String accessUri = "/p/" + nonce + "/n/" + namespace + "/b/" + bucketName + "/o/";
        if (objectName != null) {
            // To enable PAR lookup, where accessUri will be a query param that the SDK client does not automatically
            // URI-encode, we need to encode the object name which may contain illegal chars ourselves, so that we can
            // safely parse the accessUri query param as a URI. Also see HttpPathQueryHelpers#getParIdFromParUrl
            accessUri += URIUtil.encodePath(objectName);
        }
        LOG.info("Creating PAR with STS {}, nonce [SANITIZED], verifierId {}", stringToSign, verifierId);

        try {
            CreatePreAuthenticatedRequestRequest createReq = CreatePreAuthenticatedRequestRequest.builder()
                    .withContext(context)
                    .withAuthInfo(authInfo)
                    .withVerifierId(verifierId)
                    .withName(name)
                    .withBucket(bucketName)
                    .withObject(objectName)
                    .withAccessType(accessType)
                    .withTimeExpires(timeExpires)
                    .build();
            return Pair.pair(createReq, accessUri);
        } catch (IllegalArgumentException iae) {
            LOG.error("Found invalid create PAR request", iae);
            throw new HttpException(V2ErrorCode.INVALID_PAR_CREATE_REQUEST, request.path(), iae);
        }

    }

    public static ObjectMetadata readObjectMetadata(
            HttpServerRequest request,
            String namespace,
            String bucketName,
            String objectName,
            Map<String, String> metadata) {
        final long contentLen = Long.parseLong(request.getHeader(HttpHeaders.CONTENT_LENGTH));
        return new ObjectMetadata(
                namespace,
                bucketName,
                objectName,
                contentLen,
                null,
                metadata,
                new Date(),
                new Date(),
                null,
                null,
                null,
                ArchivalState.Available);
    }

    public static AbortableBlobReadStream readStream(WSRequestContext wsRequestContext,
                                                     HttpServerRequest request,
                                                     WebServerMetricsBundle webServerMetricsBundle,
                                                     Optional<String> contentMD5,
                                                     long contentLength) {
        final DigestAlgorithm algorithm = DigestAlgorithm.MD5;
        final Optional<Digest> digest = contentMD5.map(c -> Digest.fromBase64Encoded(algorithm, c));
        final String rid = wsRequestContext.getCommonRequestContext().getOpcRequestId();

        MeasuringReadStream<Buffer> measuringReadStream = new MeasuringReadStream<>(
                webServerMetricsBundle.getFirstByteLatency(),
                wsRequestContext.getStartTime(),
                wsRequestContext.getCommonRequestContext().getMetricScope(),
                new TrafficRecorderReadStream(request, rid));
        return new HttpServerRequestReadStream(algorithm, digest, contentLength, request, measuringReadStream);

    }

    public static void writeContentRangeHeaders(RoutingContext context, ByteRange byteRange, long entireContentLength) {
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final MetricScope metricScope = wsRequestContext.getCommonRequestContext().getMetricScope();
        final HttpServerResponse response = context.response();

        metricScope.annotate("range", true);
        metricScope.annotate("rangeFrom", byteRange.getStart());
        metricScope.annotate("rangeTo", byteRange.getEnd());

        response.setStatusCode(HttpResponseStatus.PARTIAL_CONTENT);
        response.putHeader(HttpHeaders.CONTENT_LENGTH, Long.toString(byteRange.getLength()));
        response.putHeader(HttpHeaders.CONTENT_RANGE,
            String.format("bytes %d-%d/%d", byteRange.getStart(), byteRange.getEnd(), entireContentLength)
        );
    }

    /**
     * Write the status code and headers for HEAD and GET requests that are common to most API.
     */
    public static void writeReadObjectResponseHeadersAndStatus(
        RoutingContext context,
        ObjectMetadata objectMetadata,
        @Nullable ByteRange byteRange) {

        final HttpServerResponse response = context.response();

        long entireContentLength = objectMetadata.getSizeInBytes();

        ArchivalState archivalState = objectMetadata.getArchivalState();

        if (archivalState != ArchivalState.Available) {
            response.putHeader(CommonHeaders.ARCHIVAL_STATE, archivalState.toString());
            if (archivalState == ArchivalState.Restored) {
                DateFormat format = new SimpleDateFormat(DateUtil.SWAGGER_DATE_TIME_PATTERN);
                Date archivedTime = objectMetadata.getArchivedTime().orElse(null);
                String timeOfArchival = format.format(archivedTime);
                response.putHeader(CommonHeaders.ARCHIVAL_TIME, timeOfArchival);
            } else {
                response.headers().remove(CommonHeaders.ARCHIVAL_TIME);
            }
        } else {
            response.headers().remove(CommonHeaders.ARCHIVAL_STATE);
        }

        //All of our read object APIs support range so always include the header
        response.putHeader(HttpHeaders.ACCEPT_RANGES, "bytes");

        if (byteRange == null) {
            response.setStatusCode(HttpResponseStatus.OK);
            response.putHeader(HttpHeaders.CONTENT_LENGTH, Long.toString(entireContentLength));
            objectMetadata.getChecksum().addHeaderToResponseV2Get(response);
        } else {
            writeContentRangeHeaders(context, byteRange, entireContentLength);
        }
    }

    /**
     * Pump a stream of data from storage servers back to the client on GET object requests.
     *
     * This method is guaranteed to not throw an exception, but the returned CompletableFuture may complete with an
     * exception.
     *
     * @param context  the routing context
     * @param response the HTTP response
     * @param stream   the stream of blob data to return
     * @return a CompletableFuture that is completed when the request has failed or all the bytes have been pumped. For
     *         historical reasons, the CF is never completed exceptionally, even if the request fails with an
     *         exception. Instead, the RoutingContext::fail method is called if there is an exception and the stream
     *         is aborted (with abort()) if the HttpServerResponse is closed (before all the bytes have been pumped).
     *         The returned CF must only be used for sequencing actions after the pump completes or fails. The returned
     *         CF is guaranteed to complete on the Vert.x event loop thread.
     */
    public static CompletableFuture<Void> pumpResponseContent(RoutingContext context,
                                                              HttpServerResponse response,
                                                              AbortableReadStream<Buffer> stream) {
        final CompletableFuture<Void> cf = new CompletableFuture<>();
        final Pump pump = Pump.pump(stream, response);
        stream.endHandler(x -> {
            response.end();
            cf.complete(null);
        });

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final AtomicBoolean recursion = new AtomicBoolean(false);

        Handler<Throwable> exceptionHandler = ex -> {

            /**
             * We found cases (mostly in testing) where we infinitely recursed to here.
             * For instance,
             * 1. ByteBackedReadStream (wrapped by DefaultBlobReadStream) tries to call endHandler
             * 2. DefaultBlobReadStream performs content-length validation and fails in the endHandler
             *    and triggers an exception to be handled
             * 3. exception is handled by this block and the stream is tried to be resumed (1) which causes recursion.
             */
            if (recursion.get()) {
                LOG.warn("We have re-entered the exception handler. That is an indication that resuming the stream" +
                        "is triggering an exception handler. You should look into that.");
                return;
            }

            try {
                pump.stop();
            } catch (IllegalStateException ise) {
                LOG.trace("Failed to stop the pump", ise);
            }
            // make sure that we continue to consume the data
            recursion.set(true);
            stream.handler(null); //de-register the handler because we want to drop the data on the floor anyways
            stream.resume();
            context.fail(ex);
            cf.complete(null);
        };
        stream.exceptionHandler(exceptionHandler);
        response.exceptionHandler(exceptionHandler);
        wsRequestContext.pushEndHandler(c -> {
            try {
                pump.stop();
            } catch (IllegalStateException ise) {
                LOG.trace("Failed to stop the pump", ise);
            }
            // stop the further request to occur
            stream.abort();
            cf.complete(null);
        });
        pump.start();
        return cf;
    }

    /**
     * Buffer a stream of data from storage servers back to the client on GET object requests.
     *
     * @param stream  the read stream of data.
     *
     * @return a {@link CompletableFuture} containing a {@link Buffer}.
     */
    public static CompletableFuture<Buffer> bufferStreamData(AbortableReadStream<Buffer> stream) {
        final Buffer buffer = Buffer.buffer();
        final CompletableFuture<Buffer> bufferFuture = new CompletableFuture<>();
        try {
            stream.exceptionHandler(x -> {
                try {
                    stream.handler(null);
                } catch (IllegalStateException ex) {
                    LOG.trace("Failed to reset the data handler.");
                }
            });
            stream.endHandler(v -> bufferFuture.complete(buffer));
            stream.handler(buffer::appendBuffer);
            stream.resume();
        } catch (Throwable e) {
            bufferFuture.completeExceptionally(e);
        }
        return bufferFuture;
    }

    /**
     * Returns true if the given MediaType is 'application' or 'text'. For example
     * 'application/json' or 'text/xml'.
     */
    private static boolean isApplicationOrText(MediaType mediaType) {
        return "application".equalsIgnoreCase(mediaType.type()) || "text".equalsIgnoreCase(mediaType.type());
    }

    public static NamespaceMetadata getNamespaceMetadata(HttpServerRequest request,
                                                         byte[] bytes,
                                                         ObjectMapper mapper) throws Exception {
        ChecksumHelper.checkContentMD5(request, bytes);
        try {
            NamespaceMetadata namespaceMetadata = mapper.readValue(bytes, NamespaceMetadata.class);
            return namespaceMetadata;
        } catch (JsonMappingException e) {
            throw new InvalidNamespaceMetadataException("Invalid data for updating namespace");
        }
    }

    /**
     * Read the body of an HTTP request, parse it as a UTF-8 encoded JSON string as a
     * {@link PutObjectLifecyclePolicyDetails} with the given namespace object names and bucket name.
     *
     * @param request    the HTTP request from which the body was read, used to get headers.
     * @param bytes      the raw bytes of the HTTP request body which must be a JSON object encoded in UTF-8.
     * @param mapper     the Jackson object mapper to convert the JSON to ObjectLifecycleDetails.
     * @return a well-formed ObjectLifecycleDetails.
     */
    public static PutObjectLifecyclePolicyDetails getObjectLifecycleDetails(HttpServerRequest request,
                                                                            byte[] bytes,
                                                                            ObjectMapper mapper) {
        ChecksumHelper.checkContentMD5(request, bytes);
        try {
            final PutObjectLifecyclePolicyDetails putObjectLifecyclePolicyDetails =
                    mapper.readValue(bytes, PutObjectLifecyclePolicyDetails.class);
            Validator.validateObjectLifecycleDetails(putObjectLifecyclePolicyDetails);

            return putObjectLifecyclePolicyDetails;
        } catch (IOException e) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Could not parse body as valid ObjectLifecycleDetails.",
                request.path(), e);
        }
    }

    /**
     * Serialize {@link PutObjectLifecyclePolicyDetails} back to byte array.
     *
     * @param request    the HTTP request from which the body was read, used to get headers.
     * @param details    the {@link PutObjectLifecyclePolicyDetails}.
     * @param mapper     the Jackson object mapper to convert the JSON to ObjectLifecycleDetails.
     * @return byte array.
     */
    public static byte[] serializeObjectLifecycleDetails(HttpServerRequest request,
                                                         PutObjectLifecyclePolicyDetails details,
                                                         ObjectMapper mapper) {
        try {
            return mapper.writeValueAsBytes(details);
        } catch (IOException e) {
            throw new HttpException(V2ErrorCode.INVALID_JSON,
                "Could not serialize ObjectLifecycleDetails back to byte array.", request.path(), e);
        }
    }

    /**
     * Read the body of an HTTP request, parse it as a UTF-8 encoded JSON string into a {@link RestoreObjectsDetails} object.
     *
     * @param path the request path to use in exception messages.
     * @param mapper  the Jackson object mapper to convert the JSON data to a RestoreObjectsDetails object.
     * @param bytes   the raw bytes of the HTTP request.
     * @return a well-formed RestoreObjectsDetails.
     */
    public static RestoreObjectsDetails readRestoreObjectDetails(
            @Nullable  String path, ObjectMapper mapper, byte[] bytes) {
        final RestoreObjectsDetails restoreObjectsDetails;
        try {
            restoreObjectsDetails = mapper.readValue(bytes, RestoreObjectsDetails.class);
        } catch (IOException e) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, path, e);
        }

        if (StringUtils.isBlank(restoreObjectsDetails.getObjectName())) {
            throw new HttpException(V2ErrorCode.INVALID_OBJECT_NAME, path, "Object name cannot be empty");
        }

        final Duration timeToArchive = restoreObjectsDetails.getTimeToArchive();
        final Duration timeToArchiveMin = RestoreObjectsDetails.getTimeToArchiveMin();
        final Duration timeToArchiveMax = RestoreObjectsDetails.getTimeToArchiveMax();
        if ((timeToArchive != null) && ((timeToArchive.compareTo(timeToArchiveMin) < 0) ||
                (timeToArchive.compareTo(timeToArchiveMax) > 0))) {
            throw new HttpException(V2ErrorCode.INVALID_RESTORE_HOURS,
                    "Hours value should range between " + timeToArchiveMin.toHours() + " and " +
                            timeToArchiveMax.toHours() + " hours", path);
        }
        return restoreObjectsDetails;
    }

    /**
     * Read the body of an HTTP request, parse it as a UTF-8 encoded JSON string into a {@link CopyPartDetails} object.
     *
     * @param path   the request path to use in exception messages.
     * @param mapper the Jackson object mapper to convert the JSON data to a RestoreObjectsDetails object.
     * @param bytes  the raw bytes of the HTTP request.
     * @return a well-formed CopyPartDetails.
     */
    public static CopyPartDetails readCopyPartDetails(
            @Nullable String path, ObjectMapper mapper, byte[] bytes) {
        final CopyPartDetails copyPartDetails;
        try {
            copyPartDetails = mapper.readValue(bytes, CopyPartDetails.class);
        } catch (IOException e) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, path, e);
        }

        if (StringUtils.isBlank(copyPartDetails.getSourceNamespace())) {
            throw new HttpException(V2ErrorCode.MISSING_NAMESPACE, path, "Source namespace cannot be empty");
        }
        if (StringUtils.isBlank(copyPartDetails.getSourceBucket())) {
            throw new HttpException(V2ErrorCode.MISSING_BUCKET_NAME, path, "Source bucket cannot be empty");
        }
        if (StringUtils.isBlank(copyPartDetails.getSourceObject())) {
            throw new HttpException(V2ErrorCode.MISSING_OBJECT_NAME, path, "Source object name cannot be empty");
        }

        if (!StringUtils.isBlank(copyPartDetails.getRange())) {
            boolean validate = ByteRange.isValidHeader(copyPartDetails.getRange());
            if (!validate) {
                throw new RangeNotSatisfiableException("Invalid byte range passed");
            }
        }

        return CopyPartDetails.builder().copy(copyPartDetails)
                .sourceNamespace(NamespaceCaseWhiteList.lowercaseNamespace(Api.V2,
                        copyPartDetails.getSourceNamespace())).build();
    }

    public static CreateReplicationPolicyDetails readReplicationPolicyDetails(
            HttpServerRequest request, String srcBucketName, byte[] bytes, ObjectMapper mapper) {
        ChecksumHelper.checkContentMD5(request, bytes);
        final CreateReplicationPolicyDetails details;
        try {
            details = mapper.readValue(bytes, CreateReplicationPolicyDetails.class);
        } catch (IOException ioe) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Could not parse body as a JSON object", request.path(),
                    ioe);
        }
        final String name = details.getName();
        if (StringUtils.isBlank(name)) {
            throw new HttpException(V2ErrorCode.INVALID_REPLICATION_POLICY_NAME, "Policy name is blank",
                    request.path());
        }
        // Validation
        final String destRegion = details.getDestinationRegionName();
        if (StringUtils.isBlank(destRegion)) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Destination region is missing", request.path());
        }

        ConfigRegion destConfigRegion = CrossRegionUtils.getConfigRegion(destRegion);
        if (destConfigRegion == null) {
            throw new HttpException(V2ErrorCode.INVALID_REGION, "Unknown region " + destRegion, request.path());
        }
        final Region srcRegion = ConfigRegion.fromSystemProperty().toRegion();
        if (srcRegion != Region.DEV && destConfigRegion.toRegion().getRealm() != srcRegion.getRealm()) {
            throw new HttpException(V2ErrorCode.INVALID_REGION,
                    "Invalid region " + destRegion + ", replication between realms is not allowed", request.path());
        }

        final String destinationBucket = details.getDestinationBucketName();
        if (StringUtils.isBlank(destinationBucket)) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Destination bucket is missing", request.path());
        }

        if (destinationBucket.equals(srcBucketName) && destConfigRegion.toRegion() == srcRegion) {
            throw new HttpException(V2ErrorCode.INVALID_BUCKET_NAME,
                    "Replication to the same bucket in the same region is not allowed", request.path());
        }
        return details;
    }

    public static ByteRange tryParseByteRange(String range, long totalSizeInBytes) {
        try {
            //Special case when a byte range was passed but the content length is zero.
            if (StringUtils.isNotBlank(range) && totalSizeInBytes == 0) {
                throw new RangeNotSatisfiableException("Invalid byte range passed");
            }
            return ByteRange.from(range, totalSizeInBytes);
        } catch (IllegalStateException | IllegalArgumentException e) {
            throw new RangeNotSatisfiableException("Invalid byte range passed");
        }
    }

    public static void validateByteRange(ByteRange byteRange, WebServerConfiguration webServerConfiguration,
                                         ObjectMetadata objMeta) {
        if (byteRange != null) {
            if (byteRange.getLength() > webServerConfiguration.getMaxUploadPartSize().longValue(BYTE)
                    || byteRange.getLength() < 1) {
                throw new IllegalArgumentException(String.format("The size of the range should be within" +
                                " 1 to %d (inclusive)",
                        webServerConfiguration.getMaxUploadPartSize().longValue(BYTE)));
            }
        } else if (objMeta.getSizeInBytes()
                > webServerConfiguration.getMaxUploadPartSize().longValue(BYTE)) {
            throw new ObjectTooLargeException(String.format("The size of the source object exceeds the " +
                            "maximum part size (%d bytes).",
                    webServerConfiguration.getMaxUploadPartSize().longValue(BYTE)));
        }

    }

    public static ReencryptObjectDetails getReencryptObjectDetails(HttpServerRequest request,
                                                                   byte[] bytes,
                                                                   ObjectMapper mapper) {
        try {
            return mapper.readValue(bytes, ReencryptObjectDetails.class);
        } catch (IOException e) {
            throw new HttpException(V2ErrorCode.INVALID_JSON, "Could not parse body as valid ReencryptObjectDetails.",
                    request.path(), e);
        }
    }
}
