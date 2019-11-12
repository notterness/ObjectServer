package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.oracle.pic.casper.common.config.MultipartLimit;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.NotModifiedException;
import com.oracle.pic.casper.common.exceptions.PreconditionFailedException;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.common.Checksum;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.NamespaceCaseWhiteList;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.s3.CompleteMultipartUpload;
import com.oracle.pic.casper.webserver.api.s3.model.CopyObjectDetails;
import com.oracle.pic.casper.webserver.api.s3.model.Delete;
import com.oracle.pic.casper.webserver.api.s3.model.RestoreRequest;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.time.DateUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;

import static com.oracle.pic.casper.common.rest.ContentType.APPLICATION_XML_UTF8;

public final class S3HttpHelpers {

    /**
     * This is the smallest part number that can be created.
     */
    static final int MIN_LIMIT = 1;

    /**
     * This is the largest part number that can be created.
     */
    static final int MAX_LIMIT = 1000;

    /**
     * This is the smallest part number that can be used as a part number marker
     * when listing parts. This is different from {@link #MIN_LIMIT} -- you can
     * not create part number 0, but you can list parts using 0 as the part
     * number marker.
     *
     * (See CASPER-1348)
     */
    static final int LIST_PART_MIN = 0;

    private S3HttpHelpers() { }

    public static XmlMapper getXmlMapper() {
        DateFormat dateFormat = new SimpleDateFormat(DateUtil.S3_XML_FORMAT_PATTERN);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return (XmlMapper) new XmlMapper()
            .configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, true)
            .setDateFormat(dateFormat)
            .setPropertyNamingStrategy(PropertyNamingStrategy.UPPER_CAMEL_CASE)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);
    }

    public static XmlMapper getXmlMapperWithoutXmlDeclaration() {
        return (XmlMapper) getXmlMapper()
                .configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, false);
    }

    public static String getHost(HttpServerRequest request) {
        String host = request.getHeader(HttpHeaders.HOST);
        if (host == null) {
            throw new S3HttpException(S3ErrorCode.INVALID_REQUEST, "Missing required header for this request: host",
                request.path());
        }
        return host;
    }

    /**
     * Get the namespace from the host header because S3 does not have the notion of namespace
     * e.g. with host header faketenant.objectstorage.us-phoenix-1.oraclecloud.com, we extract faketenant as the NS
     * Lower cases the namespace to support case insensitive lookups.
     */
    public static String getNamespace(HttpServerRequest request, WSRequestContext context) {
        String namespace = NamespaceCaseWhiteList.lowercaseNamespace(Api.V2,
                getHost(request).split("\\.")[0]);
        // IOS-12460: Underscores are not allowed in host names, so we provide
        // a special workaround for the cisco_tetration namespace.
        if (namespace.equals("ciscotetrations3compat")) {
            namespace = "cisco_tetration";
        }
        context.setNamespaceName(namespace);
        return namespace;
    }

    public static String getBucketName(HttpServerRequest request, WSRequestContext context) {
        final String name = Preconditions.checkNotNull(request.getParam(S3Api.BUCKET_PARAM));
        context.setBucketName(name);
        return name;
    }

    public static String getObjectName(HttpServerRequest request, WSRequestContext context) {
        final String name = Preconditions.checkNotNull(request.getParam(S3Api.OBJECT_PARAM));
        context.setObjectName(name);
        return name;
    }

    //example: x-amz-copy-source: /source-bucket/sourceobject (object name may contain "/")
    public static String getSourceBucketName(HttpServerRequest request) {
        String sourceBucketName = extractSourceBucketAndObjectNamesFromHeaders(request)[0];
        try {
            return Validator.validateBucket(sourceBucketName);
        } catch (Exception e) {
            throw new S3HttpException(S3ErrorCode.NO_SUCH_BUCKET,
                    "x-amz-copy-source is not specified correctly. it should be source_bucket/object",
                    // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                    request.path() == null ? request.path() : request.path().replace("/%2F", "//"));
        }
    }

    //example: x-amz-copy-source: /source-bucket/sourceobject (object name may contain "/")
    public static String getSourceObjectName(HttpServerRequest request) {
        String sourceObjectName = extractSourceBucketAndObjectNamesFromHeaders(request)[1];
        try {
            Validator.validateObjectName(sourceObjectName);
            return sourceObjectName;
        } catch (Exception e) {
            throw new S3HttpException(S3ErrorCode.NO_SUCH_KEY,
                    "x-amz-copy-source is not specified correctly. it should be source_bucket/object",
                    // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                    request.path() == null ? request.path() : request.path().replace("/%2F", "//"));
        }
    }

    private static String[] extractSourceBucketAndObjectNamesFromHeaders(HttpServerRequest request) {
        try {
            final String copySource = Preconditions.checkNotNull(request.getHeader(S3Api.COPY_SOURCE_HEADER));
            final String splitCopySource = copySource.startsWith("/") ? copySource.substring(1) : copySource;
            final String decodeSource = URLDecoder.decode(splitCopySource, StandardCharsets.UTF_8.name());
            String[] source = decodeSource.split("/", 2);
            if (source.length != 2) {
                throw new Exception();
            }
            return source;
        } catch (Exception e) {
            throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT,
                    "x-amz-copy-source is not specified correctly. it should be source_bucket/object",
                    // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                    request.path() == null ? request.path() : request.path().replace("/%2F", "//"));
        }
    }

    public static String getUploadID(HttpServerRequest request) {
        return Preconditions.checkNotNull(request.getParam(S3Api.UPLOADID_PARAM));
    }

    public static String unquoteIfNeeded(String string) {
        if (string == null) {
            return null;
        } else if (string.length() >= 2 && string.startsWith("\"") && string.endsWith("\"")) {
            return string.substring(1, string.length() - 1);
        }
        return string;
    }

    //See https://tools.ietf.org/html/rfc7232 for precedence reference
    public static void checkConditionalHeaders(HttpServerRequest request, @Nonnull ObjectMetadata metadata) {
        String ifMatch = unquoteIfNeeded(request.getHeader(HttpHeaders.IF_MATCH));
        String ifNoneMatch = unquoteIfNeeded(request.getHeader(HttpHeaders.IF_NONE_MATCH));
        Date ifModifiedSince = DateUtil.s3HttpHeaderStringToDate(request.getHeader(HttpHeaders.IF_MODIFIED_SINCE));
        Date ifUnmodifiedSince = DateUtil.s3HttpHeaderStringToDate(request.getHeader("If-Unmodified-Since"));
        Date lastModified = DateUtils.truncate(metadata.getModificationTime(), Calendar.SECOND);
        Date now = new Date();

        String md5 = Optional.ofNullable(metadata.getChecksum()).map(Checksum::getHex).orElse(null);

        if (ifMatch != null && !ifMatch.equals(md5)) {
            throw new PreconditionFailedException();
        }
        if (ifMatch == null && ifUnmodifiedSince != null && ifUnmodifiedSince.before(lastModified)) {
            throw new PreconditionFailedException();
        }
        if (ifNoneMatch != null && ifNoneMatch.equals(md5)) {
            throw new NotModifiedException();
        }
        if (ifNoneMatch == null && ifModifiedSince != null && !ifModifiedSince.after(now)
                && !lastModified.after(ifModifiedSince)) {
            throw new NotModifiedException();
        }
    }

    public static void checkConditionalHeadersForCopyPart(HttpServerRequest request, @Nonnull ObjectMetadata metadata) {
        String ifMatch = unquoteIfNeeded(request.getHeader(S3Api.COPY_SOURCE_IF_MATCH));
        String ifNoneMatch = unquoteIfNeeded(request.getHeader(S3Api.COPY_SOURCE_IF_NONE_MATCH));
        Date ifModifiedSince = DateUtil.
                s3HttpHeaderStringToDate(request.getHeader(S3Api.COPY_SOURCE_IF_MODIFIED_SINCE));
        Date ifUnmodifiedSince = DateUtil.
                s3HttpHeaderStringToDate(request.getHeader(S3Api.COPY_SOURCE_IF_UNMODIFIED_SINCE));
        Date lastModified = DateUtils.truncate(metadata.getModificationTime(), Calendar.SECOND);

        String md5 = Optional.ofNullable(metadata.getChecksum()).map(Checksum::getHex).orElse(null);

        if (ifMatch != null && !ifMatch.equals(md5)) {
            throw new PreconditionFailedException("Failed on if-match evaluation.");
        }
        if (ifUnmodifiedSince != null && ifUnmodifiedSince.before(lastModified)) {
            if  (ifMatch != null && ifMatch.equals(md5)) {
                return;
            } else {
                throw new PreconditionFailedException("Failed on if-unmodified-since evaluation.");
            }
        }
        if (ifNoneMatch != null && ifNoneMatch.equals(md5)) {
            if (ifModifiedSince != null && lastModified.after(ifModifiedSince)) {
                return;
            } else {
                throw new PreconditionFailedException("Failed on if-none-match evaluation.");
            }
        }
        if (ifModifiedSince != null && !lastModified.after(ifModifiedSince)) {
            throw new PreconditionFailedException("Failed on if-modified-since evaluation.");
        }
    }

    public static void checkConditionalHeadersForCopy(HttpServerRequest request,
                                                      CopyObjectDetails details,
                                                      @Nonnull ObjectMetadata metadata) {
        final String ifMatch = details.getIfMatchEtag();
        final String ifNoneMatch = details.getIfNoneMatchEtag();
        final Instant ifModifiedSince = details.getModifiedSinceConstraint();
        final Instant ifUnmodifiedSince = details.getUnmodifiedSinceConstraint();
        final Instant lastModified = metadata.getModificationTime().toInstant().truncatedTo(ChronoUnit.SECONDS);

        final String md5 = Optional.ofNullable(metadata.getChecksum()).map(Checksum::getHex).orElse(null);

        if (ifMatch != null && ((ifNoneMatch != null) || (ifModifiedSince != null))) {
            throw new S3HttpException(S3ErrorCode.INVALID_REQUEST, "x-amz-copy-source-if-match header can be be used" +
                    "only with  x-amz-copy-source-if-unmodified-since", request.path());

        }
        if (ifNoneMatch != null && ((ifMatch != null) || (ifUnmodifiedSince != null))) {
            throw new S3HttpException(S3ErrorCode.INVALID_REQUEST, "x-amz-copy-source-if-none-match header can be be " +
                    "used only with  x-amz-copy-source-if-modified-since", request.path());

        }
        if (ifMatch != null && !ifMatch.equals("*") && !ifMatch.equals(md5)) {
            throw new PreconditionFailedException();
        }
        if (ifMatch == null && ifUnmodifiedSince != null && ifUnmodifiedSince.isBefore(lastModified)) {
            throw new PreconditionFailedException();
        }
        if (ifNoneMatch != null && (ifNoneMatch.equals("*") || ifNoneMatch.equals(md5))) {
            throw new PreconditionFailedException();
        }
        if (ifNoneMatch == null && ifModifiedSince != null &&
                (ifModifiedSince.equals(lastModified) || ifModifiedSince.isAfter(lastModified))) {
            throw new PreconditionFailedException();
        }
    }

    public static void validateContentMD5ContentLengthHeaders(HttpServerRequest request) {
        Optional<String> md5 = ChecksumHelper.getContentMD5Header(request);
        if (!md5.isPresent()) {
            throw new S3HttpException(S3ErrorCode.INVALID_REQUEST, "Missing required header for this request:" +
                    " Content-MD5", request.path());
        }
        HttpContentHelpers.validateContentLengthHeader(request);
    }

    public static Optional<Integer> getIntegerParam(HttpServerRequest request, String param) {
        return getIntegerParam(request, param, MIN_LIMIT, MAX_LIMIT, false);
    }

    public static Optional<Integer> getIntegerParam(HttpServerRequest request,
                                                    String param,
                                                    int minLimit,
                                                    int maxLimit,
                                                    boolean adjustMax) {
        final String string = request.getParam(param);
        if (string == null) {
            return Optional.empty();
        }
        int value;
        try {
            value = Integer.parseInt(string);
        } catch (NumberFormatException nfex) {
            throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT, "The '" + param +
                    "' parameter must be a valid integer (it was '" + string + "')", request.path());
        }
        if (value < minLimit || value > maxLimit) {
            if (adjustMax && value > maxLimit) {
                value = maxLimit;
            } else {
                throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT, "The '" + param +
                        "' parameter must be between " + minLimit + " and " + (adjustMax ? Integer.MAX_VALUE : maxLimit)
                        + " (it was " + value + ")",
                        request.path());
            }
        }
        return Optional.of(value);
    }

    public static Optional<Integer> getMaxKeys(HttpServerRequest request) {
        return getIntegerParam(request, S3Api.MAX_KEY_PARAM, 0, MAX_LIMIT, true);
    }

    public static Optional<Integer> getMaxParts(HttpServerRequest request) {
        return getIntegerParam(request, S3Api.MAX_PARTS_PARAM);
    }

    public static Optional<Integer> getMaxUploads(HttpServerRequest request) {
        return getIntegerParam(request, S3Api.MAX_UPLOADS_PARAM);
    }

    public static Optional<Integer> getPartMarker(HttpServerRequest request) {
        return getIntegerParam(request, S3Api.PART_MARKER_PARAM, LIST_PART_MIN, MultipartLimit.MAX_UPLOAD_PART_NUM,
            false);
    }

    public static boolean isListV2(HttpServerRequest request) {
        Optional<Integer> type = getIntegerParam(request, S3Api.LIST_TYPE_PARAM);
        if (type.isPresent()) {
            if (type.get() != 2) {
                throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT, "The '" + S3Api.LIST_TYPE_PARAM +
                    "' parameter must be '2'; it was '" + type.get() + "'", request.path());
            }
            return true;
        }
        return false;
    }

    public static Optional<String> getEncoding(HttpServerRequest request) {
        String encoding = request.getParam(S3Api.ENCODING_PARAM);
        if (encoding == null) {
            return Optional.empty();
        }
        if (!encoding.equalsIgnoreCase("url")) {
            throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT, "The '" + S3Api.ENCODING_PARAM +
                "' parameter must be 'url'", request.path());
        }
        return Optional.of(encoding);
    }

    public static int getPartNumberForPutPart(HttpServerRequest request) {
        String partStr = request.getParam(S3Api.PARTNUM_PARAM);
        if (partStr == null) {
            throw new S3HttpException(S3ErrorCode.INVALID_PART, "The '" + S3Api.PARTNUM_PARAM +
                "' parameter is missing", request.path());
        }
        final int value;
        try {
            value = Integer.parseInt(partStr);
        } catch (NumberFormatException nfex) {
            throw new S3HttpException(S3ErrorCode.INVALID_PART, "The '" + S3Api.PARTNUM_PARAM +
                "' parameter must be a valid integer (it was '" + partStr + "')", request.path());
        }
        if (!MultipartLimit.isWithinRange(value)) {
            throw new S3HttpException(S3ErrorCode.INVALID_PART, MultipartLimit.PART_NUM_ERROR_MESSAGE,
                    request.path());
        }
        return value;
    }

    public static String getKeyMarker(HttpServerRequest request) {
        return request.getParam(S3Api.KEY_MARKER_PARAM);
    }

    public static String getUploadIdMarker(HttpServerRequest request) {
        return request.getParam(S3Api.UPLOADID_MARKER_PARAM);
    }

    public static String getPrefix(HttpServerRequest request) {
        return Strings.emptyToNull(request.getParam(S3Api.PREFIX_PARAM));
    }

    public static String getDelimiter(HttpServerRequest request) {
        return Strings.emptyToNull(request.getParam(S3Api.DELIMITER_PARAM));
    }

    public static String getStartAfter(HttpServerRequest request) {
        return request.getParam(S3Api.START_AFTER_PARAM);
    }

    public static String getVersionIdMarker(HttpServerRequest request) {
        return Strings.emptyToNull(request.getParam(S3Api.VERSION_ID_MARKER_PARAM));
    }

    public static String getContinuationToken(HttpServerRequest request) {
        return request.getParam(S3Api.CONTINUATION_TOKEN_PARAM);
    }

    public static String getMarker(HttpServerRequest request) {
        return Strings.emptyToNull(request.getParam(S3Api.MARKER_PARAM));
    }

    public static boolean getFetchOwner(HttpServerRequest request) {
        return "true".equals(request.getParam(S3Api.FETCH_OWNER));
    }

    public static void failSigV2(HttpServerRequest request) {
        // Some clients (e.g. s3cmd) try SIGV2 first and then upgrade to SIGV4 if the request fails.
        // To make sure those clients work we want to return a specific error
        // code if the Authorization header is incorrect.
        final String authorization = request.headers().get(HttpHeaders.AUTHORIZATION);
        if (authorization != null && !authorization.startsWith("AWS4-HMAC-SHA256")) {
            throw new S3HttpException(S3ErrorCode.INVALID_REQUEST,
                "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.",
                request.path());
        }
    }

    @Nullable
    public static String getContentSha256(HttpServerRequest request) {
        final String contentSha256 = request.headers().get(S3Api.CONTENT_SHA256);
        if (request.params().contains("X-Amz-Algorithm")) {
            return S3Api.UNSIGNED_PAYLOAD;
        } else if (contentSha256 != null && contentSha256.equals(S3Api.STREAMING_SHA256_PAYLOAD)) {
            throw new S3HttpException(S3ErrorCode.INVALID_REQUEST,
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD is not supported.", request.path());
        } else {
            return contentSha256;
        }
    }

    public static boolean isSignedPayload(String contentSha256) {
        return contentSha256 != null && !S3Api.UNSIGNED_PAYLOAD.equals(contentSha256);
    }

    public static void validateSha256(String expectedSha256, byte[] bytes, HttpServerRequest request) {
        if (isSignedPayload(expectedSha256)) {
            final String actualSha256 = ChecksumHelper.computeHexSHA256(bytes);
            if (!actualSha256.equals(expectedSha256)) {
                throw new S3HttpException(S3ErrorCode.CONTENT_SHA_MISMATCH,
                        "The provided 'x-amz-content-sha256' header does not match what was computed.",
                        request.path());
            }
        }
    }

    public static void validateEmptySha256(String expectedSha256, HttpServerRequest request) {
        if (isSignedPayload(expectedSha256) && !S3Api.SHA256_EMPTY.equalsIgnoreCase(expectedSha256)) {
            throw new S3HttpException(S3ErrorCode.CONTENT_SHA_MISMATCH,
                "The provided 'x-amz-content-sha256' header does not match what was computed.",
                request.path());
        }
    }

    public static <T> void writeXmlResponse(HttpServerResponse response, T content, XmlMapper mapper)
        throws JsonProcessingException {
        String body = mapper.writeValueAsString(content);
        response.setStatusCode(HttpResponseStatus.OK);
        response.putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_XML_UTF8);
        response.end(body);
    }

    public static void startChunkedXmlResponse(HttpServerResponse response, boolean printXmlTag) {
        response.putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_XML_UTF8);
        response.setChunked(true);

        if (printXmlTag) {
            response.write("<?xml version='1.0' encoding='UTF-8'?>");
        }
    }

    public static <T> void endChunkedXmlResponse(HttpServerResponse response, T content, XmlMapper mapper)
            throws JsonProcessingException {
        response.setStatusCode(HttpResponseStatus.OK);
        String body = mapper.writeValueAsString(content);
        response.write(body);
        response.end();
    }

    public static void endChunkedXmlErrorResponse(RoutingContext context, Throwable thrown, XmlMapper mapper) {
        S3HttpException exception = (S3HttpException) S3HttpException.rewrite(context, thrown);

        if (context.response().headWritten()) {
            try {
                context.response().setStatusCode(exception.getError().getErrorCode().getStatusCode());
                String body = mapper.writeValueAsString(exception.getError());
                context.response().write(body);
            } catch (JsonProcessingException e) {
                // Can't do anything else
            } finally {
                context.response().end();
            }
        }
    }

    public static CompleteMultipartUpload parseCompleteMultipartUpload(byte[] bytes) throws Exception {
        final CompleteMultipartUpload completeUpload = getXmlMapper().readValue(bytes, CompleteMultipartUpload.class);
        Preconditions.checkNotNull(completeUpload.getPart());
        completeUpload.getPart().forEach(part -> {
            Preconditions.checkNotNull(part.getPartNumber());
            Preconditions.checkNotNull(part.getETag());
        });
        return completeUpload;
    }

    public static Delete parseBulkDelete(byte[] bytes, HttpServerRequest request)  {
        final Delete delete;
        try {
            delete = getXmlMapper().readValue(bytes, Delete.class);
        } catch (Exception ex) {
            throw s3ErrorXML(request);
        }
        int size = delete.getObject() == null ? 0 : delete.getObject().size();
        if (size <= 0 || size > 1000) {
            throw s3ErrorXML(request);
        }
        delete.getObject().forEach(object -> {
            if (object.getKey() == null) {
                throw s3ErrorXML(request);
            }
        });
        return delete;
    }

    public static S3HttpException s3ErrorXML(HttpServerRequest request) {
         return new S3HttpException(S3ErrorCode.MALFORMED_XML,
                "The XML you provided was not well-formed or did not validate against our published schema.",
                // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                request.path().replace("/%2F", "//"));
    }

    public static RestoreRequest parseRestoreRequest(byte[] bytes, HttpServerRequest request)  {
        final RestoreRequest restoreRequest;
        try {
            restoreRequest = getXmlMapper().readValue(bytes, RestoreRequest.class);
        } catch (Exception ex) {
            throw new S3HttpException(S3ErrorCode.MALFORMED_XML,
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                    // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                    request.path().replace("/%2F", "//"));
        }
        if ((restoreRequest.getDays() <= 0) || (restoreRequest.getDays() > 10)) {
            throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT,
                    "Days parameter range is between 1 and 10",
                    // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                    request.path().replace("/%2F", "//"));
        }
        return restoreRequest;
    }

    public static String getLocation(String bucketName, String objectName) {
        return "/" + bucketName + "/" + objectName;
    }

    public static String getLocation(String bucketName) {
        return "/" + bucketName;
    }

    public static void validateRangeHeader(HttpServerRequest request) {
        Optional.ofNullable(request.getHeader(S3Api.COPY_SOURCE_RANGE_HEADER))
                .ifPresent(rangeHeader -> {
                    boolean validate = ByteRange.isValidHeader(rangeHeader);
                    if (!validate) {
                        throw new S3HttpException(S3ErrorCode.INVALID_REQUEST, "Invalid range",
                                request.path());
                    }
                });
    }

    public static ByteRange tryParseByteRange(HttpServerRequest request, long totalSizeInBytes) {
        //Special case when a byte range was passed but the content length is zero.
        if (request.headers().contains(S3Api.COPY_SOURCE_RANGE_HEADER) && totalSizeInBytes == 0) {
            throw new S3HttpException(S3ErrorCode.INVALID_REQUEST, "The source object is empty",
                    request.path());
        }
        try {
            return ByteRange.fromWithoutAutoCorrectingEnd(request.getHeader(S3Api.COPY_SOURCE_RANGE_HEADER),
                    totalSizeInBytes);
        } catch (IllegalStateException | IllegalArgumentException e) {
            throw new S3HttpException(S3ErrorCode.INVALID_REQUEST, "Invalid request",
                    request.path());
        }
    }
}
