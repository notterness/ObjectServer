package com.webutils.webserver.utils;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public enum S3ErrorCode implements ErrorCode {
    NOT_MODIFIED(304, "NotModified"),
    BAD_REQUEST(400, "BadRequest"),
    AUTHORIZATION_HEADER_MALFORMED(400, "AuthorizationHeaderMalformed"),
    AUTHORIZATION_QUERY_PARAMETERS_ERROR(400, "AuthorizationQueryParametersError"),
    BAD_DIGEST(400, "BadDigest"),
    ENTITY_TOO_LARGE(400, "EntityTooLarge"),
    INVALID_ARGUMENT(400, "InvalidArgument"),
    INVALID_BUCKET_NAME(400, "InvalidBucketName"),
    INVALID_PART(400, "InvalidPart"),
    INVALID_REQUEST(400, "InvalidRequest"),
    INVALID_URI(400, "InvalidURI"),
    KEY_TOO_LONG(400, "KeyTooLong"),
    MALFORMED_XML(400, "MalformedXML"),
    METADATA_TOO_LARGE(400, "MetadataTooLarge"),
    TOO_MANY_BUCKETS(400, "TooManyBuckets"),
    CONTENT_SHA_MISMATCH(400, "XAmzContentSHA256Mismatch"),
    INVALID_TAG(400, "InvalidTag"),
    MALFORMED_URI(400, "MalformedURI"),
    CONTENT_MD5_UNMATCHED(400, "Content-MD5Unmatched"),
    STORAGE_LIMIT_EXCEEDED(400, "StorageLimitExceeded"),
    STORAGE_QUOTA_EXCEEDED(400, "StorageQuotaExceeded"),
    FORBIDDEN(403, "Forbidden"),
    ACCESS_DENIED(403, "AccessDenied"),
    INVALID_OBJECT_STATE(403, "InvalidObjectState"),
    SIGNATURE_DOES_NOT_MATCH(403, "SignatureDoesNotMatch"),
    REQUEST_TIME_TOO_SKEWED(403, "RequestTimeTooSkewed"),
    BUCKET_IS_READONLY(403, "BucketIsReadonly"),
    BUCKET_REPLICATION_ENABLED(403, "BucketReplicationEnabled"),
    NOT_FOUND(404, "NotFound"),
    NO_SUCH_BUCKET(404, "NoSuchBucket"),
    NO_SUCH_KEY(404, "NoSuchKey"),
    NO_SUCH_UPLOAD(404, "NoSuchUpload"),
    NO_SUCH_TAGSET(404, "NoSuchTagSet"),
    NO_SUCH_CORS_CONFIGURATION(404, "NoSuchCORSConfiguration"),
    NO_SUCH_LIFECYCLE_CONFIGURATION(404, "NoSuchLifecycleConfiguration"),
    NO_SUCH_REPLICATION_CONFIGURATION(404, "NoSuchReplicationConfiguration"),
    NO_SUCH_WEBSITE_CONFIGURATION(404, "NoSuchWebsiteConfiguration"),
    NO_SUCH_BUCKET_POLICY(404, "NoSuchBucketPolicy"),
    CONFLICT(409, "Conflict"),
    BUCKET_ALREADY_EXISTS(409, "BucketAlreadyExists"),
    BUCKET_NOT_EMPTY(409, "BucketNotEmpty"),
    CONCURRENT_OBJECT_UPDATE(409, "ConcurrentObjectUpdate"),
    RESTORE_ALREADY_IN_PROGRESS(409, "RestoreAlreadyInProgress"),
    ALREADY_ARCHIVED_OBJECT(409, "AlreadyArchived"),
    PAR_STILL_EXISTS(409, "PreauthenticatedRequestStillExists"),
    MISSING_CONTENT_LENGTH(411, "MissingContentLength"),
    PRECONDITION_FAILED(412, "PreconditionFailed"),
    NAMESPACE_DELETED(412, "NamespaceDeleted"),
    INVALID_RANGE(416, "InvalidRange"),
    TOO_MANY_REQUESTS(429, "TooManyRequests"),
    INTERNAL_ERROR(500, "InternalError"),
    NOT_IMPLEMENTED(501, "NotImplemented"),
    SERVICE_UNAVAILABLE(503, "ServiceUnavailable"),
    SERVICE_UNAVAILABLE_503(503, "503 Service Unavailable");

    private final int statusCode;
    private String errorName;
    private static final Map<Integer, S3ErrorCode> FROM_STATUS_CODE = ImmutableMap.<Integer, S3ErrorCode>builder().put(BAD_REQUEST.statusCode, BAD_REQUEST).put(FORBIDDEN.statusCode, FORBIDDEN).put(NOT_FOUND.statusCode, NOT_FOUND).put(CONFLICT.statusCode, CONFLICT).put(PRECONDITION_FAILED.statusCode, PRECONDITION_FAILED).put(INTERNAL_ERROR.statusCode, INTERNAL_ERROR).put(NOT_IMPLEMENTED.statusCode, NOT_IMPLEMENTED).put(SERVICE_UNAVAILABLE.statusCode, SERVICE_UNAVAILABLE).put(413, ENTITY_TOO_LARGE).build();

    private S3ErrorCode(int statusCode, String errorName) {
        this.statusCode = statusCode;
        this.errorName = errorName;
    }

    public int getStatusCode() {
        return this.statusCode;
    }

    public String getErrorName() {
        return this.errorName;
    }

    public String toString() {
        return "S3ErrorCode{statusCode=" + this.statusCode + ", errorName='" + this.errorName + '\'' + '}';
    }

    public static S3ErrorCode fromStatusCode(int statusCode) {
        return (S3ErrorCode)FROM_STATUS_CODE.getOrDefault(statusCode, INTERNAL_ERROR);
    }
}
