package com.webutils.webserver.utils;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

public enum V2ErrorCode implements ErrorCode {
    BAD_REQUEST(400, "BadRequest"),
    INVALID_JSON(400, "InvalidJSON"),
    MISMATCHED_NAMESPACE_NAMES(400, "MismatchedNamespaceNames"),
    MISMATCHED_BUCKET_NAMES(400, "MismatchedBucketNames"),
    INVALID_NAMESPACE_NAME(400, "InvalidNamespaceName"),
    INVALID_BUCKET_NAME(400, "InvalidBucketName"),
    INVALID_OBJECT_NAME(400, "InvalidObjectName"),
    INVALID_ACTION_TYPE(400, "InvalidLifecycleActionType"),
    INVALID_RULE_NAME(400, "InvalidLifecycleRuleName"),
    INVALID_TIME_AMOUNT(400, "InvalidLifecycleTimeAmount"),
    INVALID_TIME_UNIT(400, "InvalidLifecycleTimeUnit"),
    INVALID_PREFIX(400, "InvalidLifecyclePrefix"),
    INVALID_PATTERN(400, "InvalidLifecyclePattern"),
    INVALID_OBJECT_PROPERTY(400, "InvalidObjectProperty"),
    INVALID_BUCKET_PROPERTY(400, "InvalidBucketProperty"),
    INVALID_ATTRIBUTE_NAME(400, "InvalidAttributeName"),
    INVALID_PAGE(400, "InvalidPage"),
    MISSING_BUCKET_NAME(400, "MissingBucketName"),
    MISSING_COMPARTMENT(400, "MissingCompartmentId"),
    MISSING_OBJECT_NAME(400, "MissingObjectName"),
    MISSING_NAMESPACE(400, "MissingNamespace"),
    MISSING_UPLOAD_ID(400, "MissingUploadId"),
    MISSING_PAR_CREATE_DETAILS(400, "MissingParCreateDetails"),
    INVALID_PAR_CREATE_REQUEST(400, "InvalidParCreateRequest"),
    INVALID_UPLOAD_PART(400, "InvalidUploadPart"),
    INVALID_UPLOAD_PART_ORDER(400, "InvalidUploadPartOrder"),
    INVALID_METADATA(400, "InvalidMetadata"),
    INVALID_NAMESPACE_METADATA(400, "InvalidNamespaceMetadata"),
    INVALID_CLIENT_REQ_ID(400, "InvalidClientRequestId"),
    INVALID_CONTENT_MD5(400, "InvalidContentMD5"),
    INVALID_CONTENT_LEN(400, "InvalidContentLength"),
    INVALID_UPLOAD_PART_NUMBER(400, "InvalidUploadPartNumber"),
    UNMATCHED_CONTENT_MD5(400, "UnmatchedContentMD5"),
    INVALID_LIMIT(400, "InvalidLimit"),
    METADATA_TOO_LARGE(400, "MetadataTooLarge"),
    INVALID_CONDITIONAL_HEADERS(400, "InvalidConditionalHeaders"),
    IF_MATCH_DISALLOWED(400, "IfMatchDisallowed"),
    IF_NONE_MATCH_DISALLOWED(400, "IfNoneMatchDisallowed"),
    NO_COND_MULT_ENTITIES(400, "NoCondMultEntities"),
    INVALID_MANIFEST_HEADER(400, "InvalidManifestHeader"),
    INVALID_SORT_BY(400, "InvalidSortBy"),
    INVALID_SORT_ORDER(400, "InvalidSortOrder"),
    INVALID_METER_SETTING(400, "InvalidMeterSetting"),
    INVALID_BUCKET_OPTIONS_JSON(400, "InvalidBucketOptionsJSON"),
    INVALID_PAR_URI(400, "InvalidParUri"),
    SOURCE_SAME_AS_NEW(400, "SourceNameSameAsNewName"),
    TOO_MANY_BUCKETS(400, "TooManyBuckets"),
    INVALID_ARCHIVE_STATE(400, "InvalidArchiveState"),
    INVALID_CHECKSUM(400, "MissingOrInvalidChecksum"),
    INVALID_CHECKSUM_TYPE(400, "MissingOrInvalidChecksumType"),
    INVALID_ENCODING_TYPE(400, "InvalidEncodingType"),
    NOT_ARCHIVED_OBJECT(400, "NotArchivedObject"),
    TAGS_TOO_LARGE(400, "TagsTooLarge"),
    INVALID_TAG_KEY(400, "InvalidTagKey"),
    INVALID_TAG_VALUE(400, "InvalidTagValue"),
    INVALID_RESTORE_HOURS(400, "InvalidRestoreHours"),
    INVALID_REGION(400, "InvalidRegion"),
    INVALID_KMS_KEY_ID(400, "InvalidKmsKeyId"),
    MALFORMED_URI(400, "MalformedURI"),
    INVALID_COMPARTMENT_ID(400, "InvalidCompartmentId"),
    INVALID_TENANT_ID(400, "InvalidTenantId"),
    INVALID_WORK_REQUEST_TYPE(400, "InvalidWorkRequestType"),
    INVALID_WORK_REQUEST_ID(400, "InvalidWorkRequestId"),
    INVALID_BUCKET_ID(400, "InvalidBucketId"),
    INVALID_REPLICATION_POLICY_ID(400, "InvalidReplicationPolicyId"),
    INVALID_REPLICATION_POLICY_NAME(400, "InvalidReplicationPolicyName"),
    INVALID_REPLICATION_OPTIONS(400, "InvalidReplicationOptions"),
    INVALID_GLOB(400, "InvalidGlob"),
    INSUFFICIENT_SERVICE_PERMISSIONS(400, "InsufficientServicePermissions"),
    RELATED_RESOURCE_NOT_FOUND(400, "RelatedResourceNotAuthorizedOrNotFound"),
    STORAGE_LIMIT_EXCEEDED(400, "StorageLimitExceeded"),
    STORAGE_QUOTA_EXCEEDED(400, "StorageQuotaExceeded"),
    UNAUTHORIZED(401, "Unauthorized"),
    NOT_AUTHENTICATED(401, "NotAuthenticated"),
    CLOCK_SKEW(401, "NotAuthenticated"),
    FORBIDDEN(403, "Forbidden"),
    BUCKET_IS_READONLY(403, "BucketIsReadonly"),
    BUCKET_REPLICATION_ENABLED(403, "BucketReplicationEnabled"),
    NOT_FOUND(404, "NotFound"),
    NAMESPACE_NOT_FOUND(404, "NamespaceNotFound"),
    OBJECT_NOT_FOUND(404, "ObjectNotFound"),
    BUCKET_NOT_FOUND(404, "BucketNotFound"),
    LIFECYCLE_POLICY_NOT_FOUND(404, "LifecyclePolicyNotFound"),
    UPLOAD_NOT_FOUND(404, "UploadNotFound"),
    PAR_NOT_FOUND(404, "PreauthenticatedRequestNotFound"),
    COMPARTMENT_ID_NOT_FOUND(404, "CompartmentIdNotFound"),
    CHUNK_NOT_FOUND(404, "ObjectChunkNotFound"),
    INVALID_NOT_AUTHORIZED_TAGSET(404, "Invalid/NotAuthorized/NotFound"),
    NOT_AUTHORIZED_FOUND_KMS_KEY(404, "NotAuthorizedOrFoundKmsKey"),
    KMS_HOST_NOT_FOUND(404, "KmsHostNotFound"),
    KMS_KEY_NOT_PROVIDED(404, "KmsKeyIdNotProvided"),
    REPLICATION_POLICY_CLIENT_ERROR(404, "ReplicationPolicyClientError"),
    NOT_ACCEPTABLE(406, "NotAcceptable"),
    CREATE_REPLICATION_POLICY_TIMEOUT(408, "CreateReplicationPolicyTimeout"),
    CONFLICT(409, "Conflict"),
    BUCKET_NOT_EMPTY(409, "BucketNotEmpty"),
    PAR_STILL_EXISTS(409, "PreauthenticatedRequestStillExists"),
    BUCKET_ALREADY_EXISTS(409, "BucketAlreadyExists"),
    CONCURRENT_BUCKET_UPDATE(409, "ConcurrentBucketUpdate"),
    CONCURRENT_OBJECT_UPDATE(409, "ConcurrentObjectUpdate"),
    NOT_RESTORED(409, "NotRestored"),
    RESTORED_CHUNK_CONFLICT(409, "RestoredChunkConflict"),
    RESTORE_IN_PROGRESS(409, "RestoreInProgress"),
    ALREADY_RESTORED_OBJECT(409, "AlreadyRestored"),
    ALREADY_ARCHIVED_OBJECT(409, "AlreadyArchived"),
    NOT_CANCELABLE(409, "NotCancelable"),
    KMS_KEY_DISABLED(409, "KmsKeyDisabled"),
    ENCRYPTION_STATE_CONFLICT(409, "ObjectNotEncryptedWithKms"),
    REQUEST_ALREADY_EXISTS(409, "RequestAlreadyExists"),
    REPLICATION_POLICY_CONFLICT(409, "ReplicationPolicyConflict"),
    REPLICATION_OPTIONS_CONFLICT(409, "ReplicationOptionsConflict"),
    BUCKET_NOT_READONLY(409, "BucketNotReadOnly"),
    CONTENT_LEN_REQUIRED(411, "ContentLengthRequired"),
    PRECONDITION_FAILED(412, "PreconditionFailed"),
    LIMIT_TOO_HIGH(412, "LimitTooHigh"),
    IF_MATCH_FAILED(412, "IfMatchFailed"),
    IF_NONE_MATCH_FAILED(412, "IfNoneMatchFailed"),
    ETAG_MISMATCH(412, "ETagMismatch"),
    NAMESPACE_DELETED(412, "NamespaceDeleted"),
    ENTITY_TOO_LARGE(413, "RequestEntityTooLarge"),
    UNSUPPORTED_MEDIA_TYPE(415, "UnsupportedMediaType"),
    INVALID_CONTENT_TYPE(415, "InvalidContentType"),
    INVALID_CONTENT_ENC(415, "InvalidContentEncoding"),
    INVALID_RANGE(416, "InvalidRange"),
    UNMATCHED_ETAG_MD5(422, "ETagMD5Mismatch"),
    IF_MATCH_REQUIRED(428, "IfMatchRequired"),
    TOO_MANY_REQUESTS(429, "TooManyRequests"),
    INTERNAL_SERVER_ERROR(500, "InternalServerError"),
    NOT_IMPLEMENTED(501, "NotImplemented"),
    SERVICE_UNAVAILABLE(503, "ServiceUnavailable");

    private int statusCode;
    private String errorName;
    private static final Map<Integer, V2ErrorCode> FROM_STATUS_CODE = ImmutableMap.<Integer, V2ErrorCode>builder().put(BAD_REQUEST.statusCode, BAD_REQUEST).put(UNAUTHORIZED.statusCode, UNAUTHORIZED).put(FORBIDDEN.statusCode, FORBIDDEN).put(NOT_FOUND.statusCode, NOT_FOUND).put(CONFLICT.statusCode, CONFLICT).put(PRECONDITION_FAILED.statusCode, PRECONDITION_FAILED).put(ENTITY_TOO_LARGE.statusCode, ENTITY_TOO_LARGE).put(UNSUPPORTED_MEDIA_TYPE.statusCode, UNSUPPORTED_MEDIA_TYPE).put(TOO_MANY_REQUESTS.statusCode, TOO_MANY_REQUESTS).put(INTERNAL_SERVER_ERROR.statusCode, INTERNAL_SERVER_ERROR).put(NOT_IMPLEMENTED.statusCode, NOT_IMPLEMENTED).put(SERVICE_UNAVAILABLE.statusCode, SERVICE_UNAVAILABLE).build();
    private static final Map<String, V2ErrorCode> FROM_NAME = new HashMap();

    static {
        V2ErrorCode[] var3;
        int var2 = (var3 = values()).length;

        for(int var1 = 0; var1 < var2; ++var1) {
            V2ErrorCode errorCode = var3[var1];
            FROM_NAME.put(errorCode.errorName, errorCode);
        }
    }

    private V2ErrorCode(int statusCode, String errorName) {
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
        return "V2ErrorCode{statusCode=" + this.statusCode + ", errorName='" + this.errorName + '\'' + '}';
    }

    public static V2ErrorCode fromStatusCode(int statusCode) {
        return (V2ErrorCode)FROM_STATUS_CODE.getOrDefault(statusCode, INTERNAL_SERVER_ERROR);
    }

    public static V2ErrorCode fromName(String errorName) {
        return (V2ErrorCode)FROM_NAME.get(errorName);
    }
}
