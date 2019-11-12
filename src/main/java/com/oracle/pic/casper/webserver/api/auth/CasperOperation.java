package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.webserver.auth.CasperResourceKind;
import com.oracle.pic.casper.webserver.api.auditing.AuditLevel;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.identity.authorization.permissions.ActionKind;


/**
 * Casper operation names for use when authorizing client requests (see {@link Authorizer}.
 *
 * Each operation has a "summary" name which is the name sent to the authorization service when authorizing client
 * requests. The "summary" name must match the "operationId" field of the operation in the Swagger specification.
 *
 * The "isCreateOp" field is used to determine what happens when an account is suspended. Resources in a suspended
 * account may be read but not created, so we disallow operations that would create buckets or objects, and allow
 * operations that would only read, delete or update the value of buckets or objects.
 *
 * "actionKind" is a hint to Identity about the kind of action so that fast decisions can be made for some requests.
 *
 * "resourceKind" is the kind of resource the operation is acting upon.
 *
 * WARNING: These values are used by Embargo V3 rules (see
 * {@link EmbargoV3Operation} and
 * {@link com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Rule}. Changing the {@link #summary} strings will
 * break those rules.
 */
public enum CasperOperation {
    GET_NAMESPACE("GetNamespace", false, ActionKind.READ, CasperResourceKind.NAMESPACES, AuditLevel.ALWAYS),
    GET_NAMESPACE_METADATA("GetNamespaceMetadata", false, ActionKind.READ, CasperResourceKind.NAMESPACES,
        AuditLevel.ALWAYS),
    UPDATE_NAMESPACE_METADATA("UpdateNamespaceMetadata", false, ActionKind.UPDATE, CasperResourceKind.NAMESPACES,
        AuditLevel.ALWAYS),
    LIST_BUCKETS("ListBuckets", false, ActionKind.LIST, CasperResourceKind.BUCKETS, AuditLevel.ALWAYS),
    CREATE_BUCKET("CreateBucket", true, ActionKind.CREATE, CasperResourceKind.BUCKETS, AuditLevel.ALWAYS),
    GET_BUCKET("GetBucket", false, ActionKind.READ, CasperResourceKind.BUCKETS, AuditLevel.ALWAYS),
    HEAD_BUCKET("HeadBucket", false, ActionKind.READ, CasperResourceKind.BUCKETS, AuditLevel.ALWAYS),
    UPDATE_BUCKET("UpdateBucket", false, ActionKind.UPDATE, CasperResourceKind.BUCKETS, AuditLevel.ALWAYS),
    DELETE_BUCKET("DeleteBucket", false, ActionKind.DELETE, CasperResourceKind.BUCKETS, AuditLevel.ALWAYS),
    LIST_OBJECTS("ListObjects", false, ActionKind.LIST, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    GET_OBJECT("GetObject", false, ActionKind.READ, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    HEAD_OBJECT("HeadObject", false, ActionKind.READ, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    PUT_OBJECT("PutObject", true, ActionKind.CREATE, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    DELETE_OBJECT("DeleteObject", false, ActionKind.DELETE, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    RENAME_OBJECT("RenameObject", false, ActionKind.UPDATE, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    CREATE_MULTIPART_UPLOAD("CreateMultipartUpload", true, ActionKind.CREATE, CasperResourceKind.OBJECTS,
        AuditLevel.OBJECT_ACCESS),
    LIST_MULTIPART_UPLOAD("ListMultipartUploads", false, ActionKind.LIST, CasperResourceKind.BUCKETS,
        AuditLevel.OBJECT_ACCESS),
    COMMIT_MULTIPART_UPLOAD("CommitMultipartUpload", true, ActionKind.CREATE, CasperResourceKind.OBJECTS,
        AuditLevel.OBJECT_ACCESS),
    ABORT_MULTIPART_UPLOAD("AbortMultipartUpload", false, ActionKind.DELETE, CasperResourceKind.OBJECTS,
        AuditLevel.OBJECT_ACCESS),
    LIST_MULTIPART_UPLOAD_PARTS("ListMultipartUploadParts", false, ActionKind.LIST, CasperResourceKind.OBJECTS,
        AuditLevel.OBJECT_ACCESS),
    UPLOAD_PART("UploadPart", true, ActionKind.CREATE, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    COPY_PART("CopyPart", true, ActionKind.CREATE, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    CREATE_PAR("CreatePar", true, ActionKind.CREATE, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    GET_PAR("GetPar", false, ActionKind.READ, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    DELETE_PAR("DeletePar", false, ActionKind.DELETE, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    LIST_PARS("ListPars", false, ActionKind.LIST, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    RESTORE_OBJECT("RestoreObjects", false, ActionKind.UPDATE, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    ARCHIVE_OBJECT("ArchiveObjects", false, ActionKind.UPDATE, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    CREATE_COPY_REQUEST("CreateCopyRequest", true, ActionKind.CREATE, CasperResourceKind.OBJECTS,
        AuditLevel.OBJECT_ACCESS),
    CREATE_BULK_RESTORE_REQUEST("CreateBulkRestoreRequest", true, ActionKind.CREATE, CasperResourceKind.OBJECTS,
        AuditLevel.OBJECT_ACCESS),
    GET_WORK_REQUEST("GetWorkRequest", false, ActionKind.READ, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    LIST_WORK_REQUESTS("ListWorkRequests", false, ActionKind.LIST, CasperResourceKind.OBJECTS,
        AuditLevel.OBJECT_ACCESS),
    CANCEL_WORK_REQUEST("CancelWorkRequest", false, ActionKind.DELETE, CasperResourceKind.OBJECTS,
        AuditLevel.OBJECT_ACCESS),
    MERGE_OBJECT_METADATA("MergeObjectMetadata", false, ActionKind.UPDATE, CasperResourceKind.OBJECTS,
        AuditLevel.OBJECT_ACCESS),
    REPLACE_OBJECT_METADATA("ReplaceObjectMetadata", false, ActionKind.UPDATE, CasperResourceKind.OBJECTS,
        AuditLevel.OBJECT_ACCESS),
    PUT_OBJECT_LIFECYCLE_POLICY("PutObjectLifecyclePolicy", true, ActionKind.CREATE, CasperResourceKind.OBJECT_FAMILY,
        AuditLevel.ALWAYS),
    CREATE_REPLICATION_POLICY("CreateReplicationPolicy", true, ActionKind.CREATE, CasperResourceKind.BUCKETS,
        AuditLevel.ALWAYS),
    GET_REPLICATION_POLICY("GetReplicationPolicy", false, ActionKind.READ, CasperResourceKind.BUCKETS,
        AuditLevel.ALWAYS),
    UPDATE_REPLICATION_POLICY("UpdateReplicationPolicy", true, ActionKind.UPDATE, CasperResourceKind.BUCKETS,
        AuditLevel.ALWAYS),
    DELETE_REPLICATION_POLICY("DeleteReplicationPolicy", false, ActionKind.DELETE, CasperResourceKind.BUCKETS,
        AuditLevel.ALWAYS),
    LIST_REPLICATION_POLICIES("ListReplicationPolicies", false, ActionKind.LIST, CasperResourceKind.BUCKETS,
        AuditLevel.ALWAYS),
    LIST_REPLICATION_SOURCES("ListReplicationSources", false, ActionKind.LIST, CasperResourceKind.BUCKETS,
        AuditLevel.ALWAYS),
    MAKE_BUCKET_WRITABLE("MakeBucketWritable", false, ActionKind.UPDATE, CasperResourceKind.BUCKETS, AuditLevel.ALWAYS),

    /**
     * The following are non public supported CasperOperations (undocumented)
     */
    GET_SERVER_INFO("GetServerInfo", false, ActionKind.READ, CasperResourceKind.NAMESPACES, AuditLevel.NEVER),
    GET_CLIENT_INFO("GetClientInfo", false, ActionKind.READ, CasperResourceKind.NAMESPACES, AuditLevel.NEVER),
    REENCRYPT_OBJECT("ReencryptObject", false, ActionKind.UPDATE, CasperResourceKind.OBJECTS, AuditLevel.OBJECT_ACCESS),
    ADD_BUCKET_METER_SETTING("MeterSettingRBucket", false, ActionKind.CREATE, CasperResourceKind.BUCKETS,
        AuditLevel.NEVER),
    GET_BUCKET_OPTIONS("GetBucketOptions", false, ActionKind.READ, CasperResourceKind.BUCKETS,
        AuditLevel.NEVER),
    UPDATE_BUCKET_OPTIONS("UpdateBucketOptions", false, ActionKind.UPDATE, CasperResourceKind.BUCKETS,
        AuditLevel.NEVER),
    UPDATE_REPLICATION_OPTIONS("UpdateReplicationOptions", false, ActionKind.UPDATE, CasperResourceKind.BUCKETS,
            AuditLevel.NEVER),
    REENCRYPT_BUCKET("ReencryptBucket", false, ActionKind.UPDATE, CasperResourceKind.BUCKETS, AuditLevel.ALWAYS),
    CHECK_OBJECT("CheckObject", false, ActionKind.READ, CasperResourceKind.OBJECTS, AuditLevel.NEVER),
    PURGE_DELETED_TAGS("PurgeDeletedTags", false, ActionKind.UPDATE, CasperResourceKind.BUCKETS, AuditLevel.NEVER),
    POST_LOGGING("PostBucketLogging", false, ActionKind.UPDATE, CasperResourceKind.BUCKETS, AuditLevel.NEVER),
    DELETE_LOGGING("DeleteBucketLogging", false, ActionKind.UPDATE, CasperResourceKind.BUCKETS, AuditLevel.NEVER),
    GET_USAGE("GetUsage", false, ActionKind.READ, CasperResourceKind.NAMESPACES, AuditLevel.NEVER);

    private final String summary;
    private final boolean isCreateOp;
    private final ActionKind actionKind;
    private final CasperResourceKind resourceKind;
    //only audit namespace/bucket level operations which are low volume
    private AuditLevel auditLevel;

    CasperOperation(String summary, boolean isCreateOp, ActionKind actionKind, CasperResourceKind resourceKind,
                    AuditLevel auditLevel) {
        this.summary = summary;
        this.isCreateOp = isCreateOp;
        this.actionKind = actionKind;
        this.resourceKind = resourceKind;
        this.auditLevel = auditLevel;
    }

    public String getSummary() {
        return summary;
    }

    public boolean isCreateOp() {
        return isCreateOp;
    }

    public ActionKind getActionKind() {
        return actionKind;
    }

    public CasperResourceKind getResourceKind() {
        return resourceKind;
    }

    public AuditLevel getAuditLevel() {
        return auditLevel;
    }
}
