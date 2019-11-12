package com.oracle.pic.casper.webserver.api.ratelimit;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents all possible parameters that can match elements
 * of an {@link EmbargoV3Rule}.
 */
public final class EmbargoV3Operation {

    private static final Logger LOG = LoggerFactory.getLogger(EmbargoV3Operation.class);

    /**
     * Describes the API the call is being made on. We can not use the existing
     * API enum because it only differentiates between V1 and V2 (but not S3
     * and Swift).
     *
     * V1 does not appear here because the V1 API is deprecated and it will not
     * be supported by EmbargoV3.
     */
    public enum Api {
        Swift("swift"),
        S3("s3"),
        V1("v1"),
        V2("v2");

        private final String name;

        Api(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * The API-type of the request ("v2", "s3", or "swift").
     */
    private final Api api;

    /**
     * The operation being performed (e.g. "getNamespace", "listObjects").
     */
    private final CasperOperation operation;

    /**
     * The namespace the operation is being performed in (e.g. "bmcostests").
     */
    private final String namespace;

    /**
     * The bucket the operation is being performed against.
     */
    private final String bucket;

    /**
     * The object the operation is being performed against.
     */
    private final String object;

    private EmbargoV3Operation(
            @Nonnull Api api,
            @Nonnull CasperOperation operation,
            @Nullable String namespace,
            @Nullable String bucket,
            @Nullable String object) {
        this.api = Preconditions.checkNotNull(api);
        this.operation = Preconditions.checkNotNull(operation);
        this.namespace = namespace;
        this.bucket = bucket;
        this.object = object;
        this.checkState();
    }

    @Nonnull
    public Api getApi() {
        return api;
    }

    @Nonnull
    public CasperOperation getOperation() {
        return operation;
    }

    @Nullable
    public String getNamespace() {
        return namespace;
    }

    @Nullable
    public String getBucket() {
        return bucket;
    }

    @Nullable
    public String getObject() {
        return object;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("api", api.getName())
            .add("operation", operation.getSummary())
            .add("namespace", namespace)
            .add("bucket", bucket)
            .add("object", object)
            .toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Make sure the combination of operation time and arguments makes sense.
     */
    private void checkState() {
        Preconditions.checkState(api != null); // Must have an API
        Preconditions.checkState(operation != null); // Must have an operation
        /*
            ListWorkRequest operation takes a bucket, but no namespace
         */
        //Preconditions.checkState(bucket == null || namespace != null); // Can't have bucket without a namespace
        Preconditions.checkState(object == null || bucket != null); // Can't have an object without a bucket
        switch (operation) {
            // General operations
            case LIST_WORK_REQUESTS:
                // List work request takes an optional bucket name, but doesn't take a namespace parameter.
                // So, while ListWorkRequest is a valid operation, we cannot check the operation w.r.t
                // namespace, bucket, or object parameters.
                break;
            case GET_NAMESPACE:
            case GET_WORK_REQUEST:
            case CANCEL_WORK_REQUEST:
                Preconditions.checkState(bucket == null);
                Preconditions.checkState(object == null);
                break;

            // Namespace-level operations
            case GET_NAMESPACE_METADATA:
            case LIST_BUCKETS:
            case UPDATE_NAMESPACE_METADATA:
                Preconditions.checkState(namespace != null);
                Preconditions.checkState(bucket == null);
                Preconditions.checkState(object == null);
                break;

            // Bucket-level operations
            case CREATE_BUCKET:
            case DELETE_BUCKET:
            case CREATE_BULK_RESTORE_REQUEST:
            case CREATE_PAR:
            case CREATE_REPLICATION_POLICY:
            case DELETE_PAR:
            case DELETE_REPLICATION_POLICY:
            case GET_BUCKET:
            case GET_PAR:
            case GET_REPLICATION_POLICY:
            case HEAD_BUCKET:
            case CREATE_MULTIPART_UPLOAD:
            case LIST_MULTIPART_UPLOAD:
            case LIST_OBJECTS:
            case LIST_PARS:
            case LIST_REPLICATION_POLICIES:
            case PUT_OBJECT_LIFECYCLE_POLICY:
            case UPDATE_BUCKET:
            case UPDATE_REPLICATION_POLICY:
            case REENCRYPT_BUCKET:
            case MAKE_BUCKET_WRITABLE:
            case UPDATE_BUCKET_OPTIONS:
            case LIST_REPLICATION_SOURCES:
            case GET_BUCKET_OPTIONS:
            case ADD_BUCKET_METER_SETTING:
            case PURGE_DELETED_TAGS:
                Preconditions.checkState(namespace != null);
                Preconditions.checkState(bucket != null);
                Preconditions.checkState(object == null);
                break;

            // Object-level operations
            case ARCHIVE_OBJECT:
            case ABORT_MULTIPART_UPLOAD:
            case COMMIT_MULTIPART_UPLOAD:
            case COPY_PART:
            case CREATE_COPY_REQUEST:
            case DELETE_OBJECT:
            case GET_OBJECT:
            case HEAD_OBJECT:
            case LIST_MULTIPART_UPLOAD_PARTS:
            case MERGE_OBJECT_METADATA:
            case PUT_OBJECT:
            case RENAME_OBJECT:
            case REPLACE_OBJECT_METADATA:
            case RESTORE_OBJECT:
            case UPLOAD_PART:
            case REENCRYPT_OBJECT:
                Preconditions.checkState(namespace != null);
                Preconditions.checkState(bucket != null);
                Preconditions.checkState(object != null);
                break;

            default:
                LOG.warn("Unknown operation {} used to create {}", operation, this.getClass().getSimpleName());
                throw new IllegalStateException(String.format(
                        "Unknown operation %s used to create %s", operation, this.getClass().getSimpleName()));
        }
    }

    /**
     * Used to create an {@link EmbargoV3Operation}. The builder is mutated
     * in-place instead of returning a copy because we do not want to
     * create too many temporary objects.
     */
    public static final class Builder {
        /**
         * The API-type of the request ("v2", "s3", or "swift").
         */
        private Api api;

        /**
         * The operation being performed (e.g. "getNamespace", "listObjects").
         */
        private CasperOperation operation;

        /**
         * The namespace the operation is being performed in (e.g. "bmcostests").
         */
        private String namespace;

        /**
         * The bucket the operation is being performed against.
         */
        private String bucket;

        /**
         * The object the operation is being performed against.
         */
        private String object;

        private Builder() {
        }

        public Builder setApi(Api api) {
            this.api = api;
            return this;
        }

        public Builder setOperation(CasperOperation operation) {
            this.operation = operation;
            return this;
        }

        public Builder setNamespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder setBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder setObject(String object) {
            this.object = object;
            return this;
        }

        public EmbargoV3Operation build() {
            return new EmbargoV3Operation(
                this.api,
                this.operation,
                this.namespace,
                this.bucket,
                this.object);
        }
    }
}
