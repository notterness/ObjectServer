package com.oracle.pic.casper.webserver.api.backend;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Ordering;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;
import com.oracle.pic.casper.common.config.v2.ArchiveConfiguration;
import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.encryption.EncryptionConstants;
import com.oracle.pic.casper.common.encryption.EncryptionKey;
import com.oracle.pic.casper.common.encryption.EncryptionUtils;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.encryption.store.Secrets;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.AlreadyArchivedObjectException;
import com.oracle.pic.casper.common.exceptions.AlreadyRestoredObjectException;
import com.oracle.pic.casper.common.exceptions.AuthorizationException;
import com.oracle.pic.casper.common.exceptions.BucketLockedForMigrationException;
import com.oracle.pic.casper.common.exceptions.BucketLockedMigrationCompleteException;
import com.oracle.pic.casper.common.exceptions.IfMatchException;
import com.oracle.pic.casper.common.exceptions.IfNoneMatchException;
import com.oracle.pic.casper.common.exceptions.InternalServerErrorException;
import com.oracle.pic.casper.common.exceptions.InvalidUploadIdMarkerException;
import com.oracle.pic.casper.common.exceptions.InvalidUploadPartException;
import com.oracle.pic.casper.common.exceptions.KmsKeyNotProvidedException;
import com.oracle.pic.casper.common.exceptions.MultipartUploadNotFoundException;
import com.oracle.pic.casper.common.exceptions.NotArchivedObjectException;
import com.oracle.pic.casper.common.exceptions.ObjectNotFoundException;
import com.oracle.pic.casper.common.exceptions.ObjectTooLargeException;
import com.oracle.pic.casper.common.exceptions.RestoreInProgressException;
import com.oracle.pic.casper.common.exceptions.TooBusyException;
import com.oracle.pic.casper.common.json.JsonSerializer;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.metrics.MetricsBundle;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.model.BucketStorageTier;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.util.IdUtil;
import com.oracle.pic.casper.common.util.ParUtil;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.webserver.auth.AuthTestConstants;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.mds.MdsBucketKey;
import com.oracle.pic.casper.mds.MdsObjectKey;
import com.oracle.pic.casper.mds.common.client.MdsRequestId;
import com.oracle.pic.casper.mds.common.exceptions.MdsDbTooBusyException;
import com.oracle.pic.casper.mds.common.exceptions.MdsException;
import com.oracle.pic.casper.mds.common.exceptions.MdsIfMatchException;
import com.oracle.pic.casper.mds.common.exceptions.MdsIfNoneMatchException;
import com.oracle.pic.casper.mds.common.exceptions.MdsNotFoundException;
import com.oracle.pic.casper.mds.common.exceptions.MdsResourceTenantException;
import com.oracle.pic.casper.mds.common.grpc.TimestampUtils;
import com.oracle.pic.casper.mds.loadbalanced.MdsExecutor;
import com.oracle.pic.casper.mds.object.AbortUploadRequest;
import com.oracle.pic.casper.mds.object.ArchiveObjectRequest;
import com.oracle.pic.casper.mds.object.BeginUploadRequest;
import com.oracle.pic.casper.mds.object.DeleteObjectRequest;
import com.oracle.pic.casper.mds.object.DeleteObjectResponse;
import com.oracle.pic.casper.mds.object.FinishUploadResponse;
import com.oracle.pic.casper.mds.object.GetObjectRequest;
import com.oracle.pic.casper.mds.object.GetObjectResponse;
import com.oracle.pic.casper.mds.object.HeadObjectRequest;
import com.oracle.pic.casper.mds.object.HeadObjectResponse;
import com.oracle.pic.casper.mds.object.ListBasicResponse;
import com.oracle.pic.casper.mds.object.ListNameOnlyResponse;
import com.oracle.pic.casper.mds.object.ListObjectsRequest;
import com.oracle.pic.casper.mds.object.ListObjectsResponse;
import com.oracle.pic.casper.mds.object.ListS3Response;
import com.oracle.pic.casper.mds.object.ListUploadedPartsRequest;
import com.oracle.pic.casper.mds.object.ListUploadedPartsResponse;
import com.oracle.pic.casper.mds.object.ListUploadsRequest;
import com.oracle.pic.casper.mds.object.ListUploadsResponse;
import com.oracle.pic.casper.mds.object.ListV1Request;
import com.oracle.pic.casper.mds.object.ListV1Response;
import com.oracle.pic.casper.mds.object.MdsObjectStorageTier;
import com.oracle.pic.casper.mds.object.MdsObjectSummary;
import com.oracle.pic.casper.mds.object.MdsPartEtag;
import com.oracle.pic.casper.mds.object.MdsPartInfo;
import com.oracle.pic.casper.mds.object.MdsUploadInfo;
import com.oracle.pic.casper.mds.object.MdsUploadKey;
import com.oracle.pic.casper.mds.object.ObjectServiceGrpc.ObjectServiceBlockingStub;
import com.oracle.pic.casper.mds.object.RenameObjectRequest;
import com.oracle.pic.casper.mds.object.RestoreObjectRequest;
import com.oracle.pic.casper.mds.object.SetObjectEncryptionKeyRequest;
import com.oracle.pic.casper.mds.object.UpdateObjectMetadataRequest;
import com.oracle.pic.casper.mds.object.UpdateObjectMetadataResponse;
import com.oracle.pic.casper.mds.object.exception.MdsAlreadyArchivedException;
import com.oracle.pic.casper.mds.object.exception.MdsAlreadyRestoredException;
import com.oracle.pic.casper.mds.object.exception.MdsBucketLockedForMigrationException;
import com.oracle.pic.casper.mds.object.exception.MdsBucketLockedMigrationCompleteException;
import com.oracle.pic.casper.mds.object.exception.MdsConcurrentObjectModificationException;
import com.oracle.pic.casper.mds.object.exception.MdsDstIfNoneMatchException;
import com.oracle.pic.casper.mds.object.exception.MdsInvalidUploadIdMarkerException;
import com.oracle.pic.casper.mds.object.exception.MdsInvalidUploadPartException;
import com.oracle.pic.casper.mds.object.exception.MdsNotArchivedObjectException;
import com.oracle.pic.casper.mds.object.exception.MdsObjectOverwriteNotAllowedException;
import com.oracle.pic.casper.mds.object.exception.MdsObjectTooLargeException;
import com.oracle.pic.casper.mds.object.exception.MdsResourceListObjectsException;
import com.oracle.pic.casper.mds.object.exception.MdsRestoreInProgressException;
import com.oracle.pic.casper.mds.object.exception.MdsUploadNotFoundException;
import com.oracle.pic.casper.mds.object.exception.ObjectExceptionClassifier;
import com.oracle.pic.casper.metadata.utils.CryptoUtils;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.ETagType;
import com.oracle.pic.casper.objectmeta.NoSuchUploadException;
import com.oracle.pic.casper.objectmeta.ObjectDb;
import com.oracle.pic.casper.objectmeta.StorageObjectDb;
import com.oracle.pic.casper.objectmeta.UnexpectedEntityTagException;
import com.oracle.pic.casper.webserver.api.auditing.ObjectEvent;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.AuthorizationResponse;
import com.oracle.pic.casper.webserver.api.auth.Authorizer;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.NotAuthenticatedException;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpPathHelpers;
import com.oracle.pic.casper.webserver.api.common.MdsClientHelper;
import com.oracle.pic.casper.webserver.api.common.MdsTransformer;
import com.oracle.pic.casper.webserver.api.common.MetadataUpdater;
import com.oracle.pic.casper.webserver.api.common.RecycleBinHelper;
import com.oracle.pic.casper.webserver.api.eventing.CasperObjectEvent;
import com.oracle.pic.casper.webserver.api.eventing.EventAction;
import com.oracle.pic.casper.webserver.api.eventing.EventPublisher;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.CreateUploadRequest;
import com.oracle.pic.casper.webserver.api.model.FinishUploadRequest;
import com.oracle.pic.casper.webserver.api.model.FinishUploadRequest.PartAndETag;
import com.oracle.pic.casper.webserver.api.model.ListResponse;
import com.oracle.pic.casper.webserver.api.model.ObjectBaseSummary;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.ObjectProperties;
import com.oracle.pic.casper.webserver.api.model.ObjectSummaryCollection;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.model.PartMetadata;
import com.oracle.pic.casper.webserver.api.model.PostObjectMetadataResponse;
import com.oracle.pic.casper.webserver.api.model.ReencryptObjectDetails;
import com.oracle.pic.casper.webserver.api.model.RenameRequest;
import com.oracle.pic.casper.webserver.api.model.RestoreObjectsDetails;
import com.oracle.pic.casper.webserver.api.model.SwiftObjectSummaryCollection;
import com.oracle.pic.casper.webserver.api.model.UploadIdentifier;
import com.oracle.pic.casper.webserver.api.model.UploadMetadata;
import com.oracle.pic.casper.webserver.api.model.UploadPaginatedList;
import com.oracle.pic.casper.webserver.api.model.WSStorageObject;
import com.oracle.pic.casper.webserver.api.model.WSStorageObjectChunk;
import com.oracle.pic.casper.webserver.api.model.WSStorageObjectSummary;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;
import com.oracle.pic.casper.webserver.api.model.exceptions.ConcurrentObjectModificationException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidCompartmentIdException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidObjectNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchCompartmentIdException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchNamespaceException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchObjectException;
import com.oracle.pic.casper.webserver.api.model.exceptions.TooLongObjectNameException;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;
import com.oracle.pic.casper.webserver.api.replication.ReplicationEnforcer;
import com.oracle.pic.casper.webserver.api.s3.S3HttpHelpers;
import com.oracle.pic.casper.webserver.api.s3.model.CopyObjectDetails;
import com.oracle.pic.casper.webserver.api.s3.model.CopyObjectResult;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.server.MdsClients;
import com.oracle.pic.casper.webserver.util.BucketOptionsUtil;
import com.oracle.pic.casper.webserver.util.CommonUtils;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.commons.id.InvalidOCIDException;
import com.oracle.pic.commons.id.OCIDParser;
import com.oracle.pic.identity.authentication.Principal;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.vertx.ext.web.RoutingContext;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static javax.measure.unit.NonSI.BYTE;

/**
 * The backend implementation for the Casper V2 API and the Swift API.
 * <p>
 * This class provides a layer between the data storage services (volume metadata, von generation, storage servers,
 * object metadata and bucket data) and the front end HTTP service implementations (Casper V2 API and Swift API). To
 * prevent details of those layers from leaking into this one, this layer provides its own model and exception classes
 * and it purposefully avoids any knowledge of HTTP concepts. Currently the backend databases use the terms "scope",
 * "namespace" and "object" while the API uses "namespace", "bucket" and "object" (correspondingly). That translation
 * is done here, so things named "namespace" can be confusing.
 * <p>
 * The methods of this class return {@link CompletableFuture} objects, and typically run on background worker threads
 * (in this case, usually on a Vert.x worker thread pool). The methods should not ever throw exceptions, but will
 * return exceptions through the CompletableFutures. If a method of this class throws an exception it is always an
 * indication of a code bug. The documentation for the methods lists known exceptions that can be returned through the
 * CompletableFutures.
 */
public final class Backend {
    private static final Logger LOG = LoggerFactory.getLogger(Backend.class);
    private static final int REENCRYPTION_MAX_CHUNK_LIMIT = 100;
    private static final int OBJECT_NAME_SIZE_LIMIT = 1024;
    private static final List<ObjectProperties> S3_OBJECT_PROPERTIES = ImmutableList.of(
            ObjectProperties.NAME,
            ObjectProperties.MD5,
            ObjectProperties.TIMEMODIFIED,
            ObjectProperties.SIZE,
            ObjectProperties.ARCHIVED);
    private static final List<ObjectProperties> OBJECT_PROPERTIES_BASIC = ImmutableList.of(
            ObjectProperties.NAME,
            ObjectProperties.MD5,
            ObjectProperties.TIMECREATED,
            ObjectProperties.SIZE,
            ObjectProperties.ETAG);
    private static final String SERVICE_UNAVAILABLE_MSG = "The service is currently unavailable.";

    private final Authorizer authorizer;
    private final WebServerConfiguration webServerConf;
    private final JsonSerializer jsonSerializer;
    private final DecidingKeyManagementService kms;

    private final BucketBackend bucketBackend;
    private final ArchiveConfiguration archiveConf;
    private final EventPublisher eventPublisher;
    private final ImmutableSet<String> oboPrincipal = ImmutableSet.of(
            "ocid1.tenancy.integ-next..aaaaaaaa4lseruoctkfw5uy3cbrtcrxm7xfsiwxwaufuwvfqkgupovf3teda",
            "ocid1.tenancy.integ-stable..aaaaaaaa56kx2mt73nuyyufydkk3x37jcmnrpmroxgzor5n6lsbwevqs5lcq",
            "ocid1.tenancy.oc1..aaaaaaaa4elvfqbgjzaknixh3i7mzl5ddapulli3ibng3fgbziqorrvp3wla",
            "ocid1.tenancy.oc1..aaaaaaaaczbt3lmiyuv5mtq77af53wekv4qsesv3xl3effzoozf6kbhfimra",
            "ocid1.tenancy.oc1..aaaaaaaa5uyc6opunr2wsqm3ilq4yiayn2gupn5rikosmwaw3y2pf6eqfzdq",
            "ocid1.tenancy.oc1..aaaaaaaan4z5of2mubf25itggfykcii3xiocx33naljm37rhlvx4h2hm7eoq",
            "ocid1.tenancy.oc1..aaaaaaaajzzeurnp6jfwdmpmakx7cf4ay56nualnzoc7g3zwgihqinirpnua",
            AuthTestConstants.FAKE_TENANT_ID);

    private final ResourceControlledMetadataClient metadataClient;
    private final MdsExecutor<ObjectServiceBlockingStub> objectClient;
    private final long requestDeadlineSeconds;
    private final long listObjectRequestDeadlineSeconds;
    private final int listObjectsMaxMessageBytes;
    private final int getObjectMaxMessageBytes;
    /**
     * If recycle-bin functionality is enabled we only allow OLM to delete
     * objects from the recycle bin. In order to enforce that we need to
     * know the OCID of the OLM Service Principal.
     */
    private final String olmTenantId;

    public static final class BackendHelper {

        private BackendHelper() {
        }

        protected static RuntimeException toRuntimeException(StatusRuntimeException ex) {
            final MdsException mdsException = ObjectExceptionClassifier.fromStatusRuntimeException(ex);
            return toRuntimeException(mdsException);
        }

        public static RuntimeException toRuntimeException(MdsException mdsException) {
            if (mdsException instanceof MdsBucketLockedForMigrationException) {
                return new BucketLockedForMigrationException(mdsException.getMessage());
            } else if (mdsException instanceof MdsBucketLockedMigrationCompleteException) {
                return new BucketLockedMigrationCompleteException(mdsException.getMessage());
            } else if (mdsException instanceof MdsConcurrentObjectModificationException) {
                return new ConcurrentObjectModificationException(mdsException.getMessage(), mdsException);
            } else if (mdsException instanceof MdsDstIfNoneMatchException) {
                return new IfNoneMatchException(mdsException.getMessage(), mdsException);
            } else if (mdsException instanceof MdsIfMatchException) {
                return new IfMatchException(mdsException.getMessage(), mdsException);
            } else if (mdsException instanceof MdsIfNoneMatchException) {
                return new IfNoneMatchException(mdsException.getMessage(), mdsException);
            } else if (mdsException instanceof MdsUploadNotFoundException) {
                return new NoSuchUploadException(mdsException.getMessage());
            } else if (mdsException instanceof MdsInvalidUploadPartException) {
                return new InvalidUploadPartException(mdsException.getMessage(), mdsException);
            } else if (mdsException instanceof MdsInvalidUploadIdMarkerException) {
                return new InvalidUploadIdMarkerException(mdsException.getMessage(), mdsException);
            } else if (mdsException instanceof MdsNotArchivedObjectException) {
                return new NotArchivedObjectException(mdsException.getMessage(), mdsException);
            } else if (mdsException instanceof MdsAlreadyArchivedException) {
                return new AlreadyArchivedObjectException(mdsException.getMessage(), mdsException);
            } else if (mdsException instanceof MdsAlreadyRestoredException) {
                return new AlreadyRestoredObjectException(mdsException.getMessage(), mdsException);
            } else if (mdsException instanceof MdsRestoreInProgressException) {
                return new RestoreInProgressException(mdsException.getMessage(), mdsException);
            } else if (mdsException instanceof MdsObjectTooLargeException) {
                return new ObjectTooLargeException(mdsException.getMessage(), mdsException);
            } else if (mdsException instanceof MdsNotFoundException) {
                return new NoSuchObjectException(mdsException.getMessage());
            } else if (mdsException instanceof MdsResourceTenantException) {
                return new TooBusyException(SERVICE_UNAVAILABLE_MSG, mdsException);
            } else if (mdsException instanceof MdsResourceListObjectsException) {
                return new TooBusyException(SERVICE_UNAVAILABLE_MSG, mdsException);
            } else if (mdsException instanceof MdsDbTooBusyException) {
                return new TooBusyException();
            } else if (mdsException instanceof MdsObjectOverwriteNotAllowedException) {
                return new NoSuchBucketException(BucketBackend.noSuchBucketMsg());
            } else {
                return new InternalServerErrorException(mdsException.getMessage(), mdsException);
            }
        }

        public static <T> T invokeMds(
                MetricsBundle aggregateBundle, MetricsBundle apiBundle, boolean retryable, Callable<T> callable) {
            return MdsClientHelper.invokeMds(aggregateBundle, apiBundle, retryable,
                    callable, BackendHelper::toRuntimeException);
        }
    }

    public Backend(MdsClients mdsClients,
                   Authorizer authorizer,
                   WebServerConfiguration webServerConf,
                   JsonSerializer jsonSerializer,
                   DecidingKeyManagementService kms,
                   BucketBackend bucketBackend,
                   ArchiveConfiguration archiveConf,
                   EventPublisher eventPublisher,
                   ResourceControlledMetadataClient metadataClient,
                   String olmTenantId) {
        this.authorizer = authorizer;
        this.webServerConf = webServerConf;
        this.jsonSerializer = jsonSerializer;
        this.kms = kms;
        this.bucketBackend = bucketBackend;
        this.archiveConf = archiveConf;
        this.eventPublisher = eventPublisher;
        this.metadataClient = metadataClient;
        this.objectClient = mdsClients.getObjectMdsExecutor();
        this.requestDeadlineSeconds = mdsClients.getObjectRequestDeadline().getSeconds();
        this.listObjectRequestDeadlineSeconds = mdsClients.getListObjectRequestDeadline().getSeconds();
        this.listObjectsMaxMessageBytes = mdsClients.getListObjectsMaxMessageBytes();
        this.getObjectMaxMessageBytes = mdsClients.getGetObjectMaxMessageBytes();
        this.olmTenantId = olmTenantId;
    }

    /**
     * @return a ObjectClient with a deadline set and opcRequestId as metadata.
     */
    public static ObjectServiceBlockingStub getClientWithOptions(ObjectServiceBlockingStub stub,
                                                                 long requestDeadlineSeconds,
                                                                 CommonRequestContext context) {
        Metadata metadata = new Metadata();
        metadata.put(MdsRequestId.OPC_REQUEST_ID_KEY, context.getOpcRequestId());
        return MetadataUtils.attachHeaders(
                stub.withDeadlineAfter(requestDeadlineSeconds, TimeUnit.SECONDS),
                metadata);
    }

    /**
     * @return a ObjectClient with a deadline set and opcRequestId as metadata.
     */
    public static ObjectServiceBlockingStub getClientWithOptions(ObjectServiceBlockingStub stub,
                                                                 long requestDeadlineSeconds,
                                                                 String opcRequestId) {
        Metadata metadata = new Metadata();
        metadata.put(MdsRequestId.OPC_REQUEST_ID_KEY, opcRequestId);
        return MetadataUtils.attachHeaders(
                stub.withDeadlineAfter(requestDeadlineSeconds, TimeUnit.SECONDS),
                metadata);
    }


    public static ObjectServiceBlockingStub getClientWithOptions(ObjectServiceBlockingStub stub,
                                                                 long requestDeadlineSeconds,
                                                                 CommonRequestContext context,
                                                                 int msgLimit) {
        Metadata metadata = new Metadata();
        metadata.put(MdsRequestId.OPC_REQUEST_ID_KEY, context.getOpcRequestId());
        return MetadataUtils.attachHeaders(stub.withDeadlineAfter(requestDeadlineSeconds, TimeUnit.SECONDS)
                .withMaxInboundMessageSize(msgLimit), metadata);
    }

    /**
     * Rename an object.
     *
     * @param context       the common context used for logging and metrics.
     * @param authInfo      information about the authenticated user making the request.
     * @param renameRequest is this from a rename request.
     * @return the new object metadata or an exception. A partial list of exceptions that can be returned
     * includes: InvalidBucketNameException, InvalidObjectNameException, InvalidMetadataException,
     * IfNoneMatchException, ConcurrentObjectModificationException and IfMatchException
     */
    public ObjectMetadata renameObject(
            RoutingContext context,
            AuthenticationInfo authInfo,
            String namespace,
            String bucketName,
            RenameRequest renameRequest) {
        Preconditions.checkArgument(authInfo != null, "AuthInfo cannot be null.");
        Preconditions.checkNotNull(renameRequest.getSourceName());
        Preconditions.checkNotNull(renameRequest.getNewName());
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);
        Validator.validateObjectName(renameRequest.getNewName());
        Validator.validateObjectName(renameRequest.getSourceName());
        HttpMatchHelpers.validateIfNoneMatchEtag(renameRequest.getNewObjIfNoneMatchETag());

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("backend:renameObject");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, renameRequest.getNewName());

        final WSTenantBucketInfo bucket = bucketBackend.getBucketMetadataWithCache(
                context, childScope, namespace, bucketName, Api.V2);
        final WSRequestContext wsContext = WSRequestContext.get(context);
        wsContext.setBucketOcid(bucket.getOcid());
        wsContext.setBucketCreator(bucket.getCreationUser());
        wsContext.setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(bucket.getOptions()));

        authorizeOperationForPermissions(childScope, context, authorizer, authInfo, bucket,
                CasperOperation.RENAME_OBJECT,
                CasperPermission.OBJECT_CREATE,
                CasperPermission.OBJECT_OVERWRITE);

        // Blocking rename object into the recycle bin
        // Note that we do want to allow renames out of the recycle bin.
        if (RecycleBinHelper.isBinObject(bucket, renameRequest.getNewName())) {
            throw new AuthorizationException(V2ErrorCode.FORBIDDEN.getStatusCode(),
                    V2ErrorCode.FORBIDDEN.getErrorName(), "Not authorized to create objects in the Recycle Bin");
        }

        ReplicationEnforcer.throwIfReadOnly(context, bucket);

        MdsObjectSummary info = MetricScope.timeScope(childScope, "objectMds:renameObject",
                innerScope -> {
                    try {
                        RenameObjectRequest request = RenameObjectRequest.newBuilder()
                                .setBucketKey(MdsTransformer.toMdsBucketKey(bucket.getKey()))
                                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                .setSrcName(renameRequest.getSourceName())
                                .setDstName(renameRequest.getNewName())
                                .setSrcIfMatchEtag(StringUtils.defaultString(renameRequest.getSrcObjIfMatchETag()))
                                .setDstIfMatchEtag(StringUtils.defaultString(renameRequest.getNewObjIfMatchETag()))
                                .setDstIfNoneMatchEtag(
                                        StringUtils.defaultString(renameRequest.getNewObjIfNoneMatchETag()))
                                .build();
                        return BackendHelper.invokeMds(
                                MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_RENAME_OBJECT,
                                false, () -> objectClient.execute(
                                        c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                                .renameObject(request).getObjectSummary()));
                    } catch (ConcurrentObjectModificationException ex) {
                        throw new ConcurrentObjectModificationException("The object '" + renameRequest.getNewName() +
                                "' in bucket '" + bucketName + "' was concurrently modified" +
                                " while being renamed, so the rename operation has failed.", ex);
                    }
                });
        wsContext.addOldState("objectName", renameRequest.getSourceName());
        wsContext.addNewState("objectName", renameRequest.getNewName());

        final ObjectMetadata objectMetadata = BackendConversions.mdsObjectSummaryToObjectMetadata(info, kms);
        wsContext.setEtag(objectMetadata.getETag());
        ServiceLogsHelper.logServiceEntry(context);
        return objectMetadata;
    }

    /**
     * Merge Metadata of an object.
     *
     * @param context      the common context used for metrics and logging.
     * @param namespace    the namespace.
     * @param authInfo     information about the authenticated user making the request.
     * @param bucketName   the bucket name.
     * @param objectName   the object name.
     * @param expectedETag the expected entity tag for the object, or null to delete any object.
     * @param etagOverride the etag override of the new object, non-null if the request comes from a cross-region
     *                     replication worker
     * @return New ETag of object and last modified date.
     */
    public PostObjectMetadataResponse mergeObjectMetadata(RoutingContext context,
                                                          AuthenticationInfo authInfo,
                                                          String namespace,
                                                          String bucketName,
                                                          String objectName,
                                                          Map<String, String> requestMetadata,
                                                          @Nullable String expectedETag,
                                                          @Nullable String etagOverride) {
        Preconditions.checkNotNull(authInfo, "AuthInfo cannot be null");
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);
        Validator.validateObjectName(objectName);
        Preconditions.checkNotNull(requestMetadata, "Metadata argument cannot be be null");

        Validator.validateMetadata(requestMetadata, jsonSerializer);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("backend:updateObjectMetadata");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, objectName);

        final WSTenantBucketInfo bucket =
                bucketBackend.getBucketMetadataWithCache(context, childScope, namespace, bucketName, Api.V2);
        final WSRequestContext wsContext = WSRequestContext.get(context);
        wsContext.setBucketOcid(bucket.getOcid());
        wsContext.setBucketCreator(bucket.getCreationUser());
        wsContext.setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(bucket.getOptions()));

        authorizeOperationForPermissions(childScope, context, authorizer, authInfo, bucket,
                CasperOperation.MERGE_OBJECT_METADATA, CasperPermission.OBJECT_OVERWRITE);

        // Blocking merge metadata for the recycle bin objects
        if (RecycleBinHelper.isBinObject(bucket, objectName)) {
            throw new AuthorizationException(V2ErrorCode.FORBIDDEN.getStatusCode(),
                    V2ErrorCode.FORBIDDEN.getErrorName(), "Not authorized to perform metadata operations on objects" +
                    " in the Recycle Bin");
        }

        ReplicationEnforcer.throwIfReadOnly(context, bucket);

        // Need to decide correct number for max retries for update Object metadata.
        // need to retry as update object metadata should not fail.
        // similar s3 operation doesnt throw 409.
        // max duration is set to 10 seconds as this operation does 2 db operation under retry
        final RetryPolicy baseRetryPolicy = new RetryPolicy()
                .retryOn(ConcurrentObjectModificationException.class)
                .withBackoff(10, 1000, TimeUnit.MILLISECONDS)
                .withJitter(0.25)
                .withMaxDuration(10000, TimeUnit.MILLISECONDS)
                .withMaxRetries(3);
        final RetryPolicy retryPolicy;
        if (expectedETag == null || expectedETag.equals("*")) {
            // We are going to retry operation on if-match failure for case when user has not specified a
            // specific etag. we will pass the source etag in this scenario so that there is no lossy merge
            // during multiple threads trying to merge different keys.
            // max duration is set to 10 seconds as this operation does 2 db operation under retry
            retryPolicy = new RetryPolicy()
                    .retryOn(ConcurrentObjectModificationException.class, IfMatchException.class)
                    .withBackoff(10, 1000, TimeUnit.MILLISECONDS)
                    .withJitter(0.25)
                    .withMaxDuration(10000, TimeUnit.MILLISECONDS)
                    .withMaxRetries(3);
        } else {
            retryPolicy = baseRetryPolicy;
        }

        final String etag = etagOverride == null ? CommonUtils.generateETag() : etagOverride;
        final Instant timeUpdated = Failsafe.with(retryPolicy).get(() -> {
            final Optional<WSStorageObject> storageObject = getObject(context, bucket, objectName);
            if (!storageObject.isPresent()) {
                throw new ObjectNotFoundException("Object not found");
            }
            final WSStorageObject wsStorageObject = storageObject.get();
            final String sourceETag;
            final Map<String, String> newMetadata;
            final Map<String, String> oldMetadata = wsStorageObject.getMetadata(kms);

            // We are going to retry operation on if-match failure for case when user has not specified a
            // specific etag. we will pass the source etag in this scenario so that there is no lossy merge
            // during multiple threads trying to merge different keys.
            if (expectedETag == null || expectedETag.equals("*")) {
                sourceETag = storageObject.get().getETag();
            } else {
                sourceETag = expectedETag;
            }
            // merge metadata with current metadata and revalidate limits.
            newMetadata = MetadataUpdater.updateMetadata(false, oldMetadata, requestMetadata);
            Validator.validateMetadata(newMetadata, jsonSerializer);
            wsContext.addOldState("metadata", oldMetadata);
            wsContext.addNewState("metadata", newMetadata);
            final EncryptionKey encryptionKey = wsStorageObject.getEncryptedEncryptionKey().getEncryptionKey(kms);
            final String encryptedMetadata =
                    CryptoUtils.encryptMetadata(newMetadata, encryptionKey);
            // Add validator call.
            return MetricScope.timeScope(childScope, "objectMds:updateObjectMetadata", innerScope -> {
                final UpdateObjectMetadataRequest request = UpdateObjectMetadataRequest.newBuilder()
                        .setBucketToken(bucket.getMdsBucketToken().toByteString())
                        .setObjectKey(objectKey(objectName,
                                namespace,
                                bucketName,
                                bucket.getKey().getApi()))
                        .setIfMatchEtag(StringUtils.defaultString(sourceETag))
                        .setNewEtag(etag)
                        .setNewMetadata(encryptedMetadata)
                        .build();
                try {
                    UpdateObjectMetadataResponse response = BackendHelper.invokeMds(
                            MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_UPDATE_OBJECT_METADATA,
                            false, () -> objectClient.execute(
                                    c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                            .updateObjectMetadata(request)));
                    ServiceLogsHelper.logServiceEntry(context);
                    return TimestampUtils.toInstant(response.getUpdateTime());
                } catch (IfMatchException ex) {
                    // if user never specified ifMatch then we must have passed it from our internal retry logic
                    // and received error
                    if (expectedETag == null || expectedETag.equals("*")) {
                        throw new ConcurrentObjectModificationException(
                                "The object '" + objectName + "' in bucket '" + bucketName +
                                        "' was concurrently modified while being updated," +
                                        " so the update operation has failed.", ex);
                    } else {
                        throw new UnexpectedEntityTagException("The entity tags do not match");
                    }
                } catch (ConcurrentObjectModificationException ex) {
                    throw new ConcurrentObjectModificationException(
                            "The object '" + objectName + "' in bucket '" + bucketName +
                                    "' was concurrently modified while being updated," +
                                    " so the update operation has failed.", ex);
                } catch (ObjectNotFoundException ex) {
                    throw new ObjectNotFoundException(
                            "The object '" + objectName + "' in bucket '" + bucketName +
                                    " is not found, so the update operation has failed.", ex);
                }
            });
        });
        return new PostObjectMetadataResponse(etag, Date.from(timeUpdated));
    }

    /**
     * Replace Metadata of an object.
     *
     * @param context      the common context used for metrics and logging.
     * @param namespace    the namespace.
     * @param authInfo     information about the authenticated user making the request.
     * @param bucketName   the bucket name.
     * @param objectName   the object name.
     * @param expectedETag the expected entity tag for the object, or null to delete any object.
     * @param etagOverride the etag override of the new object, non-null if the request comes from a cross-region
     *                     replication worker
     * @return New ETag of object and last modified date.
     */
    public PostObjectMetadataResponse replaceObjectMetadata(RoutingContext context,
                                                            AuthenticationInfo authInfo,
                                                            String namespace,
                                                            String bucketName,
                                                            String objectName,
                                                            Map<String, String> requestMetadata,
                                                            @Nullable String expectedETag,
                                                            @Nullable String etagOverride) {
        Preconditions.checkNotNull(authInfo, "AuthInfo cannot be null");
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);
        Validator.validateObjectName(objectName);
        Preconditions.checkNotNull(requestMetadata, "Metadata argument cannot be be null");

        Validator.validateMetadata(requestMetadata, jsonSerializer);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("backend:updateObjectMetadata");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, objectName);

        final WSTenantBucketInfo bucket =
                bucketBackend.getBucketMetadataWithCache(context, childScope, namespace, bucketName, Api.V2);
        final WSRequestContext wsContext = WSRequestContext.get(context);
        wsContext.setBucketOcid(bucket.getOcid());
        wsContext.setBucketCreator(bucket.getCreationUser());
        wsContext.setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(bucket.getOptions()));

        authorizeOperationForPermissions(childScope, context, authorizer, authInfo, bucket,
                CasperOperation.REPLACE_OBJECT_METADATA, CasperPermission.OBJECT_OVERWRITE);

        // Blocking replace metadata for the recycle bin objects
        if (RecycleBinHelper.isBinObject(bucket, objectName)) {
            throw new AuthorizationException(V2ErrorCode.FORBIDDEN.getStatusCode(),
                    V2ErrorCode.FORBIDDEN.getErrorName(), "Not authorized to perform metadata operations on objects" +
                    " in the Recycle Bin");
        }

        ReplicationEnforcer.throwIfReadOnlyButNotReplicationScope(context, bucket);

        // Need to decide correct number for max retries for update Object metadata.
        // need to retry as update object metadata should not fail.
        // similar s3 operation doesnt throw 409.
        // max duration is set to 10 seconds as this operation does 2 db operation under retry
        final RetryPolicy retryPolicy = new RetryPolicy()
                .retryOn(ConcurrentObjectModificationException.class)
                .withBackoff(10, 1000, TimeUnit.MILLISECONDS)
                .withJitter(0.25)
                .withMaxDuration(10000, TimeUnit.MILLISECONDS)
                .withMaxRetries(3);

        final String etag = etagOverride == null ? CommonUtils.generateETag() : etagOverride;
        final Instant timeUpdated = Failsafe.with(retryPolicy).get(() -> {
                    Optional<WSStorageObject> storageObject = getObject(context, bucket, objectName);
                    if (!storageObject.isPresent()) {
                        throw new ObjectNotFoundException("Object not found");
                    }
                    final WSStorageObject wsStorageObject = storageObject.get();
                    final String sourceETag;
                    final Map<String, String> newMetadata;
                    final Map<String, String> contentMetadata;
                    sourceETag = expectedETag;
                    contentMetadata = wsStorageObject.getMetadata(kms).entrySet().stream().filter(
                            entry -> HttpHeaderHelpers.PRESERVED_CONTENT_HEADERS_LOOKUP.containsKey(entry.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    newMetadata = MetadataUpdater.updateMetadata(false, contentMetadata, requestMetadata);
                    Validator.validateMetadata(newMetadata, jsonSerializer);
                    wsContext.addOldState("metadata", wsStorageObject.getMetadata(kms));
                    wsContext.addNewState("metadata", newMetadata);
                    wsContext.setEtag(wsStorageObject.getETag());
                    final EncryptionKey encryptionKey =
                            wsStorageObject.getEncryptedEncryptionKey().getEncryptionKey(kms);
                    final String encryptedMetadata = CryptoUtils.encryptMetadata(newMetadata, encryptionKey);
                    return MetricScope.timeScope(childScope, "objectMds:updateObjectMetadata",
                            innerScope -> {
                        final UpdateObjectMetadataRequest request = UpdateObjectMetadataRequest.newBuilder()
                                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                .setObjectKey(objectKey(objectName, namespace, bucketName, bucket.getKey().getApi()))
                                .setIfMatchEtag(StringUtils.defaultString(sourceETag))
                                .setNewEtag(etag)
                                .setNewMetadata(encryptedMetadata)
                                .build();
                        try {
                            UpdateObjectMetadataResponse response =  BackendHelper.invokeMds(
                                    MdsMetrics.OBJECT_MDS_BUNDLE,
                                    MdsMetrics.OBJECT_MDS_UPDATE_OBJECT_METADATA,
                                    false,
                                    () -> objectClient.execute(
                                            c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                                    .updateObjectMetadata(request)));
                            ServiceLogsHelper.logServiceEntry(context);
                            return TimestampUtils.toInstant(response.getUpdateTime());
                        } catch (IfMatchException ex) {
                            throw new UnexpectedEntityTagException("The entity tags do not match");
                        } catch (ConcurrentObjectModificationException ex) {
                            throw new ConcurrentObjectModificationException(
                                    "The object '" + objectName + "' in bucket '" + bucketName +
                                            "' was concurrently modified while being updated," +
                                            " so the update operation has failed.", ex);
                        } catch (ObjectNotFoundException ex) {
                            throw new ObjectNotFoundException(
                                    "The object '" + objectName + "' in bucket '" + bucketName +
                                            " is not found, so the update operation has failed.", ex);
                        }
                    });
        });
        return new PostObjectMetadataResponse(etag, Date.from(timeUpdated));
    }

    /**
     * Restore an object.
     */
    public void restoreObject(RoutingContext context,
                              AuthenticationInfo authInfo,
                              String namespaceName, String bucketName, RestoreObjectsDetails restoreObjectsDetails) {
        Validator.validateV2Namespace(namespaceName);
        Validator.validateBucket(bucketName);
        Validator.validateObjectName(restoreObjectsDetails.getObjectName());

        final Duration timeToArchive = restoreObjectsDetails.getTimeToArchive();
        final Duration timeToArchiveMin = RestoreObjectsDetails.getTimeToArchiveMin();
        final Duration timeToArchiveMax = RestoreObjectsDetails.getTimeToArchiveMax();

        if (timeToArchive != null) {
            Preconditions.checkArgument(((timeToArchive.compareTo(timeToArchiveMin) >= 0) &&
                    (timeToArchive.compareTo(timeToArchiveMax) <= 0)), "Hours value should range between "
                    + timeToArchiveMin.toHours() + " and " + timeToArchiveMax.toHours() + " hours");
        }

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope scope = rootScope.child("Backend:restoreObjects");
        final String objectName = restoreObjectsDetails.getObjectName();
        WSTenantBucketInfo bucket =
                bucketBackend.getBucketMetadataWithCache(context, scope, namespaceName, bucketName, Api.V2);
        authorizeOperationForPermissions(scope, context, authorizer, authInfo, bucket,
                CasperOperation.RESTORE_OBJECT,
                CasperPermission.OBJECT_RESTORE);
        final WSRequestContext wsContext = WSRequestContext.get(context);
        wsContext.setBucketName(bucket.getBucketName());
        wsContext.setBucketCreator(bucket.getCreationUser());
        wsContext.setBucketLoggingStatus(BucketOptionsUtil.getBucketLoggingStatusFromOptions(bucket.getOptions()));

        HttpPathHelpers.logPathParameters(rootScope, namespaceName, bucketName, objectName);

        final Duration durationForRestore;
        final Duration reArchivePeriod;

        if (timeToArchive != null) {
            reArchivePeriod = timeToArchive;
        } else {
            reArchivePeriod = archiveConf.getReArchivePeriod();
        }
        durationForRestore = archiveConf.getDurationForRestore();

        BackendHelper.invokeMds(MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_RESTORE_OBJECT,
                false, () -> objectClient.execute(
                        c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                .restoreObject(RestoreObjectRequest.newBuilder()
                                        .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                        .setObjectKey(objectKey(objectName, namespaceName, bucketName,
                                                bucket.getKey().getApi()))
                                        .setTimeToArchive(TimestampUtils.toProtoDuration(reArchivePeriod))
                                        .setTimeToRestore(TimestampUtils.toProtoDuration(durationForRestore))
                                        .build())));
        wsContext.addOldState("archivalState", ArchivalState.Archived.getState());
        wsContext.addNewState("archivalState", ArchivalState.Restoring.getState());
        ServiceLogsHelper.logServiceEntry(context);
    }

    /**
     * Archive an object.
     */
    public void archiveObject(RoutingContext context,
                              String namespaceName,
                              String bucketName,
                              String objectName,
                              @Nullable String ifMatch) {
        Validator.validateV2Namespace(namespaceName);
        Validator.validateBucket(bucketName);
        Validator.validateObjectName(objectName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope scope = rootScope.child("Backend:restoreObjects");
        WSTenantBucketInfo bucket =
                bucketBackend.getBucketMetadataWithCache(context, scope, namespaceName, bucketName, Api.V2);

        final WSRequestContext wsContext = WSRequestContext.get(context);
        wsContext.setBucketOcid(bucket.getOcid());
        wsContext.setCompartmentID(bucket.getCompartment());
        wsContext.setBucketLoggingStatus(BucketOptionsUtil.getBucketLoggingStatusFromOptions(bucket.getOptions()));

        /*
         * if it's archival bucket, objects are by default archived, and restored object will be
         * automatically re-archived after timeout
         */
        final boolean isArchivalBucket = bucket.getStorageTier() == BucketStorageTier.Archive;
        if (isArchivalBucket) {
            throw new AlreadyArchivedObjectException();
        }

        HttpPathHelpers.logPathParameters(rootScope, namespaceName, bucketName, objectName);
        final Duration minimumRetentionPeriod = archiveConf.getMinimumRetentionPeriodForArchives();
        final ArchiveObjectRequest.Builder builder = ArchiveObjectRequest.newBuilder()
                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                .setObjectKey(objectKey(objectName, namespaceName, bucketName,
                        bucket.getKey().getApi()))
                .setIfMatchEtag(StringUtils.defaultString(ifMatch));
        if (minimumRetentionPeriod != null) {
            builder.setMinRetentionPeriod(TimestampUtils.toProtoDuration(minimumRetentionPeriod));
        }

        BackendHelper.invokeMds(MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_ARCHIVE_OBJECT,
            false, () -> objectClient.execute(c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                        .archiveObject(builder.build())));
        wsContext.addOldState("archivalState", ArchivalState.Available.getState());
        wsContext.addNewState("archivalState", ArchivalState.Archived.getState());

        ServiceLogsHelper.logServiceEntry(context);
    }


    /**
     * Delete a V1 object.
     */
    @Deprecated
    public Optional<Date> deleteV1Object(RoutingContext context,
                                         String namespace,
                                         String bucketName,
                                         String objectName,
                                         @Nullable String expectedETag) {
        Validator.validateObjectName(objectName);
        Validator.validateBucket(bucketName);

        return deleteObject(context,
                AuthenticationInfo.V1_USER,
                bucket -> authorizer.authorize(
                        WSRequestContext.get(context),
                        AuthenticationInfo.V1_USER,
                        bucket.getNamespaceKey(),
                        bucket.getBucketName(),
                        bucket.getCompartment(),
                        bucket.getPublicAccessType(),
                        CasperOperation.DELETE_OBJECT,
                        bucket.getKmsKeyId().orElse(null),
                        true,
                        bucket.isTenancyDeleted(),
                        CasperPermission.OBJECT_DELETE).isPresent(),
                namespace,
                bucketName,
                objectName,
                expectedETag,
                Api.V1);
    }

    /**
     * Delete a V2 object.
     *
     * @param context      the common context used for metrics and logging.
     * @param namespace    the namespace.
     * @param authInfo     information about the authenticated user making the request.
     * @param bucketName   the bucket name.
     * @param objectName   the object name.
     * @param expectedETag the expected entity tag for the object, or null to delete any object.
     * @return a CompletableFuture that contains either an optional date or an exception. If the optional is empty then
     * the requested object could not be found, otherwise the optional contains the date at which the object was
     * deleted. A partial list of exceptions that can be returned includes: InvalidBucketNameException,
     * InvalidObjectNameException, UnexpectedEntityTagException and StaleTransactionException.
     */
    public Optional<Date> deleteV2Object(RoutingContext context,
                                         AuthenticationInfo authInfo,
                                         String namespace,
                                         String bucketName,
                                         String objectName,
                                         @Nullable String expectedETag) {
        Preconditions.checkNotNull(authInfo, "AuthInfo cannot be null");
        Validator.validateV2Namespace(namespace);
        Validator.validateObjectName(objectName);
        Validator.validateBucket(bucketName);

        return deleteObject(context,
                authInfo,
                bucket -> authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        bucket.getNamespaceKey(),
                        bucket.getBucketName(),
                        bucket.getCompartment(),
                        bucket.getPublicAccessType(),
                        CasperOperation.DELETE_OBJECT,
                        bucket.getKmsKeyId().orElse(null),
                        true,
                        bucket.isTenancyDeleted(),
                        CasperPermission.OBJECT_DELETE).isPresent(),
                namespace,
                bucketName,
                objectName,
                expectedETag,
                Api.V2);
    }

    /**
     * Delete an internal PAR object, bypassing authorization.
     */
    public Optional<Date> deleteParObject(RoutingContext context,
                                          AuthenticationInfo authInfo,
                                          String namespace,
                                          String bucketName,
                                          String objectName) {
        Validator.validateV2Namespace(namespace);
        Validator.validateParObjectName(objectName);
        Validator.validateInternalBucket(bucketName, ParUtil.PAR_BUCKET_NAME_SUFFIX);
        return deleteObject(context, authInfo, bucket -> true, namespace, bucketName, objectName, null, Api.V2);
    }

    private Optional<Date> deleteObject(RoutingContext context,
                                        AuthenticationInfo authInfo,
                                        Function<WSTenantBucketInfo, Boolean> authzCB,
                                        String namespace,
                                        String bucketName,
                                        String objectName,
                                        @Nullable String expectedETag,
                                        Api api) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("backend:deleteObject");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, objectName);

        final WSTenantBucketInfo bucket =
            bucketBackend.getBucketMetadataWithCache(context, childScope, namespace, bucketName, api);

        // Put objectLevelAuditMode in the context to be retrieved by AuditFilterHandler
        if (!BucketBackend.InternalBucketType.isInternalBucket(bucketName)) {
            WSRequestContext.get(context).setObjectLevelAuditMode(bucket.getObjectLevelAuditMode());
        }

        WSRequestContext.get(context).setBucketOcid(bucket.getOcid());
        WSRequestContext.get(context).setBucketCreator(bucket.getCreationUser());
        WSRequestContext.get(context).setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(bucket.getOptions()));

        authorizeOperationForPermissionsInternal(childScope, casperPermissions -> authzCB.apply(bucket),
                bucket, CasperOperation.DELETE_OBJECT);

        if (!isOLMRequest(authInfo)) {
            // Blocking customer deletion of recycle bin objects. Such deletion is allowed only by OLM.
            if (RecycleBinHelper.isBinObject(bucket, objectName)) {
                // Return FORBIDDEN (403) as S3ErrorCode does not define 401 status code
                throw new AuthorizationException(V2ErrorCode.FORBIDDEN.getStatusCode(),
                        V2ErrorCode.FORBIDDEN.getErrorName(), "Not authorized to delete objects from the Recycle Bin");
            }

            ReplicationEnforcer.throwIfReadOnlyButNotReplicationScope(context, bucket);
        }

        try {
            Optional<Date> result;
            // Deleting objects into recycle bin is not supported for V1 API. Supported only for V2, S3 and Swift APIs
            if (!api.equals(Api.V1) && RecycleBinHelper.isEnabled(bucket) &&
                    !RecycleBinHelper.isBinObject(bucket, objectName)) {
                // Move the object to recycle bin by renaming the object
                final String newObjectName = RecycleBinHelper.constructName(objectName);
                result = MetricScope.timeScope(childScope, "storageObjectDb:renameForRecycleBin",
                        innerScope -> {
                            try {
                                final RenameObjectRequest request = RenameObjectRequest.newBuilder()
                                        .setBucketKey(MdsTransformer.toMdsBucketKey(bucket.getKey()))
                                        .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                        .setSrcName(objectName)
                                        .setDstName(newObjectName)
                                        .setSrcIfMatchEtag(StringUtils.defaultString(expectedETag))
                                        .setDstIfMatchEtag("")
                                        // Set IfNoneMatchEtag to * as the destination object is not supposed to exist
                                        .setDstIfNoneMatchEtag("*")
                                        .build();
                                BackendHelper.invokeMds(
                                        MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_RENAME_OBJECT,
                                        false, () -> objectClient.execute(
                                                c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                                        .renameObject(request)));
                                // {@link deleteObject} is designed to return the deletion time and hence return time
                                return Optional.of(TimestampUtils.toDate(TimestampUtils.getNow()));
                            } catch (NoSuchObjectException e) {
                                return Optional.empty();
                            }
                });
            } else {
                result = MetricScope.timeScope(childScope, "storageObjectDb:deleteObject",
                        (Function<MetricScope, Optional<Date>>) (innerScope) ->
                                mdsDeleteObject(context, bucket, objectName, expectedETag));

            }
            WSRequestContext.get(context).setEtag(expectedETag);
            WSRequestContext.get(context).setObjectEventsEnabled(bucket.isObjectEventsEnabled());
            eventPublisher.addEvent(new CasperObjectEvent(kms,
                    bucket,
                    objectName,
                    expectedETag,
                    ArchivalState.NotApplicable,
                    EventAction.DELETE),
                authInfo.getMainPrincipal().getTenantId(),
                bucket.getObjectLevelAuditMode());
            WSRequestContext.get(context).addObjectEvent(new ObjectEvent(ObjectEvent.EventType.DELETE_OBJECT,
                objectName, expectedETag, null));
            //Log service log only if the object was successfully deleted.
            result.ifPresent((r) -> ServiceLogsHelper.logServiceEntry(context));

            return result;
        } catch (IfMatchException ex) {
            throw new UnexpectedEntityTagException("The entity tags do not match");
        } catch (ConcurrentObjectModificationException ex) {
            throw new ConcurrentObjectModificationException(
                    "The object '" + objectName + "' in bucket '" + bucketName +
                            "' was concurrently modified while being deleted, so the delete operation has failed.", ex);
        }
    }

    private boolean isOLMRequest(AuthenticationInfo authInfo) {
        return authInfo.getServicePrincipal().isPresent() &&
                olmTenantId.equals(authInfo.getServicePrincipal().get().getTenantId());
    }

    protected static List<WSStorageObjectSummary> listObjectsFromPrefix(
            RoutingContext context,
            MdsExecutor<ObjectServiceBlockingStub> objectClient,
            long requestDeadlineSeconds,
            WSTenantBucketInfo bucket,
            int limit,
            String prefix,
            int msgSizeLimit) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        ListObjectsRequest request = ListObjectsRequest.newBuilder()
                .setBucketKey(bucketKey(bucket.getNamespaceKey().getName(), bucket.getBucketName(),
                        bucket.getNamespaceKey().getApi()))
                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                .setPageSize(limit)
                .setPrefix(StringUtils.defaultString(prefix))
                .build();

        final ListObjectsResponse response = BackendHelper.invokeMds(MdsMetrics.OBJECT_MDS_BUNDLE,
                MdsMetrics.OBJECT_MDS_LIST_OBJECTS,
                true,
                () -> objectClient.execute(c -> getClientWithOptions(c,
                        requestDeadlineSeconds,
                        commonContext,
                        msgSizeLimit).listObjects(request)));
        final ImmutableList.Builder<WSStorageObjectSummary> builder = ImmutableList.builder();
        response.getObjectSummariesList().forEach((summary) -> builder.add(
                BackendConversions.mdsObjectSummaryToWSStorageObjectSummary(summary)));
        return builder.build();
    }

    protected List<WSStorageObjectSummary> listObjectsFromPrefix(RoutingContext context,
                                                                 WSTenantBucketInfo bucket,
                                                                 int limit,
                                                                 String prefix) {
        return listObjectsFromPrefix(
                context,
                objectClient,
                listObjectRequestDeadlineSeconds,
                bucket,
                limit,
                prefix,
                listObjectsMaxMessageBytes);
    }

    /**
     * List the objects in a bucket.
     *
     * @param properties requested properties of the storage object that should be in the view.
     * @param limit      Maximum size of the returned list.
     * @param context    request context.
     * @param namespace  namespace to search.
     * @param bucketName bucket to search.
     * @param delimiter  delimiter to use to simulate hierarchical directory names on the object.
     * @param prefix     names of the objects shall match this prefix.
     * @param startWith  return objects with name greater than this name.
     * @param endBefore  return objects with name less than this name.
     * @return a CompletableFuture that contains an object summary collection or an exception. A partial list of
     * exceptions that can be returned includes: InvalidBucketNameException and InvalidObjectNameException.
     */
    public ObjectSummaryCollection listObjects(RoutingContext context,
                                               AuthenticationInfo authInfo,
                                               ImmutableList<ObjectProperties> properties,
                                               int limit,
                                               String namespace,
                                               String bucketName,
                                               @Nullable Character delimiter,
                                               @Nullable String prefix,
                                               @Nullable String startWith,
                                               @Nullable String endBefore) {
        Preconditions.checkNotNull(authInfo);
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);

        if (prefix != null && Validator.isUtf8EncodingTooLong(prefix, Validator.MAX_ENCODED_STRING_LENGTH)) {
            throw new IllegalArgumentException("Object prefix is too long");
        }

        if (startWith != null && Validator.isUtf8EncodingTooLong(startWith, Validator.MAX_ENCODED_STRING_LENGTH)) {
            throw new IllegalArgumentException("Object key is too long");
        }

        return listObjectsHelper(context,
                bucket -> authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        bucket.getNamespaceKey(),
                        bucket.getBucketName(),
                        bucket.getCompartment(),
                        bucket.getPublicAccessType(),
                        CasperOperation.LIST_OBJECTS,
                        bucket.getKmsKeyId().orElse(null),
                        true,
                        bucket.isTenancyDeleted(),
                        CasperPermission.OBJECT_INSPECT).isPresent(),
                properties,
                limit,
                namespace,
                bucketName,
                delimiter,
                prefix,
                startWith,
                endBefore).getFirst();
    }

    /**
     * List the internal PAR objects in a PAR bucket.
     */
    public ObjectSummaryCollection listParObjects(RoutingContext context,
                                                  ImmutableList<ObjectProperties> properties,
                                                  int limit,
                                                  String namespace,
                                                  String bucketName,
                                                  @Nullable Character delimiter,
                                                  @Nullable String prefix,
                                                  @Nullable String startWith,
                                                  @Nullable String endBefore) {
        Validator.validateV2Namespace(namespace);
        Validator.validateInternalBucket(bucketName, ParUtil.PAR_BUCKET_NAME_SUFFIX);

        if (prefix != null) {
            Validator.validateObjectNamePrefixForPar(prefix);
        }

        if (startWith != null) {
            Validator.validateParObjectName(startWith);
        }

        return listObjectsHelper(context, bucket -> true, properties, limit, namespace, bucketName, delimiter, prefix,
                startWith, endBefore).getFirst();
    }

    /**
     * List objects for V1 only
     */
    @Deprecated
    public PaginatedList<String> listObjectsV1(RoutingContext context, String namespace, String bucketName,
                                               int limit, @Nullable String cursor) {
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(bucketName);
        Validator.validateBucket(bucketName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("backend:listObjectsV1")
                .annotate("limit", limit)
                .annotate("cursor", cursor);
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);

        final WSTenantBucketInfo bucket =
            bucketBackend.getBucketMetadataWithCache(context, childScope, namespace, bucketName, Api.V1);
        authorizeOperationForPermissions(childScope, context, authorizer, AuthenticationInfo.V1_USER, bucket,
                CasperOperation.LIST_OBJECTS,
                CasperPermission.OBJECT_INSPECT);

        return MetricScope.timeScope(childScope, "objectMds:listObjects",
                (Function<MetricScope, PaginatedList>) (innerScope) -> {
                    ListV1Request request = ListV1Request.newBuilder()
                            .setBucketKey(bucketKey(bucket.getNamespaceKey().getName(), bucket.getBucketName(),
                                    bucket.getNamespaceKey().getApi()))
                            .setBucketToken(bucket.getMdsBucketToken().toByteString())
                            .setPageSize(limit)
                            .setStartAfter(StringUtils.defaultString(cursor))
                            .build();
                    ListV1Response response = BackendHelper.invokeMds(
                            MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_LIST_V1_OBJECTS,
                            true, () -> objectClient.execute(
                                    c -> getClientWithOptions(c, listObjectRequestDeadlineSeconds, commonContext)
                                            .listV1Objects(request)));
                    return new PaginatedList<>(
                            response.getObjectKeyList().stream().map(MdsObjectKey::getObjectName)
                                    .collect(Collectors.toList()),
                            response.getTruncated());
                });
    }

    private Pair<ObjectSummaryCollection, WSTenantBucketInfo> listObjectsHelper(
            RoutingContext context,
            Function<WSTenantBucketInfo, Boolean> authZCB,
            List<ObjectProperties> properties,
            int limit,
            String namespace,
            String bucketName,
            @Nullable Character delimiter,
            @Nullable String prefix,
            @Nullable String startWith,
            @Nullable String endBefore) {
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(bucketName);

        if (endBefore != null) {
            Validator.validateObjectName(endBefore);
        }

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final String propertiesString = properties.stream()
                .map(ObjectProperties::toString)
                .collect(Collectors.joining(", "));
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("backend:listObjects")
                .annotate("properties", propertiesString)
                .annotate("limit", limit)
                .annotate("delimiter", delimiter)
                .annotate("prefix", prefix)
                .annotate("startWith", startWith)
                .annotate("endBefore", endBefore);
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, null);

        final WSTenantBucketInfo bucket =
            bucketBackend.getBucketMetadataWithCache(context, childScope, namespace, bucketName, Api.V2);

        // Put objectLevelAuditMode in the context to be retrieved by AuditFilterHandler
        if (!BucketBackend.InternalBucketType.isInternalBucket(bucketName)) {
            WSRequestContext.get(context).setObjectLevelAuditMode(bucket.getObjectLevelAuditMode());
        }

        WSRequestContext.get(context).setBucketOcid(bucket.getOcid());
        WSRequestContext.get(context).setBucketCreator(bucket.getCreationUser());
        WSRequestContext.get(context).setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(bucket.getOptions()));

        authorizeOperationForPermissionsInternal(childScope,
                casperPermissions -> authZCB.apply(bucket),
                bucket,
                CasperOperation.LIST_OBJECTS);
        Pair<ObjectSummaryCollection, WSTenantBucketInfo> result;
        if (delimiter == null) {
            result = new Pair<>(listObjectsHelperWithoutDelimiter(context, properties, limit, childScope,
                    bucket, prefix, startWith, endBefore), bucket);
        } else {
            result = new Pair<>(listObjectsHelperWithDelimiter(context, properties, limit, delimiter, childScope,
                    bucket, prefix, startWith, endBefore), bucket);
        }
        ServiceLogsHelper.logServiceEntry(context);
        return result;
    }

    private ObjectSummaryCollection listObjectsHelperWithDelimiter(RoutingContext context,
                                                                   List<ObjectProperties> properties,
                                                                   int limit,
                                                                   Character delimiter,
                                                                   MetricScope childScope,
                                                                   WSTenantBucketInfo bucket,
                                                                   @Nullable String prefix,
                                                                   @Nullable String startWith,
                                                                   @Nullable String endBefore) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        return MetricScope.timeScope(childScope, "objectMds:listObjects", innerScope -> {
            ListResponse<? extends ObjectBaseSummary> listResponse = makeListCall(commonContext,
                    properties,
                    false,
                    1000,
                    bucket,
                    prefix,
                    startWith,
                    endBefore);

            final Iterable<ObjectSummary> objectSummaries;
            Iterable<String> prefixes;
            final List<? extends ObjectBaseSummary> objects = listResponse.getObjects();

            final ImmutableList.Builder<ObjectSummary> builder = new ImmutableList.Builder<>();
            final ImmutableSortedSet.Builder<String> prefixBuilder =
                    new ImmutableSortedSet.Builder<>(Ordering.natural());

            int prefixLength = prefix == null ? 0 : prefix.length();
            // Using Set as helper for recording the identical key name/prefix got from DB result.
            final HashSet<String> keySet = new HashSet<>();
            ObjectSummary next = null;
            String lastObject = null;
            String secondLastObject = null;

            for (ObjectBaseSummary object : objects) {
                final String name = object.getObjectName();
                final int index = (name.length() <= prefixLength) ?
                        -1 : name.indexOf(delimiter, prefixLength);

                // delimiter not present after prefix
                if (index == -1) {
                    builder.add(object.makeSummary(properties, kms));
                    keySet.add(name);
                } else {
                    if (index == name.length()) {
                        prefixBuilder.add(name);
                        keySet.add(name);
                    } else {
                        final String curPrefix = name.substring(0, index + 1);
                        // curPrefix will only be added to the result set if it is greater than startWith
                        // For eg. startWith = abc/def, prefix abc will not be added.
                        if (startWith == null || curPrefix.compareTo(startWith) >= 0) {
                            prefixBuilder.add(curPrefix);
                            keySet.add(name.substring(0, index + 1));
                        }
                    }
                }
                secondLastObject = lastObject;
                lastObject = name;
                if (keySet.size() > limit) {
                    next = object.makeSummary(properties, kms);
                    break;
                }
            }

            objectSummaries = builder.build();
            prefixes = prefixBuilder.build();
            if (next == null && listResponse.getNextObject().isPresent()) {
                next = listResponse.getNextObject().get().makeSummary(properties, kms);
            }

            return new ObjectSummaryCollection(objectSummaries, bucket.getBucketName(), prefixes,
                    next, secondLastObject, lastObject);
        });
    }

    private ObjectSummaryCollection listObjectsHelperWithoutDelimiter(RoutingContext context,
                                                                      List<ObjectProperties> properties,
                                                                      int limit,
                                                                      MetricScope childScope,
                                                                      WSTenantBucketInfo bucket,
                                                                      @Nullable String prefix,
                                                                      @Nullable String startWith,
                                                                      @Nullable String endBefore) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        return MetricScope.timeScope(childScope, "objectMds:listObjects", innerScope -> {
            ListResponse<? extends ObjectBaseSummary> listResponse = makeListCall(commonContext,
                    properties,
                    true,
                    limit,
                    bucket,
                    prefix,
                    startWith,
                    endBefore);

            final Iterable<ObjectSummary> objectSummaries;
            Iterable<String> prefixes = null;

            final List<? extends ObjectBaseSummary> objects = listResponse.getObjects();
            ImmutableList.Builder<ObjectSummary> builder = new ImmutableList.Builder<>();

            objects.forEach(object -> {
                builder.add(object.makeSummary(properties, kms));
            });
            objectSummaries = builder.build();

            ObjectSummary next = null;
            if (listResponse.getNextObject().isPresent()) {
                next = listResponse.getNextObject().get().makeSummary(properties, kms);
            }

            int objectsSize = objects.size();
            String lastObject = objectsSize < 1 ?
                    null : objects.get(objects.size() - 1).getObjectName();
            String secondLastObject = objectsSize < 2 ?
                    null : objects.get(objects.size() - 2).getObjectName();
            return new ObjectSummaryCollection(objectSummaries, bucket.getBucketName(), prefixes,
                    next, secondLastObject, lastObject);
        });
    }

    private ListResponse<? extends ObjectBaseSummary> makeListCall(CommonRequestContext commonContext,
                                                                   List<ObjectProperties> properties,
                                                                   boolean retryable,
                                                                   int limit,
                                                                   WSTenantBucketInfo bucket,
                                                                   @Nullable String prefix,
                                                                   @Nullable String startWith,
                                                                   @Nullable String endBefore) {
        if (canMakeNameOnlyListCall(properties)) {
            ListNameOnlyResponse mdsListResponse = BackendHelper.invokeMds(MdsMetrics.OBJECT_MDS_BUNDLE,
                    MdsMetrics.OBJECT_MDS_LIST_OBJECTS_NAME_ONLY,
                    retryable,
                    () -> objectClient.execute(c -> getClientWithOptions(c,
                            requestDeadlineSeconds,
                            commonContext,
                            listObjectsMaxMessageBytes).listObjectsNameOnly(ListObjectsRequest.newBuilder()
                            .setBucketToken(bucket.getMdsBucketToken().toByteString())
                            .setBucketKey(MdsTransformer.toMdsBucketKey(bucket.getKey()))
                            .setPageSize(limit)
                            .setPrefix(StringUtils.defaultString(prefix))
                            .setStartWith(StringUtils.defaultString(startWith))
                            .setEndBefore(StringUtils.defaultString(endBefore))
                            .build())));

            return BackendConversions.mdsListResponseToListResponse(mdsListResponse);
        } else if (canMakeS3ListCall(properties)) {
            ListS3Response mdsListResponse = BackendHelper.invokeMds(MdsMetrics.OBJECT_MDS_BUNDLE,
                    MdsMetrics.OBJECT_MDS_LIST_OBJECTS_S3,
                    retryable,
                    () -> objectClient.execute(c -> getClientWithOptions(c,
                            requestDeadlineSeconds,
                            commonContext,
                            listObjectsMaxMessageBytes).listObjectsS3(ListObjectsRequest.newBuilder()
                            .setBucketToken(bucket.getMdsBucketToken().toByteString())
                            .setBucketKey(MdsTransformer.toMdsBucketKey(bucket.getKey()))
                            .setPageSize(limit)
                            .setPrefix(StringUtils.defaultString(prefix))
                            .setStartWith(StringUtils.defaultString(startWith))
                            .setEndBefore(StringUtils.defaultString(endBefore))
                            .build())));
            return BackendConversions.mdsListResponseToListResponse(mdsListResponse);
        } else if (canMakeBasicListCall(properties)) {
            ListBasicResponse mdsListResponse = BackendHelper.invokeMds(MdsMetrics.OBJECT_MDS_BUNDLE,
                    MdsMetrics.OBJECT_MDS_LIST_OBJECTS_BASIC,
                    retryable,
                    () -> objectClient.execute(c -> getClientWithOptions(c,
                            requestDeadlineSeconds,
                            commonContext,
                            listObjectsMaxMessageBytes).listObjectsBasic(ListObjectsRequest.newBuilder()
                            .setBucketToken(bucket.getMdsBucketToken().toByteString())
                            .setBucketKey(MdsTransformer.toMdsBucketKey(bucket.getKey()))
                            .setPageSize(limit)
                            .setPrefix(StringUtils.defaultString(prefix))
                            .setStartWith(StringUtils.defaultString(startWith))
                            .setEndBefore(StringUtils.defaultString(endBefore))
                            .build())));
            return BackendConversions.mdsListResponseToListResponse(mdsListResponse);
        } else {
            ListObjectsResponse mdsListResponse = BackendHelper.invokeMds(MdsMetrics.OBJECT_MDS_BUNDLE,
                    MdsMetrics.OBJECT_MDS_LIST_OBJECTS,
                    retryable,
                    () -> objectClient.execute(c -> getClientWithOptions(c,
                            requestDeadlineSeconds,
                            commonContext,
                            listObjectsMaxMessageBytes).listObjects(ListObjectsRequest.newBuilder()
                            .setBucketToken(bucket.getMdsBucketToken().toByteString())
                            .setBucketKey(MdsTransformer.toMdsBucketKey(bucket.getKey()))
                            .setPageSize(limit)
                            .setPrefix(StringUtils.defaultString(prefix))
                            .setStartWith(StringUtils.defaultString(startWith))
                            .setEndBefore(StringUtils.defaultString(endBefore))
                            .build())));
            return BackendConversions.mdsListResponseToListResponse(mdsListResponse);
        }
    }

    private boolean canMakeNameOnlyListCall(List<ObjectProperties> properties) {
        return properties.size() == 1 && properties.contains(ObjectProperties.NAME);
    }

    private boolean canMakeS3ListCall(List<ObjectProperties> properties) {
        for (ObjectProperties property : properties) {
            if (!S3_OBJECT_PROPERTIES.contains(property)) {
                return false;
            }
        }
        return true;
    }

    private boolean canMakeBasicListCall(List<ObjectProperties> properties) {
        for (ObjectProperties property : properties) {
            if (!OBJECT_PROPERTIES_BASIC.contains(property)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Authorizes for operation with permissions. Throws exception if operation is not authorized.
     */
    static WSTenantBucketInfo authorizeOperationForPermissions(
            MetricScope scope,
            RoutingContext context,
            Authorizer authorizer,
            AuthenticationInfo authInfo,
            WSTenantBucketInfo bucket,
            CasperOperation operation,
            CasperPermission... permissions) {
        // Put objectLevelAuditMode in the context to be retrieved by AuditFilterHandler
        WSRequestContext.get(context).setObjectLevelAuditMode(bucket.getObjectLevelAuditMode());

        final String kmsKeyId = bucket.getKmsKeyId().orElse(null);

        return authorizeOperationForPermissionsInternal(
                scope,
                b -> authorizer.authorize(
                        WSRequestContext.get(context),
                        authInfo,
                        b.getNamespaceKey(),
                        b.getBucketName(),
                        b.getCompartment(),
                        b.getPublicAccessType(),
                        operation,
                        kmsKeyId,
                        true,
                        b.isTenancyDeleted(),
                        permissions
                ).isPresent(),
                bucket,
                operation
        );
    }

    /**
     * Authorizes for given CasperOperation with given CasperPermission against the given bucket.
     *
     * @param scope             MetricScope used to generate traces of call trees.
     * @param authorizeCallback a {@link Consumer} parameter that accepts array of {@link CasperPermission} for authZ.
     * @param bucket            the bucket name.
     * @param operation         Casper operation when authorizing client requests.
     * @return {@link WSTenantBucketInfo} of target bucket.
     **/
    static WSTenantBucketInfo authorizeOperationForPermissionsInternal(
            MetricScope scope,
            Function<WSTenantBucketInfo, Boolean> authorizeCallback,
            WSTenantBucketInfo bucket,
            CasperOperation operation) {
        VertxUtil.assertOnVertxWorkerThread();
        return MetricScope.timeScope(scope,
                String.format("authorizeOperationForPermissionsInternal:%s", operation),
                (innerScope) -> {
                    if (!authorizeCallback.apply(bucket)) {
                        throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucket.getKey().getName(),
                                bucket.getKey().getNamespace()));
                    }
                    return bucket;
                });
    }

    /**
     * Starts an upload to create a large object. Generates an upload id that is returned. Parts
     * are then written and stored against this upload. The upload may be aborted or finished, at
     * which point the object is updated to contain the data of the upload. Provides
     * optional support for concurrency control against the current object if it exists.
     */
    public UploadMetadata beginMultipartUpload(RoutingContext context,
                                               AuthenticationInfo authInfo,
                                               CreateUploadRequest createUploadRequest) {
        Validator.validateV2Namespace(createUploadRequest.getNamespace());
        Validator.validateBucket(createUploadRequest.getBucketName());
        Validator.validateObjectName(createUploadRequest.getObjectName());
        Validator.validateMetadata(createUploadRequest.getMetadata(), jsonSerializer);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope scope = rootScope.child("Backend:beginMultipartUpload");
        final WSTenantBucketInfo bucket = bucketBackend.getBucketMetadataWithCache(context,
                scope,
                createUploadRequest.getNamespace(),
                createUploadRequest.getBucketName(),
                Api.V2);
        authorizeOperationForPermissions(scope, context, authorizer, authInfo, bucket,
                CasperOperation.CREATE_MULTIPART_UPLOAD,
                CasperPermission.OBJECT_CREATE,
                CasperPermission.OBJECT_OVERWRITE);

        // Blocking multipart upload to recycle bin
        if (RecycleBinHelper.isBinObject(bucket, createUploadRequest.getObjectName())) {
            throw new AuthorizationException(V2ErrorCode.FORBIDDEN.getStatusCode(),
                    V2ErrorCode.FORBIDDEN.getErrorName(), "Not authorized to upload objects into the Recycle Bin");
        }

        ReplicationEnforcer.throwIfReadOnlyButNotReplicationScope(context, bucket);

        WSRequestContext.get(context).setBucketCreator(bucket.getCreationUser());
        WSRequestContext.get(context).setBucketOcid(bucket.getOcid());
        WSRequestContext.get(context).setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(bucket.getOptions()));

        final Principal servicePrincipal = authInfo.getServicePrincipal().orElse(null);
        final String uploadIdPrefix =
                servicePrincipal != null && oboPrincipal.contains(servicePrincipal.getTenantId()) ? "COPY_" : "";
        final String uploadId = uploadIdPrefix + IdUtil.newUniqueId().substring(uploadIdPrefix.length());
        final EncryptionKey metadataEncryptionKey = getEncryption(bucket.getKmsKeyId().orElse(null), kms);

        final String encryptedMetadata =
                CryptoUtils.encryptMetadata(createUploadRequest.getMetadata(), metadataEncryptionKey);

        final MdsObjectKey objectKey = objectKey(
                    createUploadRequest.getObjectName(),
                    bucket.getNamespaceKey().getName(),
                    bucket.getBucketName(),
                    bucket.getNamespaceKey().getApi());
        final MdsUploadInfo uploadInfo = BackendHelper.invokeMds(
                MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_BEGIN_UPLOAD,
                false, () -> objectClient.execute(c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                        .beginUpload(BeginUploadRequest.newBuilder()
                                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                .setUploadKey(mdsUploadKey(uploadId, objectKey))
                                .setEncryptionKey(MdsTransformer.toMdsEncryptionKey(metadataEncryptionKey))
                                .setMetadata(encryptedMetadata)
                                .setIfMatchEtag(StringUtils.defaultString(createUploadRequest.getIfMatchEtag()))
                                .setIfNoneMatchEtag(StringUtils.defaultString(createUploadRequest.getIfNoneMatchEtag()))
                                .setStorageTier(MdsObjectStorageTier.OST_STANDARD)
                                .build())
                        .getUpload()));

        WSRequestContext.get(context).addNewState("uploadId", uploadId);
        ServiceLogsHelper.logServiceEntry(context);

        return BackendConversions.mdsUploadInfoToUploadMetadata(uploadInfo);
    }

    protected static ListUploadsResponse listMultipartUploads(
            MdsExecutor<ObjectServiceBlockingStub> objectClient,
            long requestDeadlineSeconds,
            WSTenantBucketInfo bucket,
            CommonRequestContext commonContext,
            @Nullable String prefix,
            boolean enforceLimit,
            int limit,
            @Nullable String objectNameStartAfter,
            @Nullable String uploadId,
            boolean validateUploadIdMarker) {
        return BackendHelper.invokeMds(MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_LIST_UPLOADS,
                true, () -> objectClient.execute(c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                        .listUploads(ListUploadsRequest.newBuilder()
                                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                .setBucketKey(bucketKey(bucket.getNamespaceKey().getName(),
                                        bucket.getBucketName(),
                                        bucket.getNamespaceKey().getApi()))
                                .setLimit(limit)
                                .setValidateUploadIdStartAfter(validateUploadIdMarker)
                                .setPrefix(StringUtils.defaultString(prefix))
                                .setObjectNameStartAfter(StringUtils.defaultString(objectNameStartAfter))
                                .setUploadIdStartAfter(StringUtils.defaultString(uploadId))
                                .setEnforceLimit(enforceLimit)
                                .build())));
    }

    /**
     * Lists the uploads that are under a bucket. May provide an optional cursor to return upload
     * whose id is greater than or equal to the cursor.
     */
    public UploadPaginatedList listMultipartUploads(
            RoutingContext context,
            AuthenticationInfo authInfo,
            @Nullable Character delimiter,
            @Nullable String prefix,
            int limit,
            boolean validateUploadIdMarker,
            UploadIdentifier cursor) {
        Validator.validateV2Namespace(cursor.getNamespace());
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope scope = rootScope.child("Backend:listMultipartUploads")
                .annotate("prefix", prefix)
                .annotate("delimiter", delimiter)
                .annotate("limit", limit)
                .annotate("cursor", cursor);
        final String bucketName = cursor.getBucketName();

        Validator.validateBucket(bucketName);
        final WSTenantBucketInfo bucket =
            bucketBackend.getBucketMetadataWithCache(context, scope, cursor.getNamespace(), bucketName, Api.V2);
        WSRequestContext.get(context).setBucketOcid(bucket.getOcid());
        WSRequestContext.get(context).setBucketCreator(bucket.getCreationUser());
        WSRequestContext.get(context).setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(bucket.getOptions()));
        authorizeOperationForPermissions(scope,
                context,
                authorizer,
                authInfo,
                bucket,
                CasperOperation.LIST_MULTIPART_UPLOAD,
                CasperPermission.BUCKET_READ);
        // When delimiter is specified, we set limit as upper bound 1000 to get enough objects from DB,
        // Meanwhile, we only call DB once because looping and repeat call to DB would be susceptible
        // to motion blur.
        // Taking into account multi shard buckets in this case we making limit big enough so even if uploads reside on
        // only one bucket shard we still can get max number of uploads.
        // 4/26/2019 - The max page size is reduced from ObjectDb.MAX_LIST_UPLOADS_LIMIT * 10 to
        // ObjectDb.MAX_LIST_UPLOADS_LIMIT because object MDS has an upper limit for page size of 10k.
        final int limitToUse = delimiter == null ? limit : ObjectDb.MAX_LIST_UPLOADS_LIMIT;
        final boolean enforceLimit = (delimiter == null);
        final ListUploadsResponse mdsResponse = listMultipartUploads(
                objectClient, requestDeadlineSeconds, bucket,
                commonContext, prefix, enforceLimit,
                limitToUse, cursor.getObjectName(), cursor.getUploadId(),
                validateUploadIdMarker);
        ServiceLogsHelper.logServiceEntry(context);
        if (delimiter == null) {
            return listMultipartUploadsWithoutDelimiter(mdsResponse);
        } else {
            return listMultipartUploadsWithDelimiter(mdsResponse, prefix, delimiter, limit);
        }
    }

    private UploadPaginatedList listMultipartUploadsWithoutDelimiter(ListUploadsResponse results) {
        List<UploadMetadata> uploadMetadataList = new ArrayList<>();
        results.getUploadsList().forEach(
                uploadInfo -> uploadMetadataList.add(BackendConversions.mdsUploadInfoToUploadMetadata(uploadInfo)));
        return new UploadPaginatedList(uploadMetadataList, null, !results.getNextObjectNameStartAfter().isEmpty());
    }

    private UploadPaginatedList listMultipartUploadsWithDelimiter(ListUploadsResponse results,
                                                                  String prefix,
                                                                  Character delimiter,
                                                                  int limit) {
        List<UploadMetadata> uploadMetadataList = new ArrayList<>();
        SortedSet<String> prefixesSet = new TreeSet<>();
        int prefixLength = prefix == null ? 0 : prefix.length();
        final List<MdsUploadInfo> keys = results.getUploadsList();
        boolean isTruncate = false;

        for (MdsUploadInfo mdsUploadInfo : keys) {
            String name = mdsUploadInfo.getUploadKey().getObjectKey().getObjectName();
            int index = name.indexOf(delimiter, prefixLength);

            // Adding key to either Upload list or CommonPrefixes set only if sum of these two list is
            // less than limit number.
            if (uploadMetadataList.size() + prefixesSet.size() < limit) {
                if (index == -1) {
                    uploadMetadataList.add(BackendConversions.mdsUploadInfoToUploadMetadata(mdsUploadInfo));
                } else {
                    // If request defines prefix, it will be included in the final string,
                    // which is indeed what S3 does.
                    prefixesSet.add(name.substring(0, index + 1));
                }
                // When sum of Upload list size and CommonPrefixes set size reaches limit, we still gonna
                // to see if we could add one more key, if so, we know the list result is been truncated.
                // 1. index == -1 means that we found an object which exists in the pseudo-directory, and would
                // otherwise be added to uploadMetadataList, so the result is indeed truncated.
                // 2. Search through the set of prefixesSet to see if the prefix of this object is already present,
                // and if it isn't (!), then the result is indeed truncated. We could optimize the time complexity of
                // a corner case here when the delimiter is at the end (index == name.length() - 1), but for now
                // I will keep this as it is to make our logic more clear.
            } else if (index == -1 || !prefixesSet.contains(name.substring(0, index + 1))) {
                isTruncate = true;
                break;
            }
        }
        // Result will be marked as truncated be it marked by StorageObjectDb, or by the logic above.
        return new UploadPaginatedList(
                uploadMetadataList,
                prefixesSet,
                !results.getNextObjectNameStartAfter().isEmpty() || isTruncate);
    }

    /**
     * Lists the parts written for an upload. May provide an optional partNum cursor to return parts
     * greater than or equal to the cursor.
     */
    public PaginatedList<PartMetadata> listUploadParts(RoutingContext context,
                                                       AuthenticationInfo authInfo,
                                                       int limit,
                                                       UploadIdentifier uploadIdentifier,
                                                       @Nullable Integer partNumCursor) {
        Validator.validateV2Namespace(uploadIdentifier.getNamespace());

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope scope = rootScope.child("Backend:listUploadParts");
        final String bucketName = uploadIdentifier.getBucketName();

        Validator.validateBucket(bucketName);
        try {
            WSTenantBucketInfo bucket = bucketBackend.getBucketMetadataWithCache(context, scope,
                    uploadIdentifier.getNamespace(), bucketName, Api.V2);
            WSRequestContext.get(context).setBucketCreator(bucket.getOcid());
            WSRequestContext.get(context).setBucketCreator(bucket.getCreationUser());
            WSRequestContext.get(context).setBucketLoggingStatus(BucketOptionsUtil
                    .getBucketLoggingStatusFromOptions(bucket.getOptions()));
            authorizeOperationForPermissions(scope, context, authorizer, authInfo, bucket,
                    CasperOperation.LIST_MULTIPART_UPLOAD_PARTS, CasperPermission.OBJECT_INSPECT);
            ListUploadedPartsResponse results = MetricScope.timeScope(scope, "mdsObjectClient:listUploadedParts",
                    (innerScope) -> {
                        final ListUploadedPartsRequest.Builder builder = ListUploadedPartsRequest.newBuilder()
                                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                .setUploadKey(mdsUploadKey(uploadIdentifier.getUploadId(),
                                        objectKey(uploadIdentifier.getObjectName(),
                                                bucket.getNamespaceKey().getName(),
                                                bucket.getBucketName(),
                                                bucket.getNamespaceKey().getApi())))
                                .setLimit(limit);
                        if (partNumCursor != null) {
                            builder.setPartNumStartAfter(Int32Value.of(partNumCursor));
                        }
                        return BackendHelper.invokeMds(
                                MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_LIST_UPLOADED_PARTS,
                                true, () -> objectClient.execute(
                                        c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                                .listUploadedParts(builder.build())));
                    });
            ServiceLogsHelper.logServiceEntry(context);
            return new PaginatedList<>(results.getPartsList().stream()
                    .map(s -> BackendConversions.mdsPartInfoToPartMetadata(s))
                    .collect(Collectors.toList()),
                    results.hasNextPartNumStartAfter());
        } catch (NoSuchUploadException ex) {
            throw new MultipartUploadNotFoundException("No such upload", ex);
        }
    }

    /**
     * Aborts the upload and frees up the space used to store the parts.
     */
    public void abortMultipartUpload(RoutingContext context,
                                     AuthenticationInfo authInfo,
                                     UploadIdentifier uploadIdentifier) {
        Validator.validateV2Namespace(uploadIdentifier.getNamespace());

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope scope = rootScope.child("Backend:abortMultipartUpload");
        final String bucketName = uploadIdentifier.getBucketName();
        Validator.validateBucket(bucketName);
        WSTenantBucketInfo bucket = bucketBackend.getBucketMetadataWithCache(context, scope,
                uploadIdentifier.getNamespace(), bucketName, Api.V2);
        WSRequestContext.get(context).setBucketOcid(bucket.getOcid());
        WSRequestContext.get(context).setBucketCreator(bucket.getCreationUser());
        WSRequestContext.get(context).setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(bucket.getOptions()));

        authorizeOperationForPermissions(scope,
                context, authorizer, authInfo, bucket,
                CasperOperation.ABORT_MULTIPART_UPLOAD,
                CasperPermission.OBJECT_DELETE);
        try {
            MetricScope.timeScope(scope, "objectMds:abortUploadRequest",
                    (Consumer<MetricScope>) (innerScope) -> BackendHelper.invokeMds(
                            MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_ABORT_UPLOAD,
                            false, () -> objectClient.execute(
                                    c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                            .abortUpload(AbortUploadRequest.newBuilder()
                                                    .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                                    .setUploadKey(mdsUploadKey(uploadIdentifier.getUploadId(),
                                                            objectKey(uploadIdentifier.getObjectName(),
                                                                    bucket.getNamespaceKey().getName(),
                                                                    bucket.getBucketName(),
                                                                    bucket.getNamespaceKey().getApi())))
                                                    .build()))));
        } catch (NoSuchUploadException ex) {
            throw new MultipartUploadNotFoundException("No such upload", ex);
        }
        WSRequestContext.get(context).addOldState("uploadId", uploadIdentifier.getUploadId());
        ServiceLogsHelper.logServiceEntry(context);
    }

    /**
     * Finishes the part and updates the object to contain the information of the part. Provides
     * optional support for concurrency control against the current object if it exists.
     *
     * @return the new object metadata.
     */
    public ObjectMetadata finishMultipartUpload(RoutingContext context,
                                                AuthenticationInfo authInfo,
                                                FinishUploadRequest finishUploadRequest) {
        return finishMultipartUpload(context, authInfo, finishUploadRequest, null, null, null);
    }

    public ObjectMetadata finishMultipartUpload(RoutingContext context,
                                                AuthenticationInfo authInfo,
                                                FinishUploadRequest finishUploadRequest,
                                                @Nullable String md5Override,
                                                @Nullable String etagOverride,
                                                @Nullable Integer partCountOverride) {
        Validator.validateV2Namespace(finishUploadRequest.getUploadIdentifier().getNamespace());
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope scope = rootScope.child("Backend:finishMultipartUpload");
        final String bucketName = finishUploadRequest.getUploadIdentifier().getBucketName();

        Validator.validateBucket(bucketName);
        WSTenantBucketInfo bucket = bucketBackend.getBucketMetadataWithCache(context,
                scope,
                finishUploadRequest.getUploadIdentifier().getNamespace(),
                bucketName, Api.V2);
        authorizeOperationForPermissions(scope,
                context, authorizer, authInfo, bucket,
                CasperOperation.COMMIT_MULTIPART_UPLOAD,
                CasperPermission.OBJECT_CREATE,
                CasperPermission.OBJECT_OVERWRITE);

        final String md5;
        if (md5Override == null) {
            md5 = calculateMD5ForUpload(context, scope, bucket, finishUploadRequest.getUploadIdentifier(),
                    finishUploadRequest.getETagType(), finishUploadRequest.getPartsToCommit());
        } else {
            md5 = md5Override;
        }

        return finishMultipartUploadHelper(context, authInfo, finishUploadRequest, bucket, scope, md5, etagOverride,
                partCountOverride);
    }

    /**
     * Finishes the upload assuming all parts with this {@link UploadIdentifier} are to be included in the final object.
     */
    public ObjectMetadata finishMultipartUploadViaPars(RoutingContext context,
                                                       AuthenticationInfo authInfo,
                                                       UploadIdentifier uploadIdentifier,
                                                       String ifMatch,
                                                       String ifNoneMatch) {
        Validator.validateV2Namespace(uploadIdentifier.getNamespace());
        // NOTE: 15 lines of almost duplicate code with the above method, but couldn't think of a way to refactor
        // without sacrificing readability. Go ahead and fix it if are smarter than me!
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope scope = rootScope.child("Backend:finishMultipartUpload");
        final String bucketName = uploadIdentifier.getBucketName();

        Validator.validateBucket(bucketName);
        WSTenantBucketInfo bucket = bucketBackend.getBucketMetadataWithCache(context, scope,
                uploadIdentifier.getNamespace(), bucketName, Api.V2);

        authorizeOperationForPermissions(scope,
                context, authorizer, authInfo, bucket,
                CasperOperation.COMMIT_MULTIPART_UPLOAD,
                CasperPermission.OBJECT_CREATE,
                CasperPermission.OBJECT_OVERWRITE);

        final Collection<String> partsMd5s = new ArrayList<>();
        final SortedMap<Integer, PartAndETag> partsToCommit = new TreeMap<>();
        MetricScope.timeScopeC(scope, "objectMds:listUploadedParts",
                innerScope -> {
                    ListUploadedPartsRequest.Builder builder = ListUploadedPartsRequest.newBuilder()
                            .setBucketToken(bucket.getMdsBucketToken().toByteString())
                            .setUploadKey(mdsUploadKey(uploadIdentifier.getUploadId(),
                                    objectKey(uploadIdentifier.getObjectName(),
                                            bucket.getNamespaceKey().getName(),
                                            bucket.getBucketName(),
                                            bucket.getNamespaceKey().getApi())))
                            .setLimit(StorageObjectDb.MAX_LIST_MULTIPART_LIMIT);
                    ListUploadedPartsResponse response;
                    Int32Value partNum = null;
                    while (true) {
                        final ListUploadedPartsRequest.Builder builderCurrent = builder.clone();
                        if (partNum != null) {
                            builderCurrent.setPartNumStartAfter(partNum);
                        }
                        try {
                            response = BackendHelper.invokeMds(
                                    MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_LIST_UPLOADED_PARTS, true,
                                    () -> objectClient.execute(
                                            c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                                    .listUploadedParts(builderCurrent.build())));
                        } catch (NoSuchUploadException ex) {
                            throw new MultipartUploadNotFoundException(ex.getMessage(), ex);
                        }
                        response.getPartsList().forEach(p -> {
                            int uploadPartNum = p.getPartKey().getUploadPartNum();
                            PartAndETag partAndETag = PartAndETag.builder()
                                    .uploadPartNum(uploadPartNum)
                                    .eTag(p.getObjectBase().getEtag())
                                    .build();
                            partsToCommit.put(uploadPartNum, partAndETag);
                            partsMd5s.add(p.getObjectBase().getMd5());
                        });
                        if (!response.hasNextPartNumStartAfter()) {
                            break;
                        }
                        partNum = response.getNextPartNumStartAfter();
                    }
                });
        final String md5 = md5OfMd5(partsMd5s);

        final FinishUploadRequest finishUploadRequest = new FinishUploadRequest(
                uploadIdentifier,
                ETagType.ETAG,
                ImmutableSortedSet.copyOf(partsToCommit.values()),
                ImmutableSortedSet.of(),
                ifMatch,
                ifNoneMatch);
        return finishMultipartUploadHelper(context, authInfo, finishUploadRequest, bucket, scope, md5, null, null);
    }

    /**
     * Calculate what the MD5 of the multipart object will be _if_ we commit
     * these parts successfully.
     *
     * This method does not do extensive error checking -- we still rely on
     * the FinishUpload call to do that.
     */
    private String calculateMD5ForUpload(
            RoutingContext context,
            MetricScope scope,
            WSTenantBucketInfo bucket,
            UploadIdentifier uploadIdentifier,
            ETagType eTagType,
            Set<PartAndETag> partsToCommit) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final List<String> partMD5s = new ArrayList<>();

        // In order to calculate the MD5 of an upload we need ot know the MD5 of
        // all its parts (or rather, of all the parts we plan to commit).
        return MetricScope.timeScope(scope, "storageObjDb:listUploadedParts",
            innerScope -> {
                ListUploadedPartsRequest.Builder builder = ListUploadedPartsRequest.newBuilder()
                        .setBucketToken(bucket.getMdsBucketToken().toByteString())
                        .setUploadKey(mdsUploadKey(uploadIdentifier.getUploadId(),
                                objectKey(uploadIdentifier.getObjectName(),
                                        bucket.getNamespaceKey().getName(),
                                        bucket.getBucketName(),
                                        bucket.getNamespaceKey().getApi())))
                        .setLimit(StorageObjectDb.MAX_LIST_MULTIPART_LIMIT);
                ListUploadedPartsResponse response;
                Int32Value partNum = null;
                while (true) {
                    try {
                        ListUploadedPartsRequest.Builder builderCurrent = builder.clone();
                        if (partNum != null) {
                            builderCurrent.setPartNumStartAfter(partNum);
                        }
                        response = BackendHelper.invokeMds(
                                MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_LIST_UPLOADED_PARTS,
                                true, () -> objectClient.execute(
                                        c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                                .listUploadedParts(builderCurrent.build())));
                    } catch (NoSuchUploadException ex) {
                        // FinishUpload has an idempotency check that can return
                        // successfully if the upload has already been committed.
                        // To support that we have to return a dummy MD5 value here.
                        return "FEyd76wElpx7+tjvqo6hlA=="; // MD5 of 'fake'
                    }
                    for (MdsPartInfo mdsPartInfo : response.getPartsList()) {
                        int uploadPartNum = mdsPartInfo.getPartKey().getUploadPartNum();
                        // S3 uploads identity parts using the MD5 hash as the etag, while
                        // native uploads use the real etag.
                        final String etag = eTagType.equals(ETagType.ETAG)
                                ? mdsPartInfo.getObjectBase().getEtag() : mdsPartInfo.getObjectBase().getMd5();
                        final PartAndETag partAndETag = PartAndETag.builder()
                            .uploadPartNum(uploadPartNum)
                            .eTag(etag)
                            .build();
                        if (partsToCommit.contains(partAndETag)) {
                            // This is a request that we plan to commit.
                            partMD5s.add(mdsPartInfo.getObjectBase().getMd5());
                        }
                    }
                    if (!response.hasNextPartNumStartAfter()) {
                        break;
                    }
                    partNum = response.getNextPartNumStartAfter();
                }
                return md5OfMd5(partMD5s);
            });
    }

    private ObjectMetadata finishMultipartUploadHelper(RoutingContext context,
                                                       AuthenticationInfo authInfo,
                                                       FinishUploadRequest finishUploadRequest,
                                                       WSTenantBucketInfo bucket,
                                                       MetricScope scope,
                                                       String md5,
                                                       @Nullable String etagOverride,
                                                       @Nullable Integer partCountOverride) {
        Preconditions.checkNotNull(md5);
        final Duration minimumRetentionPeriod;
        final Duration archivePeriodForNewObject;
        final boolean isArchivalObject = bucket.getStorageTier() == BucketStorageTier.Archive;
        if (isArchivalObject) {
            minimumRetentionPeriod = archiveConf.getMinimumRetentionPeriodForArchives();
            archivePeriodForNewObject = archiveConf.getArchivePeriodForNewObject();
        } else {
            minimumRetentionPeriod = null;
            archivePeriodForNewObject = null;
        }

        WSRequestContext.get(context).setBucketCreator(bucket.getCreationUser());
        WSRequestContext.get(context).setBucketOcid(bucket.getOcid());
        WSRequestContext.get(context).setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(bucket.getOptions()));

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final AtomicBoolean isCreate = new AtomicBoolean(false);
        MdsObjectSummary mdsObjectSummary = MetricScope.timeScope(scope, "objectMds:finishUploadRequest",
                (innerScope) -> {
                    final String objectName = finishUploadRequest.getUploadIdentifier().getObjectName();
                    // If the object name is longer than the max object name size allowed in the database,
                    // the object name can never be found in the database if the request were ever sent to
                    // object MDS. We do the name length check and throw the exception without calling
                    // into object MDS. This special case here is meant to be a temporary solution to
                    // make the ObjectStorageTest pass until we find a better and more general solution.
                    try {
                        Validator.validateObjectName(objectName);
                    } catch (TooLongObjectNameException | InvalidObjectNameException ex) {
                        throw new MultipartUploadNotFoundException("No such upload");
                    }
                    try {
                        com.oracle.pic.casper.mds.object.FinishUploadRequest.Builder builder =
                                com.oracle.pic.casper.mds.object.FinishUploadRequest.newBuilder()
                                        .setUploadKey(MdsUploadKey.newBuilder()
                                                .setObjectKey(MdsObjectKey.newBuilder()
                                                        .setBucketKey(bucketKey(bucket.getNamespaceKey().getName(),
                                                                bucket.getBucketName(),
                                                                bucket.getNamespaceKey().getApi()))
                                                        .setObjectName(finishUploadRequest.getUploadIdentifier()
                                                                .getObjectName())
                                                        .build())
                                                .setUploadId(finishUploadRequest.getUploadIdentifier().getUploadId())
                                                .build())
                                        .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                        .setEtagType(MdsTransformer.toEtagType(finishUploadRequest.getETagType()))
                                        .setEtag(etagOverride == null ? CommonUtils.generateETag() : etagOverride)
                                        .setIfMatchEtag(StringUtils.defaultString(finishUploadRequest.getIfMatch()))
                                        .setIfNoneMatchEtag(
                                                StringUtils.defaultString(finishUploadRequest.getIfNoneMatch()))
                                        .setMaxFinalObjBytes(webServerConf.getMaxUploadedObjectSize().longValue(BYTE))
                                        .setMd5Override(md5);
                        if (minimumRetentionPeriod != null) {
                            builder.setMinRetention(TimestampUtils.toProtoDuration(minimumRetentionPeriod));
                        }
                        if (archivePeriodForNewObject != null) {
                            builder.setTimeToArchive(TimestampUtils.toProtoDuration(archivePeriodForNewObject));
                        }
                        if (finishUploadRequest.getPartsToCommit() != null) {
                            for (PartAndETag partAndETag : finishUploadRequest.getPartsToCommit()) {
                                builder.addPartsToCommit(MdsPartEtag.newBuilder()
                                        .setPartNum(partAndETag.getUploadPartNum())
                                        .setEtag(partAndETag.getEtag())
                                        .build());
                            }
                        }
                        if (finishUploadRequest.getPartsToExclude() != null) {
                            for (Integer integer : finishUploadRequest.getPartsToExclude()) {
                                builder.addPartsToExclude(integer);
                            }
                        } else {
                            builder.setDeleteUnspecifiedParts(true);
                        }
                        if (partCountOverride != null) {
                            builder.setMultipartOverride(true);
                            builder.setPartcountOverride(partCountOverride);
                        }
                        FinishUploadResponse finishUploadResponse = BackendHelper.invokeMds(
                                MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_FINISH_UPLOAD,
                                false, () -> objectClient.execute(
                                        c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                                .finishUpload(builder.build())));
                        if (finishUploadResponse.getIsNewObject()) {
                            isCreate.set(true);
                        }
                        return finishUploadResponse.getObjectSummary();
                    } catch (NoSuchUploadException ex) {
                        throw new MultipartUploadNotFoundException("No such upload", ex);
                    }
                });
        final WSRequestContext wsContext = WSRequestContext.get(context);
        wsContext.addOldState("uploadId", finishUploadRequest.getUploadIdentifier().getUploadId());
        wsContext.addNewState("objectName", mdsObjectSummary.getObjectKey().getObjectName());


        final ObjectMetadata objectMetadata =
            BackendConversions.mdsObjectSummaryToObjectMetadata(mdsObjectSummary, kms);
        WSRequestContext.get(context).setEtag(objectMetadata.getETag());

        WSRequestContext.get(context).setObjectEventsEnabled(bucket.isObjectEventsEnabled());
        eventPublisher.addEvent(new CasperObjectEvent(kms,
                        bucket,
                        mdsObjectSummary.getObjectKey().getObjectName(),
                        objectMetadata.getETag(),
                        objectMetadata.getArchivalState(),
                        isCreate.get() ? EventAction.CREATE : EventAction.UPDATE),
                authInfo.getMainPrincipal().getTenantId(),
                bucket.getObjectLevelAuditMode());
        WSRequestContext.get(context).addObjectEvent(
                new ObjectEvent(isCreate.get() ?
                        ObjectEvent.EventType.CREATE_OBJECT : ObjectEvent.EventType.UPDATE_OBJECT,
                        mdsObjectSummary.getObjectKey().getObjectName(), objectMetadata.getETag(),
                        objectMetadata.getArchivalState().getState()));

        WSRequestContext.get(context).setArchivalState(objectMetadata.getArchivalState());
        ServiceLogsHelper.logServiceEntry(context);
        return objectMetadata;
    }

    public void reencryptObject(RoutingContext context,
                                AuthenticationInfo authInfo,
                                String namespace,
                                String bucketName,
                                String objectName,
                                ReencryptObjectDetails reencryptObjectDetails) {
        Preconditions.checkNotNull(authInfo, "AuthInfo cannot be null");
        Preconditions.checkNotNull(reencryptObjectDetails);
        Validator.validateV2Namespace(namespace);
        Validator.validateBucket(bucketName);
        Validator.validateObjectName(objectName);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("backend:reencryptObject");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, objectName);

        final WSTenantBucketInfo bucket = bucketBackend.getBucketMetadataWithCache(context, childScope, namespace,
                bucketName, Api.V2);

        final WSStorageObject wsStorageObject =
                getObject(context, bucket, objectName).orElseThrow(() -> new ObjectNotFoundException(String.format(
                        "The object %s is not found", objectName)));

        // User can pass a kmsKey to reencrypt DEK, regardless how the object is encrypted now (by kms or master key).
        // But we don't maintain what kmsKey encrypted the object, KMS is able to decrypt encrypted DEK without knowing
        // the corresponding kmsKey. KMS can also reencrypt a encrypted DEK with new kmsKey.
        // if user doesn't provide a kmsKey, we will need to use the kmsKey associated with the bucket to reencrypt
        // the DEK of the object and it's chunks
        String kmsKeyId = reencryptObjectDetails.getKmsKeyId();
        if (!Strings.isNullOrEmpty(kmsKeyId)) {
            Validator.validateKmsKeyId(kmsKeyId);
        } else {
            kmsKeyId = bucket.getKmsKeyId().orElse(null);
        }
        if (Strings.isNullOrEmpty(kmsKeyId)) {
            throw new KmsKeyNotProvidedException("You didn't provide a kmsKeyId or the bucket is not associated " +
                    "with a KmsKey");
        }

        // for audit logs
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.setObjectLevelAuditMode(bucket.getObjectLevelAuditMode());
        wsRequestContext.setNewkmsKeyId(kmsKeyId);
        wsRequestContext.setBucketOcid(bucket.getOcid());
        wsRequestContext.setEtag(wsStorageObject.getETag());
        wsRequestContext.setBucketCreator(bucket.getCreationUser());
        wsRequestContext.setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(bucket.getOptions()));

        // It is an internal API now. we only support object that has less than REENCRYPTION_MAX_CHUNK_LIMIT chunks.
        // Because calling kms to reencrypt a DEK will introduce more latency which will timeout user's request.
        if (wsStorageObject.getObjectChunks().size() > REENCRYPTION_MAX_CHUNK_LIMIT) {
            throw new AssertionError(String.format("Object %s is too big to reencrypt.", objectName));
        }

        authorizeOperationForPermissions(childScope, context, authorizer, authInfo, bucket,
                CasperOperation.REENCRYPT_OBJECT, CasperPermission.OBJECT_OVERWRITE, CasperPermission.OBJECT_READ);

        // call kms to reencrypt object and chunks encryption keys.
        final EncryptionKey encryptionKey = wsStorageObject.getEncryptedEncryptionKey().getEncryptionKey(kms);
        final EncryptionKey reencryptedObjEncryptionKey = EncryptionUtils.rewrapDataEncryptionKey(encryptionKey,
                    kmsKeyId, kms);
        final List<byte[]> reencryptedChunkEncryptionKey = new ArrayList<>();
        final Iterator<Map.Entry<Long, WSStorageObjectChunk>> iterator =
                wsStorageObject.getObjectChunks().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, WSStorageObjectChunk> entry = iterator.next();
            final EncryptionKey chnkEncryptionKey = entry.getValue().getEncryptedEncryptionKey().getEncryptionKey(kms);
            reencryptedChunkEncryptionKey.add(EncryptionUtils.rewrapDataEncryptionKey(chnkEncryptionKey,
                    kmsKeyId, kms).getEncryptedDataEncryptionKey());
        }

        try {
            // update object encryption key and chunks encryption keys
            SetObjectEncryptionKeyRequest.Builder builder = SetObjectEncryptionKeyRequest.newBuilder()
                    .setBucketToken(bucket.getMdsBucketToken().toByteString())
                    .setObjectKey(objectKey(objectName, bucket.getNamespaceKey().getName(), bucket.getBucketName(),
                            bucket.getNamespaceKey().getApi()))
                    .setIfMatchEtag(StringUtils.defaultString(wsStorageObject.getETag()))
                    .setObjectEncryptionKey(ByteString.copyFrom(
                            reencryptedObjEncryptionKey.getEncryptedDataEncryptionKey()));
            reencryptedChunkEncryptionKey.forEach((v) -> builder.addChunkEncryptionKeys(ByteString.copyFrom(v)));
            BackendHelper.invokeMds(MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_SET_OBJECT_ENCRYPTION_KEY,
                    false, () -> objectClient.execute(
                            c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                    .setObjectEncryptionKey(builder.build())));
            ServiceLogsHelper.logServiceEntry(context);
        } catch (IfMatchException ex) {
            throw new UnexpectedEntityTagException("The entity tags do not match");
        }
    }

    @SuppressFBWarnings(value = {"NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE"})
    static EncryptionKey getEncryption(@Nullable String kmsKeyId, DecidingKeyManagementService kms) {
        final boolean usingKms = !Strings.isNullOrEmpty(kmsKeyId);
        final String encryptionKeyId = usingKms ? kmsKeyId : Secrets.ENCRYPTION_MASTER_KEY;
        return kms.generateKey(encryptionKeyId,
                EncryptionConstants.AES_ALGORITHM, EncryptionConstants.AES_KEY_SIZE_BYTES);
    }

    /**
     * During bucket creation we want to make sure Casper has the permission to decrypt the the encrypted DEK that is
     * just generated. so that the subsequent calls on the bucket, or create object in the bucket, get object from the
     * bucket will not fail due to Casper permission issue.
     * <p>
     * NOTE: we should also fire OBO call to kms to verify if the user has access to the kmsKey. In order to fire OBO
     * call to kms we need to pass user's header and then get a user token from identity. This is not supported yet,
     * kms internal SDK has no way to pass user header or token. OBO is not required during LA,
     */
    static void verifyDecryptKey(String kmsKeyId, EncryptionKey encryptionKey,
                                 DecidingKeyManagementService kms) {
        if (!Strings.isNullOrEmpty(kmsKeyId)) {
            EncryptionKey decryptedKey = kms.getRemoteKms()
                    .decryptKey(encryptionKey.getEncryptedDataEncryptionKey(), "");
            assert encryptionKey.equals(decryptedKey);
        }
    }

    public CopyObjectResult s3CopyObjectToSelf(RoutingContext context,
                                               AuthenticationInfo authInfo,
                                               String namespace,
                                               String bucketName,
                                               CopyObjectDetails copyObjectDetails) {
        final String objectName = copyObjectDetails.getSourceKey();
        final Map<String, String> requestMetadata = copyObjectDetails.getMetadata();

        Preconditions.checkNotNull(authInfo, "AuthInfo cannot be null");
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(bucketName);
        Preconditions.checkNotNull(requestMetadata, "Metadata argument cannot be be null");
        Validator.validateBucket(bucketName);
        Validator.validateObjectName(objectName);
        Validator.validateMetadata(requestMetadata, jsonSerializer);

        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope rootScope = commonContext.getMetricScope();
        final MetricScope childScope = rootScope.child("backend:s3CopyObjectToSelf");
        HttpPathHelpers.logPathParameters(rootScope, namespace, bucketName, objectName);
        final WSTenantBucketInfo bucket =
                bucketBackend.getBucketMetadataWithCache(context, childScope, namespace, bucketName, Api.V2);

        final WSRequestContext wsContext = WSRequestContext.get(context);
        wsContext.setBucketOcid(bucket.getOcid());
        wsContext.setBucketCreator(bucket.getCreationUser());
        wsContext.setBucketLoggingStatus(BucketOptionsUtil
                .getBucketLoggingStatusFromOptions(bucket.getOptions()));
        //S3 document says you should have WRITE permission on destination bucket.
        authorizeOperationForPermissions(childScope, context, authorizer, authInfo, bucket,
                CasperOperation.REPLACE_OBJECT_METADATA, CasperPermission.OBJECT_OVERWRITE);

        ReplicationEnforcer.throwIfReadOnly(context, bucket);

        // Need to decide correct number for max retries for update Object metadata.
        // need to retry as update object metadata should not fail.
        // s3 operation to copy object to itself (to update metadata) doesn't throw 409.
        // max duration is set to 10 seconds as this operation does 2 db operation under retry
        final RetryPolicy retryPolicy = new RetryPolicy()
                .retryOn(ConcurrentObjectModificationException.class)
                .withBackoff(10, 1000, TimeUnit.MILLISECONDS)
                .withJitter(0.25)
                .withMaxDuration(10000, TimeUnit.MILLISECONDS)
                .withMaxRetries(3);

        try {
            final Pair<String, Instant> result = Failsafe.with(retryPolicy).get(() -> {
                Optional<WSStorageObject> storageObject = getObject(context, bucket, objectName);
                if (!storageObject.isPresent()) {
                    throw new ObjectNotFoundException("Object not found");
                }
                final WSStorageObject wsStorageObject = storageObject.get();
                final ObjectMetadata objMeta =
                        BackendConversions.wsStorageObjectSummaryToObjectMetadata(wsStorageObject, kms);
                // the conditional headers can be done at beginning handler or here.
                // this is done here to avoid one more db call before and do it here again inside retry loop.
                S3HttpHelpers.checkConditionalHeadersForCopy(context.request(), copyObjectDetails, objMeta);
                final Map<String, String> newMetadata;
                final Map<String, String> contentMetadata = wsStorageObject.getMetadata(kms).entrySet().stream()
                        .filter(entry -> HttpHeaderHelpers.PRESERVED_CONTENT_HEADERS_LOOKUP.containsKey(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                newMetadata = MetadataUpdater.updateMetadata(false, contentMetadata, requestMetadata);
                wsContext.addOldState("metadata", wsStorageObject.getMetadata(kms));
                wsContext.addNewState("metadata", newMetadata);
                Validator.validateMetadata(newMetadata, jsonSerializer);
                final EncryptionKey encryptionKey = wsStorageObject.getEncryptedEncryptionKey().getEncryptionKey(kms);
                final String encryptedMetadata = CryptoUtils.encryptMetadata(newMetadata, encryptionKey);
                final Instant timeUpdated = MetricScope.timeScope(childScope, "objectMds:updateObjectMetadata",
                        innerScope -> {
                            UpdateObjectMetadataRequest request = UpdateObjectMetadataRequest.newBuilder()
                                    .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                    .setObjectKey(objectKey(objectName,
                                            namespace,
                                            bucketName,
                                            bucket.getKey().getApi()))
                                    .setIfMatchEtag("")
                                    .setNewEtag(CommonUtils.generateETag())
                                    .setNewMetadata(encryptedMetadata)
                                    .build();
                                UpdateObjectMetadataResponse response = BackendHelper.invokeMds(
                                        MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_UPDATE_OBJECT_METADATA,
                                        false, () -> objectClient.execute(
                                                c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                                        .updateObjectMetadata(request)));
                            return TimestampUtils.toInstant(response.getUpdateTime());
                        });
                wsContext.setEtag(objMeta.getETag());
                ServiceLogsHelper.logServiceEntry(context);
                // S3 doesn't change ETag on update metadata
                return new Pair<>(objMeta.getChecksum().getQuotedHex(), timeUpdated);
            });
            return new CopyObjectResult(result.getFirst(), Date.from(result.getSecond()));
        } catch (IfMatchException ex) {
            throw new UnexpectedEntityTagException("The entity tags do not match");
        } catch (ConcurrentObjectModificationException ex) {
            throw new ConcurrentObjectModificationException(
                    "The object '" + objectName + "' in bucket '" + bucketName +
                            "' was concurrently modified while being copied," +
                            " so the copy operation has failed.", ex);
        } catch (ObjectNotFoundException ex) {
            throw new ObjectNotFoundException(
                    "The object '" + objectName + "' in bucket '" + bucketName +
                            " is not found, so the copy operation has failed.", ex);
        }
    }

    public String getTenantName(RoutingContext context,
                                AuthenticationInfo authInfo,
                                Optional<String> tenantId) {
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        if (authInfo == AuthenticationInfo.ANONYMOUS_USER) {
            throw new NotAuthenticatedException("Get namespace is limited to authenticated users");
        }
        Optional<Tenant> tenant;

        if (tenantId.isPresent()) {
            try {
                OCIDParser.fromString(tenantId.get());
            } catch (InvalidOCIDException ex) {
                throw new InvalidCompartmentIdException("The compartment ID parameter must be valid.");
            }

            Optional<AuthorizationResponse> responseOptional = authorizer.authorize(
                    wsRequestContext.get(context),
                    authInfo,
                    null,
                    null,
                    tenantId.get(),
                    null,
                    CasperOperation.GET_NAMESPACE,
                    null,
                    false, // either TENANCY_INSPECT or OBJECTSTORAGE_NAMESPACE_READ
                    false, // Tenancy not deleted
                    CasperPermission.TENANCY_INSPECT,
                    CasperPermission.OBJECTSTORAGE_NAMESPACE_READ);
            //not empty if the authorization succeeded, empty otherwise.
            if (!responseOptional.isPresent()) {
                throw new NoSuchCompartmentIdException("Either the Tenant with  TenantID '" + tenantId.get() +
                        "' does not exist  or you are not authorized to access it");
            }
            tenant = metadataClient.getTenantByCompartmentId(tenantId.get(), ResourceLimiter.UNKNOWN_NAMESPACE);
        } else {
            tenant = metadataClient.getTenantByCompartmentId(
                    authInfo.getMainPrincipal().getTenantId(), ResourceLimiter.UNKNOWN_NAMESPACE);
        }
        if (!tenant.isPresent()) {
            //This should never occur if the user has authenticated.
            throw new NoSuchNamespaceException("Either the namespace" +
                    " does not exist or you are not authorized to view it");
        }
        wsRequestContext.setCompartmentID(tenant.get().getId());
        return tenant.get().getNamespace();
    }

    /**
     * a version of ListObject that returns SwiftObjectSummaryCollection
     * listObjectSwift is only used in com/oracle/pic/casper/webserver/api/swift/ListObjectsHandler.java
     * since SwiftContainer.fromBucket takes in both bucketName and BucketMetadata
     */
    public SwiftObjectSummaryCollection listSwiftObjects(
        RoutingContext context,
        AuthenticationInfo authInfo,
        List<ObjectProperties> properties,
        int limit,
        String namespace,
        String bucketName,
        @Nullable Character delimiter,
        @Nullable String prefix,
        @Nullable String startWith,
        @Nullable String endBefore) {
        Preconditions.checkNotNull(authInfo);
        Validator.validateBucket(bucketName);

        if (prefix != null && Validator.isUtf8EncodingTooLong(prefix, Validator.MAX_ENCODED_STRING_LENGTH)) {
            throw new IllegalArgumentException("Object prefix is too long");
        }

        if (startWith != null && Validator.isUtf8EncodingTooLong(startWith, Validator.MAX_ENCODED_STRING_LENGTH)) {
            throw new IllegalArgumentException("Object key is too long");
        }

        final Pair<ObjectSummaryCollection, WSTenantBucketInfo> pair = listObjectsHelper(
            context,
            bucket -> authorizer.authorize(WSRequestContext.get(context),
                authInfo,
                bucket.getNamespaceKey(),
                bucket.getBucketName(),
                bucket.getCompartment(),
                bucket.getPublicAccessType(),
                CasperOperation.LIST_OBJECTS,
                bucket.getKmsKeyId().orElse(null),
                true,
                bucket.isTenancyDeleted(),
                CasperPermission.OBJECT_INSPECT).isPresent(),
            properties,
            limit,
            namespace,
            bucketName,
            delimiter,
            prefix,
            startWith,
            endBefore);

        final ObjectSummaryCollection objectSummaryCollection = pair.getFirst();
        final WSTenantBucketInfo bucket = pair.getSecond();

        WSRequestContext.get(context).setBucketOcid(bucket.getOcid());

        return new SwiftObjectSummaryCollection(
            objectSummaryCollection.getObjects(),
            bucket.getBucketName(),
            bucket.getMetadata(kms),
            objectSummaryCollection.getPrefixes(),
            objectSummaryCollection.getNextObject(),
            objectSummaryCollection.getSecondLastObjectInQuery(),
            objectSummaryCollection.getLastObjectInQuery());
    }

    private GetObjectRequest getObjectRequest(WSTenantBucketInfo bucket, String objName) {
        return GetObjectRequest.newBuilder()
                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                .setObjectKey(MdsObjectKey.newBuilder()
                        .setBucketKey(bucketKey(bucket.getNamespaceKey().getName(), bucket.getBucketName(),
                                bucket.getNamespaceKey().getApi()))
                        .setObjectName(objName)
                        .build())
                .build();
    }

    protected Optional<WSStorageObject> getObject(RoutingContext context, WSTenantBucketInfo bucket, String objName) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final GetObjectRequest get = getObjectRequest(bucket, objName);
        try {
            // temp hack to get around the fact that objects can be 10TB
            GetObjectResponse response = BackendHelper.invokeMds(MdsMetrics.OBJECT_MDS_BUNDLE,
                    MdsMetrics.OBJECT_MDS_GET_OBJECT,
                    true,
                    () -> objectClient.execute(c -> getClientWithOptions(c,
                            requestDeadlineSeconds,
                            commonContext,
                            getObjectMaxMessageBytes).getObject(get)));
            return Optional.of(BackendConversions.mdsObjectToWSStorageObject(response.getObject()));
        } catch (NoSuchObjectException e) {
            return Optional.empty();
        }
    }

    private HeadObjectRequest headObjectRequest(WSTenantBucketInfo bucket, String objName) {
        return HeadObjectRequest.newBuilder()
            .setBucketToken(bucket.getMdsBucketToken().toByteString())
            .setObjectKey(MdsObjectKey.newBuilder()
                .setBucketKey(bucketKey(bucket.getNamespaceKey().getName(), bucket.getBucketName(),
                    bucket.getNamespaceKey().getApi()))
                .setObjectName(objName)
                .build())
            .build();
    }

    protected Optional<WSStorageObjectSummary> headObject(
        RoutingContext context,
        WSTenantBucketInfo bucket,
        String objName) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final HeadObjectRequest head = headObjectRequest(bucket, objName);
        try {
            HeadObjectResponse response = BackendHelper.invokeMds(
                MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_HEAD_OBJECT,
                true, () -> objectClient.execute(
                    c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                        .headObject(head)));
            return Optional.of(BackendConversions.mdsObjectSummaryToWSStorageObjectSummary(response.getObject()));
        } catch (NoSuchObjectException e) {
            return Optional.empty();
        }
    }

    private Optional<Date> mdsDeleteObject(RoutingContext context, WSTenantBucketInfo bucket, String objectName,
                                           String expectedEtag) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        try {
            DeleteObjectRequest request = DeleteObjectRequest.newBuilder()
                    .setBucketToken(bucket.getMdsBucketToken().toByteString())
                    .setObjectKey(objectKey(objectName,
                            bucket.getNamespaceKey().getName(),
                            bucket.getBucketName(),
                            bucket.getKey().getApi()))
                    .setIfMatchEtag(StringUtils.defaultString(expectedEtag))
                    .build();
            DeleteObjectResponse response = BackendHelper.invokeMds(
                    MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_DELETE_OBJECT,
                    false, () -> objectClient.execute(
                            c -> getClientWithOptions(c, requestDeadlineSeconds, commonContext)
                                    .deleteObject(request)));
            return Optional.of(TimestampUtils.toDate(response.getDeleteTime()));
        } catch (NoSuchObjectException e) {
            return Optional.empty();
        }
    }

    protected static MdsBucketKey bucketKey(String ns, String bucket, Api api) {
        return MdsBucketKey.newBuilder()
                .setNamespaceName(ns)
                .setBucketName(bucket)
                .setApi(MdsTransformer.toMdsNamespaceApi(api))
                .build();
    }

    public static MdsObjectKey objectKey(String objectName, String ns, String bucket, Api api) {
        return MdsObjectKey.newBuilder()
                .setBucketKey(bucketKey(ns, bucket, api))
                .setObjectName(objectName)
                .build();
    }

    private MdsUploadKey mdsUploadKey(String uploadId, MdsObjectKey objectKey) {
        return MdsUploadKey.newBuilder()
                .setUploadId(uploadId)
                .setObjectKey(objectKey)
                .build();
    }

    /**
     * Calculate the MD5-of-MD5 hash which is used for multipart uploads.
     * @param md5List A list of base-64 encoded MD5 hashes.
     * @return The MD5-of-MD5 of the MD5 hashes
     */
    static String md5OfMd5(Collection<String> md5List) {
        final int numHashes = md5List.size();

        // First we decode the MD5 hashes and concatenate their bytes.
        // We can preallocate the buffer (an MD5 hash is 128 bits, i.e.
        // 16 bytes
        final ByteBuffer concatenatedMd5Bytes = ByteBuffer.allocate(16 * numHashes);
        for (String md5: md5List) {
            concatenatedMd5Bytes.put(Base64.getDecoder().decode(md5));
        }

        // Next we calculate an MD5 of the concatenated MD5 hashes and
        // base-64 encode the new hash value.
        final String encodedMd5OfMd5;
        try {
            final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            final byte[] md5Bytes = messageDigest.digest(concatenatedMd5Bytes.array());
            encodedMd5OfMd5 = Base64.getEncoder().encodeToString(md5Bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        return encodedMd5OfMd5;
    }
}
