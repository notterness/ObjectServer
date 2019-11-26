package com.oracle.pic.casper.webserver.api.v2;

import com.codahale.metrics.Histogram;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.oracle.oci.casper.jopenssl.CryptoMessageDigest;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.common.config.v2.ServicePrincipalConfiguration;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.encryption.DigestUtils;
import com.oracle.pic.casper.common.encryption.EncryptionConstants;
import com.oracle.pic.casper.common.encryption.EncryptionKey;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.encryption.store.Secrets;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.AuthorizationException;
import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.common.exceptions.InvalidContentLengthException;
import com.oracle.pic.casper.common.exceptions.InvalidDigestException;
import com.oracle.pic.casper.common.exceptions.MultipartUploadNotFoundException;
import com.oracle.pic.casper.common.exceptions.StorageLimitExceededException;
import com.oracle.pic.casper.common.host.HostInfo;
import com.oracle.pic.casper.common.json.JsonSerializer;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.metrics.MetricsBundle;
import com.oracle.pic.casper.common.model.BucketStorageTier;
import com.oracle.pic.casper.common.model.ObjectStorageTier;
import com.oracle.pic.casper.common.model.Stripe;
import com.oracle.pic.casper.common.rest.CommonHeaders;
import com.oracle.pic.casper.common.util.AugmentedRequestIdFactory;
import com.oracle.pic.casper.mds.MdsEncryptionKey;
import com.oracle.pic.casper.mds.MdsObjectKey;
import com.oracle.pic.casper.mds.common.exceptions.MdsException;
import com.oracle.pic.casper.mds.common.grpc.TimestampUtils;
import com.oracle.pic.casper.mds.loadbalanced.MdsExecutor;
import com.oracle.pic.casper.mds.object.AbortPutRequest;
import com.oracle.pic.casper.mds.object.BeginChunkRequest;
import com.oracle.pic.casper.mds.object.BeginPartChunkRequest;
import com.oracle.pic.casper.mds.object.BeginPutRequest;
import com.oracle.pic.casper.mds.object.BeginPutResponse;
import com.oracle.pic.casper.mds.object.FinishChunkRequest;
import com.oracle.pic.casper.mds.object.FinishPartChunkRequest;
import com.oracle.pic.casper.mds.object.FinishPutRequest;
import com.oracle.pic.casper.mds.object.MdsDigest;
import com.oracle.pic.casper.mds.object.MdsEtagType;
import com.oracle.pic.casper.mds.object.ObjectServiceGrpc;
import com.oracle.pic.casper.mds.object.exception.MdsUploadNotFoundException;
import com.oracle.pic.casper.mds.object.exception.ObjectExceptionClassifier;
import com.oracle.pic.casper.metadata.utils.CryptoUtils;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.CasperTransactionId;
import com.oracle.pic.casper.objectmeta.CasperTransactionIdFactory;
import com.oracle.pic.casper.objectmeta.CasperTransactionIdFactoryImpl;
import com.oracle.pic.casper.objectmeta.ObjectId;
import com.oracle.pic.casper.storageclient.AthenaVolumeStorageClient;
import com.oracle.pic.casper.storageclient.SCRequestContext;
import com.oracle.pic.casper.storageclient.models.PutResult;
import com.oracle.pic.casper.storageclient.models.VolumeStorageContext;
import com.oracle.pic.casper.volumemeta.VolumeMetadataClientCache;
import com.oracle.pic.casper.volumemeta.util.VolumeMetadataUtils;
import com.oracle.pic.casper.volumemgmt.VolumeAndVonPicker;
import com.oracle.pic.casper.volumemgmt.model.PickerContext;
import com.oracle.pic.casper.volumemgmt.model.VolumeMetaAndVon;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.AuthorizationResponse;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.backend.BackendConversions;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.backend.MdsMetrics;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;
import com.oracle.pic.casper.webserver.api.common.ExtractsRequestMetadata;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpPathHelpers;
import com.oracle.pic.casper.webserver.api.common.MdsClientHelper;
import com.oracle.pic.casper.webserver.api.common.MdsTransformer;
import com.oracle.pic.casper.webserver.api.common.RecycleBinHelper;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.model.exceptions.UnauthorizedHeaderException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoContext;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.replication.ReplicationEnforcer;
import com.oracle.pic.casper.webserver.api.sg.ServiceGateway;
import com.oracle.pic.casper.webserver.api.usage.UsageCache;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.webserver.auth.limits.Limits;
import com.oracle.pic.casper.webserver.server.MdsClients;
import com.oracle.pic.casper.webserver.server.WebServerAuths;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.util.CommonUtils;
import com.oracle.pic.casper.webserver.util.Validator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.StatusRuntimeException;
import io.vertx.core.http.HttpHeaders;
import org.glassfish.jersey.server.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static javax.measure.unit.NonSI.BYTE;

@Path("n/{namespace}/b/{bucket}")
public class ObjectResource {

    private final Histogram totalChunkTime = Metrics.REGISTRY.histogram("webserver.put.chunk.total.time");

    private static final Logger LOG = LoggerFactory.getLogger(ObjectResource.class);

    private static final ImmutableSet<CasperPermission> ALL_PUT_OBJECT_PERMISSIONS =
            ImmutableSet.of(CasperPermission.OBJECT_OVERWRITE, CasperPermission.OBJECT_CREATE);

    /**
     * Clients are not necessarily required to supply a user-agent when they perform requests.
     * The way embargo works however is *null* is a match all value.
     * In order to have the ability to restrict those without user-agents we actually supply them a default one
     * rather than null.
     */
    private static final String DEFAULT_USER_AGENT = "DEFAULT_USER_AGENT";

    private final CasperConfig casperConfig;
    private final TrafficController controller;
    private final List<String> servicePrincipals;
    private final JsonSerializer jsonSerializer;
    private final WebServerAuths auths;
    private final TrafficRecorder trafficRecorder;
    private final BucketBackend bucketBackend;
    private final CasperTransactionIdFactory txnIdFactory;
    private final UsageCache usageCache;
    private final MdsExecutor<ObjectServiceGrpc.ObjectServiceBlockingStub> objectClient;
    private final VolumeAndVonPicker volVonPicker;
    private final long objectDeadlineSeconds;
    private final DecidingKeyManagementService kms;
    private final VolumeMetadataClientCache volMetaCache;
    private final AthenaVolumeStorageClient volumeStorageClient;

    public ObjectResource(CasperConfig casperConfig, TrafficController controller, JsonSerializer jsonSerializer,
                          WebServerAuths auths, TrafficRecorder trafficRecorder, BucketBackend bucketBackend,
                          MdsClients mdsClients, VolumeAndVonPicker volumeAndVonPicker, DecidingKeyManagementService kms,
                          VolumeMetadataClientCache volMetaCache, AthenaVolumeStorageClient volumeStorageClient) {
        this.casperConfig = casperConfig;
        this.controller = controller;
        this.servicePrincipals = casperConfig.getWebServerConfigurations().getServicePrincipalConfigurationList().stream()
                .map(ServicePrincipalConfiguration::getTenantId).collect(Collectors.toList());
        this.jsonSerializer = jsonSerializer;
        this.auths = auths;
        this.trafficRecorder = trafficRecorder;
        this.bucketBackend = bucketBackend;
        this.txnIdFactory = new CasperTransactionIdFactoryImpl();

        this.usageCache = new UsageCache(
                casperConfig.getWebServerConfigurations().getUsageConfiguration(),
                mdsClients.getOperatorMdsExecutor(),
                mdsClients.getOperatorDeadline());
        this.objectClient = mdsClients.getObjectMdsExecutor();
        this.volVonPicker = volumeAndVonPicker;
        this.objectDeadlineSeconds = mdsClients.getObjectRequestDeadline().getSeconds();
        this.kms = kms;
        this.volMetaCache = volMetaCache;
        this.volumeStorageClient = volumeStorageClient;
    }

    @Path("o/{object}")
    @PUT
    public Response createObject(@PathParam("namespace") String namespace,
                                 @PathParam("bucket") String bucket,
                                 @PathParam("object") String object,
                                 InputStream objectInputStream,
                                 @Context ContainerRequest request) {
        enterEmbargo(namespace, bucket, object);

        validate(namespace, request.getHeaders(), request.getPath(true));

        final String etagRound = request.getHeaderString(CasperApiV2.POLICY_ROUND_HEADER);
        final String opcRequestId = AugmentedRequestIdFactory.getFactory().newUniqueId();
        final String md5Override = request.getHeaderString(CasperApiV2.MD5_OVERRIDE_HEADER);
        final String etagOverride = request.getHeaderString(CasperApiV2.ETAG_OVERRIDE_HEADER);
        final Integer partCountOverride = parsePartCountOverride(request.getHeaderString(CasperApiV2.PART_COUNT_OVERRIDE_HEADER));


        final String realIp = request.getHeaderString(CommonHeaders.X_REAL_IP.toString());
        final MetricScope rootScope = buildMetricScope(
                request.getMethod(), realIp, request.getRequestUri().toString(), etagRound, opcRequestId, md5Override, etagOverride);

        //TODO: javax.ws.rs.core usage in 'common' code
        final Map<String, String> metadata = HttpHeaderHelpers.getUserMetadataHeaders(request, request.getPath(true));
        final ObjectMetadata objMeta = HttpContentHelpers.readObjectMetadata(
                request, namespace, bucket, object, metadata);
        // Acquire can throw an exception(with err code of 503 or 429) if the request couldnt be accepted
        controller.acquire(objMeta.getNamespace(), TrafficRecorder.RequestType.PutObject, objMeta.getSizeInBytes());
        Validator.validateBucket(objMeta.getBucketName());
        Validator.validateObjectName(objMeta.getObjectName());
        Validator.validateMetadata(objMeta.getMetadata(), jsonSerializer);

        AuthenticationInfo authInfo = auths.getAsyncAuthenticator().authenticatePutObject(request, request.getRequestUri().toString(), namespace, request.getMethod(), rootScope);
        if ((md5Override != null || etagOverride != null || partCountOverride != null ||
                etagRound != null) &&
                !(authInfo.getServicePrincipal().isPresent() &&
                        servicePrincipals.contains(authInfo.getServicePrincipal().get().getTenantId()))) {
            throw new UnauthorizedHeaderException();
        }


        HttpPathHelpers.logPathParameters(
                rootScope, objMeta.getNamespace(), objMeta.getBucketName(), objMeta.getObjectName());

        trafficRecorder.requestStarted(
                objMeta.getNamespace(), opcRequestId, TrafficRecorder.RequestType.PutObject, objMeta.getSizeInBytes());
        WSTenantBucketInfo bucketInfo = getBucketMetadata(rootScope, objMeta.getNamespace(), objMeta.getBucketName(), Api.V2);

        // Blocking creation of objects in the recycle bin
        if (RecycleBinHelper.isBinObject(bucketInfo, objMeta.getObjectName())) {
            throw new AuthorizationException(V2ErrorCode.FORBIDDEN.getStatusCode(),
                    V2ErrorCode.FORBIDDEN.getErrorName(),
                    "Not authorized to write or modify objects in the Recycle Bin");
        }

        final MetricScope writeObjectScope = rootScope.child("Backend:writeObject");
        Set<CasperPermission> allowedPermissions = checkAuthAndFetchPermissions(namespace, rootScope, authInfo,
                bucketInfo, writeObjectScope, request.getMethod(), request.getRequestUri().toString(), request.getHeaders());
        ReplicationEnforcer.throwIfReadOnlyButNotReplicationScope(request, bucketInfo);

        final Optional<Tenant> resourceTenant =
                auths.getMetadataClient().getTenantByCompartmentId(bucketInfo.getCompartment(), namespace);

        // No need to check storage limit for internal bucket.
        // If we fail to get destination tenant ocid from identity, then fail open.
        if (resourceTenant.isPresent() && !BucketBackend.InternalBucketType.isInternalBucket(bucketInfo.getBucketName())) {
            verifyStorageCapacityIsEnough(namespace, resourceTenant.get().getId(), objMeta.getSizeInBytes());
        }

        final ObjectStorageTier objectStorageTier = ObjectStorageTier.STANDARD;
        final CasperTransactionId txnId = txnIdFactory.newCasperTransactionId("writeObject");
        final boolean isSingleChunkObject =
                (objMeta.getSizeInBytes() <= casperConfig.getWebServerConfigurations().getApiConfiguration().getChunkSize().longValue(BYTE)) &&
                        casperConfig.getWebServerConfigurations().isSingleChunkOptimizationEnabled();
        VolumeMetaAndVon volMetaAndVon = getVolumeMetaAndVon(opcRequestId, rootScope, objMeta, isSingleChunkObject);
        BeginPutRequest putRequest = constructPutRequest(objMeta, bucketInfo, objectStorageTier, txnId, volMetaAndVon);

        //FIXME: rename / figure out how to better convey meaning of this call
        final String putResponseObjectId = executeRequestCheckPermissionsSendContinueReturnObjId(
                opcRequestId, bucketInfo, writeObjectScope, allowedPermissions, putRequest);


        long totalTransferred = 0;
        final long chunkSize = casperConfig.getWebServerConfigurations().getApiConfiguration().getChunkSize().longValue(BYTE);
        final EncryptionKey encKey = getEncryption(bucketInfo.getKmsKeyId().orElse(null), kms);
        CryptoMessageDigest objectDigestCalculator = DigestUtils.messageDigestFromAlgorithm(DigestAlgorithm.MD5);
        try {
            totalTransferred = handleInputStream(objectInputStream, opcRequestId, rootScope, objMeta, bucketInfo, txnId, isSingleChunkObject, volMetaAndVon, putResponseObjectId, totalTransferred, (int) chunkSize, encKey, objectDigestCalculator);
        } catch (Exception e) {
            buildAbortRequestAndInvokeMds(opcRequestId, rootScope, objMeta, bucketInfo, txnId, putResponseObjectId);
        } finally {
            rootScope.annotate("totalBytesTransferred", totalTransferred);
        }
        Digest objectDigest = new Digest(DigestAlgorithm.MD5, objectDigestCalculator.digest());
        String objectDigestString = objectDigest.getBase64Encoded();
        String encryptedMetadata = CryptoUtils.encryptMetadata(objMeta.getMetadata(), encKey);
        final String ifMatchEtag = request.getHeaderString(HttpHeaders.IF_MATCH.toString());
        final String ifNoneMatchEtag = request.getHeaderString(HttpHeaders.IF_NONE_MATCH.toString());
        FinishPutRequest finishPutRequest = constructFinishPutRequest(md5Override, etagOverride, partCountOverride, objMeta, bucketInfo, allowedPermissions, objectStorageTier, txnId, isSingleChunkObject, putResponseObjectId, totalTransferred, encKey, objectDigestString, encryptedMetadata, ifMatchEtag, ifNoneMatchEtag);

        sendRequestWithMDS(opcRequestId, finishPutRequest);

        final Optional<String> expectedMD5 = ChecksumHelper.getContentMD5Header(request);
        final Digest expectedDigest = expectedMD5.map(m->Digest.fromBase64Encoded(DigestAlgorithm.MD5, m)).orElse(null);
        if (expectedDigest != null && objectDigest != null) {
            if (!Objects.equals(expectedDigest, objectDigest)) {
                throw new InvalidDigestException(expectedDigest, objectDigest);
            }
        }
        if (totalTransferred != objMeta.getSizeInBytes()) {
            throw new InvalidContentLengthException(
                    "The size of the body (" + totalTransferred +
                            " bytes) does not match the expected content length (" + objMeta.getSizeInBytes() + " bytes)");
        }

        return Response.ok().build();
    }

    private void sendRequestWithMDS(String opcRequestId, FinishPutRequest finishPutRequest) {
        BackendConversions.mdsObjectSummaryToObjectMetadata(
                invokeMds(
                        MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_FINISH_PUT,
                        false, () -> objectClient.execute(
                                c -> Backend.getClientWithOptions(c, objectDeadlineSeconds, opcRequestId))
                                .finishPut(finishPutRequest).getSummary()), kms);
    }

    private FinishPutRequest constructFinishPutRequest(String md5Override, String etagOverride, Integer partCountOverride, ObjectMetadata objMeta, WSTenantBucketInfo bucketInfo, Set<CasperPermission> allowedPermissions, ObjectStorageTier objectStorageTier, CasperTransactionId txnId, boolean isSingleChunkObject, String putResponseObjectId, long totalTransferred, EncryptionKey encKey, String objectDigestString, String encryptedMetadata, String ifMatchEtag, String ifNoneMatchEtag) {
        final FinishPutRequest.Builder builder = FinishPutRequest.newBuilder()
                .setObjectId(putResponseObjectId)
                .setBucketToken(bucketInfo.getMdsBucketToken().toByteString())
                .setObjectKey(Backend.objectKey(objMeta.getObjectName(), bucketInfo.getNamespaceKey().getName(),
                        bucketInfo.getBucketName(), bucketInfo.getNamespaceKey().getApi()))
                .setStorageTier(MdsTransformer.toObjectStorageTier(objectStorageTier))
                .setTransactionId(txnId.toString())
                .setMetadata(encryptedMetadata)
                .setMd5(md5Override == null ? objectDigestString : md5Override)
                .setEtag(etagOverride == null ? CommonUtils.generateETag() : etagOverride)
                .setEncryptionKey(MdsTransformer.toMdsEncryptionKey(encKey))
                .setEtagType(MdsEtagType.ET_ETAG)
                .setObjectOverwriteAllowed(allowedPermissions.contains(CasperPermission.OBJECT_OVERWRITE));

        if (isSingleChunkObject) {
            builder.setChunkSizeInBytes(Int64Value.of(totalTransferred))
                    .setDigest(MdsTransformer.toMdsDigest(DigestUtils.nullDigest()))
                    .setChunkEncryptionKey(MdsTransformer.toMdsEncryptionKey(encKey))
                    .setSequenceNum(Int64Value.of(0L));
        }

        //FIXME: I moved this way down the method.  Figure out why it's okay that it's this far down, or if it's not okay, what we missed in the first pass
        // The object storage tier will always be set to STANDARD but when we read the object
        // and if the time we read it is after the archive period we set for this object,
        // we will treat this object as an archived object when we read it.
        if (bucketInfo.getStorageTier() == BucketStorageTier.Archive) {
            final Duration archiveMinRetention = casperConfig.getWebServerConfigurations().getArchiveConfiguration().getMinimumRetentionPeriodForArchives();
            builder.setMinRetention(TimestampUtils.toProtoDuration(archiveMinRetention));
            final Duration archiveNewObjPeriod = casperConfig.getWebServerConfigurations().getArchiveConfiguration().getArchivePeriodForNewObject();
            builder.setTimeToArchive(TimestampUtils.toProtoDuration(archiveNewObjPeriod));
        }

        if (ifMatchEtag != null) {
            builder.setIfMatchEtag(ifMatchEtag);
        }
        if (ifNoneMatchEtag != null) {
            builder.setIfNoneMatchEtag(ifNoneMatchEtag);
        }

        if (partCountOverride != null) {
            builder.setMultipartOverride(true);
            builder.setPartcountOverride(partCountOverride);
        }

        return builder.build();
    }

    //FIXME: how should this work for the nio side? per-ByteBuffer, or something?
    private long handleInputStream(InputStream objectInputStream, String opcRequestId, MetricScope rootScope, ObjectMetadata objMeta, WSTenantBucketInfo bucketInfo, CasperTransactionId txnId, boolean isSingleChunkObject, VolumeMetaAndVon volMetaAndVon, String putResponseObjectId, long totalTransferred, int chunkSize, EncryptionKey encKey, CryptoMessageDigest objectDigestCalculator) {
        int seqNum = 0;
        for (Chunk chunk : Chunk.getChunksFromStream(objectInputStream, chunkSize)) {
            if (chunk.getSize() == 0) continue;
            totalTransferred += chunk.getSize();
            objectDigestCalculator.update(chunk.getData());
            processChunk(chunk, encKey, isSingleChunkObject, volMetaAndVon, opcRequestId, null, rootScope, bucketInfo, objMeta.getObjectName(), new ObjectId(putResponseObjectId), txnId, seqNum);
            seqNum++;
        }
        return totalTransferred;
    }

    private void buildAbortRequestAndInvokeMds(String opcRequestId, MetricScope rootScope, ObjectMetadata objMeta, WSTenantBucketInfo bucketInfo, CasperTransactionId txnId, String putResponseObjectId) {
        final MetricScope innerScope = rootScope.child("abort");
        final AbortPutRequest abortRequest = AbortPutRequest.newBuilder()
                .setBucketToken(bucketInfo.getMdsBucketToken().toByteString())
                .setObjectKey(Backend.objectKey(objMeta.getObjectName(), bucketInfo.getNamespaceKey().getName(),
                        bucketInfo.getBucketName(), bucketInfo.getNamespaceKey().getApi()))
                .setTransactionId(txnId.toString())
                .setObjectId(putResponseObjectId)
                .build();
        try {
            invokeMds(MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_ABORT_PUT,
                    false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                            c, objectDeadlineSeconds, opcRequestId).abortPut(abortRequest)));
        } catch (Exception ex) {
            innerScope.fail(ex);
            LOG.warn("Failed to abort a Casper transaction with id {} on object {}", txnId,
                    objMeta.getObjectName(), ex);
        } finally {
            innerScope.end();
        }
    }

    private String executeRequestCheckPermissionsSendContinueReturnObjId(String opcRequestId, WSTenantBucketInfo bucketInfo, MetricScope writeObjectScope, Set<CasperPermission> allowedPermissions, BeginPutRequest putRequest) {
        Function<ObjectServiceGrpc.ObjectServiceBlockingStub, BeginPutResponse> getClient = c -> Backend.getClientWithOptions(
                c, objectDeadlineSeconds, opcRequestId).beginPut(putRequest);
        Callable<BeginPutResponse> execute = () -> objectClient.execute(getClient);
        Function<MetricScope, BeginPutResponse> invokeMds = innerScope -> invokeMds(
                MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_BEGIN_PUT,
                false, execute);

        final BeginPutResponse response = MetricScope.timeScopeF(writeObjectScope, "objectMds:beginPut", invokeMds);

        // Check the permissions the customer has. They need the OBJECT_CREATE permission if the object doesn't
        // exist yet, and the OBJECT_OVERWRITE permission if there'authorize already an existing object.
        // The check for OBJECT_CREATE is sufficient here in BeginPut. However, the OBJECT_OVERWRITE check is not
        // sufficient here because there could be a new object created by another operation after this point, so it
        if (!response.hasExistingObject() && !allowedPermissions.contains(CasperPermission.OBJECT_CREATE) ||
                response.hasExistingObject() && !allowedPermissions.contains(CasperPermission.OBJECT_OVERWRITE)) {
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketInfo.getBucketName(),
                    bucketInfo.getNamespaceKey().getName()));
        }

        // TODO: Our NIO implementation needs to handle the 100-continue after a successful "begin put".  Jersey has no solution for this.
        /*
        if (!beginPutResponse.hasExistingObject()) {
            curObjMeta = BackendConversions.mdsObjectSummaryToObjectMetadata(beginPutResponse.getExistingObject(), kms);
            HttpMatchHelpers.checkConditionalHeaders(
                    request, curObjMeta.map(ObjectMetadata::getETag).orElse(null));
            HttpContentHelpers.write100Continue(request, response);
        }*/
        return response.getObjectId();
    }

    private BeginPutRequest constructPutRequest(ObjectMetadata objMeta, WSTenantBucketInfo bucketInfo, ObjectStorageTier objectStorageTier, CasperTransactionId txnId, VolumeMetaAndVon volMetaAndVon) {
        final BeginPutRequest.Builder requestBuilder = BeginPutRequest.newBuilder()
                .setBucketToken(bucketInfo.getMdsBucketToken().toByteString())
                .setObjectKey(Backend.objectKey(
                        objMeta.getObjectName(),
                        bucketInfo.getNamespaceKey().getName(),
                        bucketInfo.getBucketName(),
                        bucketInfo.getNamespaceKey().getApi()))
                .setStorageTier(MdsTransformer.toObjectStorageTier(objectStorageTier))
                .setTransactionId(txnId.toString());
        if(volMetaAndVon == null) {
            requestBuilder.setVolumeId(Int64Value.of(volMetaAndVon.getVolumeMetadata().getVolumeId()))
                    .setVon(Int32Value.of(volMetaAndVon.getVon()))
                    .setSequenceNum(Int64Value.of(0L));
        }
        return requestBuilder.build();
    }

    private VolumeMetaAndVon getVolumeMetaAndVon(String opcRequestId, MetricScope rootScope, ObjectMetadata objMeta, boolean isSingleChunkObject) {
        VolumeMetaAndVon volMetaAndVon = null;
        if (isSingleChunkObject) {
            volMetaAndVon = MetricScope.timeScopeF(rootScope,
                    "pickVolume",
                    innerScope -> volVonPicker.getNextVon(
                            innerScope,
                            createDefaultPickerContext(objMeta.getSizeInBytes(),
                                    opcRequestId)));
        }
        return volMetaAndVon;
    }

//TODO: parameter formatting
    private Set<CasperPermission> checkAuthAndFetchPermissions(
            String namespace, MetricScope rootScope, AuthenticationInfo authInfo, WSTenantBucketInfo bucketInfo,
            MetricScope writeObjectScope, String method, String requestUri,MultivaluedMap<String, String> headers) {
        //TODO this used to use ExtractsRequestMedatadata.vcnIdFromRequest.  Determine whether we've lost functionality.
        String vcnId = headers.getFirst(CommonHeaders.X_VCN_ID.toString());
        String vcnDebugId = headers.getFirst(ServiceGateway.VCN_ID_CASPER_DEBUG_HEADER);
        final String userAgent = Optional.ofNullable(headers.getFirst(HttpHeaders.USER_AGENT.toString()))
                .orElse(DEFAULT_USER_AGENT);
        final EmbargoContext visa = auths.getEmbargo().enter(method, requestUri, userAgent, rootScope);
        Supplier<Optional<AuthorizationResponse>> authorizeFunction = authorize(namespace, rootScope, authInfo, bucketInfo, vcnId, vcnDebugId, visa);
        Optional<AuthorizationResponse> authorizationResponse = Utils.runWithScope(writeObjectScope, authorizeFunction, "authorizer");

        if (!authorizationResponse.isPresent()) {
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketInfo.getBucketName(),
                    bucketInfo.getNamespaceKey().getName()));
        }
        return authorizationResponse.get().getCasperPermissions();
    }

    private Supplier<Optional<AuthorizationResponse>> authorize(String namespace, MetricScope rootScope, AuthenticationInfo authInfo, WSTenantBucketInfo bucketInfo, String vcnId, String vcnDebugId, EmbargoContext visa) {
        return () -> {
                // Authorize putting a new object or overwriting an existing object. We call this with
                // the two permissions (create/overwrite) and authorizeAll = false. This means we will
                // authorized if either of the permissions are allowed. We need to do this because we
                // perform the authorization checks in two places. For create, we can check in BeginPut,
                // but for overwrite, we need to check in FinishPut. So after calling authorize, we need
                // to look at the AuthorizationResponse which contains the permissions that are actually
                // allowed. Then we can use that to perform the checks in BeginPut and FinishPut.
                return auths.getAuthorizer().authorize(
                        authInfo,
                        bucketInfo.getNamespaceKey(),
                        bucketInfo.getBucketName(),
                        bucketInfo.getCompartment(),
                        bucketInfo.getPublicAccessType(),
                        CasperOperation.PUT_OBJECT,
                        bucketInfo.getKmsKeyId().orElse(null),
                        false,
                        bucketInfo.isTenancyDeleted(), rootScope,
                        ALL_PUT_OBJECT_PERMISSIONS, visa, vcnId, vcnDebugId, namespace);
            };
    }

    private Integer parsePartCountOverride(String partCountOverrideHeader) {
        final Integer partCountOverride;
        try {
            partCountOverride = partCountOverrideHeader == null ? null : Integer.parseInt(partCountOverrideHeader);
        } catch (NumberFormatException e) {
            throw new BadRequestException("Cannot parse partCountOverrideHeader" + partCountOverrideHeader, e);
        }
        return partCountOverride;
    }

    private MetricScope buildMetricScope(String method, String realIP, String requestURI, String etagRound, String opcRequestId, String md5Override, String etagOverride) {
        final MetricScope rootScope = Utils.getMetricScope(method, realIP, requestURI, casperConfig.getCommonConfigurations().getTracingConfiguration(), opcRequestId);
        rootScope.annotate(CasperApiV2.MD5_OVERRIDE_HEADER, md5Override);
        rootScope.annotate(CasperApiV2.ETAG_OVERRIDE_HEADER, etagOverride);
        rootScope.annotate(CasperApiV2.POLICY_ROUND_HEADER, etagRound);
        return rootScope;
    }

    private void validate(String namespace, MultivaluedMap<String, String> headers, String path) {
        Validator.validateV2Namespace(namespace);
        HttpContentHelpers.negotiateStorageObjectContent(headers, 0,
                casperConfig.getWebServerConfigurations().getApiConfiguration().getMaxObjectSize().longValue(BYTE));
        HttpMatchHelpers.validateConditionalHeaders(headers, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.STAR_ONLY, path);
    }

    private void enterEmbargo(String namespace, String bucket, String object) {
        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.V2)
                .setOperation(CasperOperation.PUT_OBJECT)
                .setNamespace(namespace)
                .setBucket(bucket)
                .setObject(object)
                .build();
        auths.getEmbargoV3().enter(embargoV3Operation);
    }

    private void processChunk(Chunk chunk, EncryptionKey encKey, boolean isSingleChunk, VolumeMetaAndVon volumeMetaAndVon, String opcRequestId, String opcClientRequestId, MetricScope metricScope, WSTenantBucketInfo bucketInfo, String objName, ObjectId objId, CasperTransactionId txnId, long seqNum) {
        Chunk encryptedChunk = chunk.encrypt(encKey);
        if (isSingleChunk) {
            writeToStorageServer(
                    volumeMetaAndVon,
                    opcRequestId,
                    opcClientRequestId,
                    encryptedChunk,
                    metricScope);
        } else {
            long start = System.nanoTime();
            writeToStorageServerMultiChunk(bucketInfo, objName, encryptedChunk, txnId, objId, seqNum, metricScope, opcRequestId, opcClientRequestId, encKey);
            totalChunkTime.update(System.nanoTime() - start);
        }
    }

    private PutResult writeToStorageServerMultiChunk(
            WSTenantBucketInfo bucket,
            String objectName,
            Chunk chunk,
            @Nullable CasperTransactionId txnId,
            ObjectId objId,
            long seqNum,
            MetricScope scope,
            String opcRequestId,
            String opcClientRequestId,
            EncryptionKey encryptionKey) {
        final boolean isMultiPart = txnId == null;

        final VolumeMetaAndVon volMetaAndVon = MetricScope.timeScopeF(scope, "pickVolume",
                innerScope -> volVonPicker.getNextVon(
                        innerScope, createDefaultPickerContext(chunk.getSize(), opcRequestId)));

        MetricScope.timeScopeC(scope, "objectMds:beginChunk",
                v -> {
                    MdsObjectKey mdsObjectKey = Backend.objectKey(objectName, bucket.getNamespaceKey().getName(),
                            bucket.getBucketName(), bucket.getNamespaceKey().getApi());
                    if (isMultiPart) {
                        final BeginPartChunkRequest request = BeginPartChunkRequest.newBuilder()
                                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                .setObjectKey(mdsObjectKey)
                                .setObjectId(objId.getId())
                                .setSequenceNum(seqNum)
                                .setVolumeId(volMetaAndVon.getVolumeMetadata().getVolumeId())
                                .setVon(volMetaAndVon.getVon())
                                .build();
                        // Multi-part PUT object
                        invokeMds(
                                MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_BEGIN_PART_CHUNK,
                                false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                        c, objectDeadlineSeconds, opcRequestId).beginPartChunk(request)));
                    } else {
                        final BeginChunkRequest request = BeginChunkRequest.newBuilder()
                                .setObjectId(objId.getId())
                                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                .setObjectKey(mdsObjectKey)
                                .setTransactionId(txnId.toString())
                                .setSequenceNum(seqNum)
                                .setVolumeId(volMetaAndVon.getVolumeMetadata().getVolumeId())
                                .setVon(volMetaAndVon.getVon())
                                .build();
                        // Normal PUT object
                        invokeMds(
                                MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_BEGIN_CHUNK,
                                false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                        c, objectDeadlineSeconds, opcRequestId).beginChunk(request)));
                    }
                });

        PutResult putResult = writeToStorageServer(volMetaAndVon, opcRequestId, opcClientRequestId, chunk, scope);
        MetricScope.timeScopeC(scope, "storageObjDb:finishChunk",
                v -> {
                    final MdsObjectKey mdsObjectKey = Backend.objectKey(objectName, bucket.getNamespaceKey().getName(),
                            bucket.getBucketName(), bucket.getNamespaceKey().getApi());
                    final MdsDigest mdsDigest = MdsTransformer.toMdsDigest(DigestUtils.nullDigest());
                    final MdsEncryptionKey mdsEncryptionKey = MdsTransformer.toMdsEncryptionKey(encryptionKey);
                    if (isMultiPart) {
                        final FinishPartChunkRequest request = FinishPartChunkRequest.newBuilder()
                                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                .setObjectKey(mdsObjectKey)
                                .setObjectId(objId.getId())
                                .setSequenceNum(seqNum)
                                .setSizeInBytes(chunk.getSize())
                                .setDigest(mdsDigest)
                                .setEncryptionKey(mdsEncryptionKey)
                                .build();
                        // Multi-part PUT object
                        invokeMds(
                                MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_FINISH_PART_CHUNK,
                                false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                        c, objectDeadlineSeconds, opcRequestId).finishPartChunk(request)));
                    } else {
                        final FinishChunkRequest request = FinishChunkRequest.newBuilder()
                                .setObjectId(objId.getId())
                                .setBucketToken(bucket.getMdsBucketToken().toByteString())
                                .setObjectKey(mdsObjectKey)
                                .setTransactionId(txnId.toString())
                                .setSequenceNum(seqNum)
                                .setSizeInBytes(putResult.getContentLength())
                                .setDigest(mdsDigest)
                                .setEncryptionKey(mdsEncryptionKey)
                                .build();
                        // Normal PUT object
                        invokeMds(
                                MdsMetrics.OBJECT_MDS_BUNDLE, MdsMetrics.OBJECT_MDS_FINISH_CHUNK,
                                false, () -> objectClient.execute(c -> Backend.getClientWithOptions(
                                        c, objectDeadlineSeconds, opcRequestId).finishChunk(request)));
                    }
                });

        return putResult;
    }

    private PutResult writeToStorageServer(
            VolumeMetaAndVon volMetaAndVon,
            String opcRequestId,
            String opcClientRequestId,
            Chunk chunk,
            MetricScope scope) {
        final Stripe<HostInfo> hostInfoStripe =
                VolumeMetadataUtils.getStorageServerHostInfoStripe(volMetaAndVon.getVolumeMetadata(), volMetaCache);

        final MetricScope putScope = scope.child("volumeStorageClient:put");
        try {
            PutResult result = volumeStorageClient.put(
                    new SCRequestContext(
                            opcRequestId,
                            opcClientRequestId,
                            putScope),
                    new VolumeStorageContext(volMetaAndVon.getVolumeMetadata().getVolumeId(),
                            volMetaAndVon.getVon(),
                            hostInfoStripe),
                    chunk);
            putScope.annotate("chunkBytesTransferred", result.getContentLength()).end();
            return result;
        } catch (Exception e) {
            putScope.fail(e);
            throw e;
        }
    }

    @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    static EncryptionKey getEncryption(@Nullable String kmsKeyId, DecidingKeyManagementService kms) {
        // How can we tell if we are using KMS?
        //  - When encrypting data we look to see if there is a KmsKeyId associated with the bucket, if there is
        //    we are using KMS, if not we should use KmsSecretStoreImpl (which uses a master encryption key stored
        //    as a secret).
        //  - When decrypting data we look at the key version associated with the data. If the data has a key version
        //    then we encrypted the data using KmsSecretStoreImpl (secrets have versions). If not, we use KMS to
        //    decrypt the key. Note that we do NOT need the OCID of the KMS key for decryption because an encrypted
        //    KMS key is self-identifying -- it contains the ID of the key it is encrypted with.
        final boolean usingKms = !Strings.isNullOrEmpty(kmsKeyId);
        final String encryptionKeyId = usingKms ? kmsKeyId : Secrets.ENCRYPTION_MASTER_KEY;
        return kms.generateKey(encryptionKeyId,
                EncryptionConstants.AES_ALGORITHM, EncryptionConstants.AES_KEY_SIZE_BYTES);
    }

    public static <T> T invokeMds(
            MetricsBundle aggregateBundle, MetricsBundle apiBundle, boolean retryable, Callable<T> callable) {
        return MdsClientHelper.invokeMds(aggregateBundle, apiBundle, retryable,
                callable, ObjectResource::toRuntimeException);
    }

    private static <T> RuntimeException toRuntimeException(StatusRuntimeException ex) {
        final MdsException mdsException = ObjectExceptionClassifier.fromStatusRuntimeException(ex);
        if (mdsException instanceof MdsUploadNotFoundException) {
            return new MultipartUploadNotFoundException("No such upload", mdsException);
        } else {
            return Backend.BackendHelper.toRuntimeException(mdsException);
        }
    }

    private WSTenantBucketInfo getBucketMetadata(
            MetricScope scope, String ns, String bucketName, Api api) {
        return Utils.runWithScope(scope, () -> bucketBackend.getBucketMetadataWithCache(ns, bucketName, api), "PutObjectBackend:getBucketMetadata");
    }

    private void verifyStorageCapacityIsEnough(String namespace, String tenantId, Long newObjectSizeInBytes) {
        final long limitForTenant = getStorageLimitBytes(tenantId);

        if (limitForTenant == Limits.DEFAULT_NO_STORAGE_LIMIT_BYTES) {
            return;
        }

        if (limitForTenant == 0) {
            // Don't allow writing new objects/parts when the limit is 0. No need to check usage in this case.
            throw new StorageLimitExceededException("Storage limit exceeded for tenancy during upload");
        }

        final Map<String, Long> map = usageCache.getUsageFromCache(namespace);
        final long usageForTenant = Math.max(0, map.getOrDefault(namespace, 0L));

        // Make sure there is sufficient storage capacity remaining to add the new object. If capacity is full,
        // return StorageLimitExceeded error, even for 0 byte objects.
        // e.g. storage limit bytes is 10, usage is 5. We allow to put object with 5 bytes.
        // e.g. storage limit bytes is 10, usage is 10. We don't allow to put any object (including 0 byte object)
        // e.g. storage limit bytes is 0, usage is 0. We don't allow to put any object (including 0 byte object)
        final long remainingStorageCapacity = limitForTenant - usageForTenant;
        if (remainingStorageCapacity < newObjectSizeInBytes ||
                (remainingStorageCapacity == newObjectSizeInBytes && newObjectSizeInBytes == 0)) {
            throw new StorageLimitExceededException("Storage limit exceeded for tenancy during upload");
        }
    }

    private long getStorageLimitBytes(String tenantId) {
        final Long limitForTenant = auths.getLimits().getStorageLimitBytes(tenantId);
        return Math.max(Limits.DEFAULT_NO_STORAGE_LIMIT_BYTES, limitForTenant);
    }

    private static PickerContext createDefaultPickerContext(long chunkContentLength, String requestId) {
        return PickerContext.builder(requestId)
                .setObjectSizeHint(chunkContentLength)
                .build();
    }

}