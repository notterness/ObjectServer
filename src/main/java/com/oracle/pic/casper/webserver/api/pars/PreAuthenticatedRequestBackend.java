package com.oracle.pic.casper.webserver.api.pars;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.common.json.JsonSerDe;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.util.ParUtil;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.AuthorizationResponse;
import com.oracle.pic.casper.webserver.api.auth.Authorizer;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.backend.BackendConversions;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.backend.GetObjectBackend;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.model.CreatePreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.DeletePreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.GetPreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.ListPreAuthenticatedRequestsRequest;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.ObjectProperties;
import com.oracle.pic.casper.webserver.api.model.ObjectSummaryCollection;
import com.oracle.pic.casper.webserver.api.model.ParPaginatedList;
import com.oracle.pic.casper.webserver.api.model.PreAuthenticatedRequestMetadata;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;
import com.oracle.pic.casper.webserver.api.model.exceptions.NamespaceDeletedException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchObjectException;
import com.oracle.pic.casper.webserver.api.model.exceptions.ParNotFoundException;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.util.BucketOptionsUtil;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 *
 * For details on PARs see
 *
 *    https://confluence.oci.oraclecorp.com/display/CASPER/Pre+Authenticated+Requests
 *    https://confluence.oci.oraclecorp.com/display/PM/Pre-Authenticated+Requests+FAQ
 *
 * Class that encapsulates an implementation of {@link PreAuthenticatedRequestStore}
 * by using Casper itself as the backing store.
 * We do this using {@link BucketBackend} and {@link Backend} components.
 *
 * Note that PARs are stored as zero byte objects in the Casper store in special Casper buckets which are designated
 * as PAR buckets i.e bucket metadata includes key stating it is meant for PARs only.
 *
 * e.g
 *     if Alice has a bucket called AliceBucket and wants to vend PARs for that bucket to Bob, then
 *     there will be a PAR bucket designated as AliceBucket_{SOME_SUFFIX_THAT_IS_NOT_GUESSABLE_BY_CUSTOMERS}
 *     to avoid any collisions with an actual Casper customer's bucket.
 *
 * PARs can grow on the order of objects and hence Casper store needs to scale to accommodate for growth in PARs.
 * Hence PARs are stored as objects in Casper and leverage all engineering that is applied to scaling buckets.
 *
 * Creating a PAR will translate to adding a zero byte object to a PAR bucket, where all the PAR data is stored in the
 * object metadata.
 *
 * Getting a PAR will translate to getting the metadata for PAR object.
 *
 * Delete a PAR will translate to deleting the PAR object.
 *
 * Note that PAR object names will be identified by a par_id which is unique to every PAR and will be available
 * when addressing PARs.
 * PARs will naturally be sorted by par_id in the bucket and hence listing PARs will iterate over PARs in that order.
 *
 *
 * PARs can have an object name associated with the PAR.
 * e.g
 *     if Alice wants to grant Bob GET access to  /n/AliceNamespace/b/AliceBucket/o/AliceObject
 *     then the objectName = AliceObject will be associated with the PAR for this object.
 *
 * Since customers can LIST PARs by object name as well, we structure par ids as
 * bucketName # objectName # verifierId
 * where
 * bucketName is the name of the par originators bucket
 * objectName is the name of the object for which the par is being created - this can be null
 * verifierId is the unique identifier vended by the Identity SDK when managing PARs - this comes from the
 * request STS (string to sign)
 *
 * Adding the optional object name to the actual PAR object name helps in listing with object name prefix.
 *
 */
public class PreAuthenticatedRequestBackend implements PreAuthenticatedRequestStore {

    private static final Logger LOG = LoggerFactory.getLogger(PreAuthenticatedRequestBackend.class);

    private final TenantBackend tenantBackend;
    // the bucket backend component to help manage PAR buckets for customers
    private final BucketBackend bucketBackend;
    // the backend component to help manage PARs for customers
    private final Backend backend;
    private final GetObjectBackend getObjBackend;
    private final PutObjectBackend putObjBackend;
    // used for json serialization for Principal / authInfo to and from PAR md
    private final JsonSerDe jsonSerDe;
    // the authz impl used here when the backed is being used for PAR management
    private Authorizer authorizer;
    private final DecidingKeyManagementService kms;

    public PreAuthenticatedRequestBackend(TenantBackend tenantBackend,
                                          BucketBackend bucketBackend,
                                          Backend backend,
                                          GetObjectBackend getObjBackend,
                                          PutObjectBackend putObjBackend,
                                          Authorizer authorizer,
                                          JsonSerDe jsonSerDe,
                                          DecidingKeyManagementService kms) {
        this.tenantBackend = tenantBackend;
        this.bucketBackend = bucketBackend;
        this.backend = backend;
        this.getObjBackend = getObjBackend;
        this.putObjBackend = putObjBackend;

        this.jsonSerDe = jsonSerDe;
        this.authorizer = authorizer;
        this.kms = kms;
    }

    /**
     * Provision a PAR for a customer.
     * Steps
     *       1.  Create the par bucket if needed, since bucket may already exist if this is not the first PAR
     *       2.  Add the PAR to the bucket using Backend.java
     * @param request  the request describing the PAR
     * @return future with the PAR md details since this is an async operation, coz this uses Backend
     */
    @Override
    public CompletableFuture<PreAuthenticatedRequestMetadata>
    createPreAuthenticatedRequest(CreatePreAuthenticatedRequestRequest request) {

        final MetricScope scope = WSRequestContext.getMetricScope(request.getContext());
        final BackendParContext backendContext = new BackendParContext(request.getContext(), request.getAuthInfo());

        return VertxUtil.runAsync(() -> {
            WSTenantBucketInfo userBucket = authAgainstUserBucket(scope, backendContext, ParOperationAuth.CreatePar);
            WSRequestContext.get(request.getContext()).setObjectLevelAuditMode(userBucket.getObjectLevelAuditMode());

            //Setting properties in the context which will be used by the Service Logger
            WSRequestContext.get(request.getContext()).setBucketOcid(userBucket.getOcid());
            WSRequestContext.get(request.getContext()).setBucketCreator(userBucket.getCreationUser());
            WSRequestContext.get(request.getContext()).setBucketLoggingStatus(BucketOptionsUtil
                    .getBucketLoggingStatusFromOptions(userBucket.getOptions()));

            final String bucketName = userBucket.getBucketName();
            final MdsNamespace mdsNamespace =
                tenantBackend.getNamespace(request.getContext(), scope, userBucket.getNamespaceKey())
                    .orElseThrow(() -> new NoSuchBucketException(BucketBackend.noSuchBucketMsg(
                        bucketName,
                        userBucket.getNamespaceKey().getName())));

            // identify PARs by the bucket's immutableResourceId
            backendContext.parId = new FullParId(userBucket.getImmutableResourceId(),
                    request.getObjectName().orElse(null), request.getVerifierId()).toString();
            return bucketBackend.headOrCreateInternalBucket(backendContext.routingContext, backendContext.authInfo,
                    mdsNamespace.getTenantOcid(), backendContext.namespace, backendContext.parBucketName,
                    BucketBackend.InternalBucketType.PAR.getSuffix());
        }).thenCompose(parBucket -> createPar(backendContext, request));
    }

    // helper method to create a PAR object in the backend store from the create PAR request
    private CompletableFuture<PreAuthenticatedRequestMetadata> createPar(BackendParContext backendContext,
                                                                         CreatePreAuthenticatedRequestRequest request) {

        Map<String, String> map = ParConversionUtils.toMetadataMap(request, jsonSerDe);
        Date creationTime = Date.from(Instant.now());
        ObjectMetadata objectMetadata = new ObjectMetadata(
                backendContext.namespace,         // the original tenant namespace
                backendContext.parBucketName,     // bucket name for PAR objects
                backendContext.parId,             // the PAR object name  i.e. objectName # verifierId
                0,                                // zero size object
                null,                             // no computed MD5
                map,                              // attributes containing the actual PAR data
                creationTime, creationTime,       // creation and modification time are the same
                null,                             // we don't need an etag
                null, null, //archivedTime and restoredTime are null for non-archival object
                ArchivalState.Available
        );

        LOG.debug("Creating PAR in backend: {} / {} / {}", backendContext.namespace, backendContext.parBucketName,
                backendContext.parId);

        return putObjBackend.createParObject(
                backendContext.routingContext,
                backendContext.authInfo,
                objectMetadata,
                Api.V2)
                .thenApply(objMetadata -> ParConversionUtils.fromObjectMetadata(objMetadata, jsonSerDe));
    }

    /**
     * Fetch par from backend store
     * @param request the request to get the par from the backend store
     * @return future for par md pointed to by the par id supplied
     */
    @Override
    public CompletableFuture<PreAuthenticatedRequestMetadata>
    getPreAuthenticatedRequest(GetPreAuthenticatedRequestRequest request) {

        final MetricScope scope = WSRequestContext.getMetricScope(request.getContext());
        BackendParContext backendContext = new BackendParContext(request.getContext(), request.getAuthInfo());
        Validator.validateV2Namespace(backendContext.namespace);

        BackendParId backendParId = request.getParId();

        return VertxUtil.runAsync(() -> authAgainstUserBucket(scope, backendContext, ParOperationAuth.GetPar))
                .thenCompose(userBucket -> {
                    return getParHelper(userBucket, backendContext, backendParId);
                })
                .exceptionally(throwable -> {
                    Throwable t = unwrapException(throwable);
                    if (t instanceof NoSuchObjectException || t instanceof NoSuchBucketException) {
                        LOG.debug("Got PAR NOT FOUND in backend store");
                        throw new ParNotFoundException("PAR not found for : " + backendParId);
                    } else if (t instanceof NamespaceDeletedException) {
                        throw (NamespaceDeletedException) t;
                    } else {
                        throw new RuntimeException("Unexpected Error when trying to retrieve PAR", t);
                    }
                });
    }

    /**
     * Delete the par from the backend store
     * @param request the request to delete the par from the backend store
     */
    @Override
    public void deletePreAuthenticatedRequest(DeletePreAuthenticatedRequestRequest request) {

        final MetricScope scope = WSRequestContext.getMetricScope(request.getContext());
        BackendParContext backendContext = new BackendParContext(request.getContext(), request.getAuthInfo());
        Validator.validateV2Namespace(backendContext.namespace);

        WSTenantBucketInfo userBucket = authAgainstUserBucket(scope, backendContext, ParOperationAuth.DeletePar);

        backendContext.parId = request.getParId().toFullParId(userBucket.getImmutableResourceId()).toString();

        LOG.debug("Deleting PAR from store namespace: {}, bucket: {}, parId: {}", backendContext.namespace,
                backendContext.bucketName, backendContext.parId);

        try {
            backend.deleteParObject(backendContext.routingContext, backendContext.authInfo, backendContext.namespace,
                    backendContext.parBucketName, backendContext.parId)
                    .orElseThrow(() -> new ParNotFoundException("PAR not found for :" + request.getParId()));
        } catch (NoSuchObjectException | NoSuchBucketException e) {
            throw new ParNotFoundException("PAR not found for :" + request.getParId());
        } finally {
            WSRequestContext.get(request.getContext()).setObjectLevelAuditMode(userBucket.getObjectLevelAuditMode());
            //Setting properties in the context which will be used by the Service Logger
            WSRequestContext.get(request.getContext()).setBucketOcid(userBucket.getOcid());
            WSRequestContext.get(request.getContext()).setBucketCreator(userBucket.getCreationUser());
            WSRequestContext.get(request.getContext()).setBucketLoggingStatus(BucketOptionsUtil
                    .getBucketLoggingStatusFromOptions(userBucket.getOptions()));
        }
    }

    /**
     * List PARs as per request details.
     * Note that we List PARs is a paginated api. Hence the request has a pageLimit and nextPageToken attribute.
     * When listing PARs we iterate over the PARs bucket to get all the parIds. Then we do GET PAR for each parId
     * and add that to the list result being returned
     *
     * @param request the list PAR request
     * @return future with a list of PARs that match the attrs of the list request
     */
    @Override
    public CompletableFuture<ParPaginatedList<PreAuthenticatedRequestMetadata>>
    listPreAuthenticatedRequests(ListPreAuthenticatedRequestsRequest request) {

        final MetricScope scope = WSRequestContext.getMetricScope(request.getContext());
        BackendParContext backendContext = new BackendParContext(request.getContext(), request.getAuthInfo());
        Validator.validateV2Namespace(backendContext.namespace);

        // if looking up a PAR, issue get request with parId; otherwise, do normal list
        if (request.getParId().isPresent()) {
            BackendParId parId = request.getParId().get();
            LOG.debug("parId found in request {}, looking it up by sending a Get PAR request", request);
            return VertxUtil.runAsync(() -> authAgainstUserBucket(scope, backendContext, ParOperationAuth.ListPars))
                    .thenCompose(userBucket -> getParHelper(userBucket, backendContext, parId))
                    .thenApply(parMd -> new ParPaginatedList<>(Collections.singletonList(parMd), null))
                    .exceptionally(throwable -> {
                        Throwable t = unwrapException(throwable);
                        if (t instanceof NoSuchObjectException || t instanceof NoSuchBucketException) {
                            LOG.debug("Got PAR NOT FOUND in backend store");
                            return new ParPaginatedList<>(new ArrayList<>(), null);
                        } else {
                            throw new RuntimeException("Unexpected Error when trying to look up PAR", t);
                        }
                    });
        } else {
            return VertxUtil.runAsync(() -> {
                WSTenantBucketInfo userBucket = authAgainstUserBucket(scope, backendContext, ParOperationAuth.ListPars);

                WSRequestContext.get(request.getContext())
                    .setObjectLevelAuditMode(userBucket.getObjectLevelAuditMode());
                // we want the object name (backend parId), creation time, and metadata (the PAR)

                ImmutableList<ObjectProperties> properties = ImmutableList.of(
                        ObjectProperties.NAME,
                        ObjectProperties.TIMECREATED,
                        ObjectProperties.METADATA);

                int pageLimit = request.getPageLimit();
                String listPrefix = Joiner.on(ParUtil.PAR_ID_SEPARATOR_BACKEND)
                        .join(userBucket.getImmutableResourceId(), request.getObjectNamePrefix().orElse(""));

                LOG.debug("Listing PARs for request {} with prefix {} and limit {}", request, listPrefix, pageLimit);

                try {
                    ObjectSummaryCollection osc =
                            backend.listParObjects(backendContext.routingContext, properties, pageLimit,
                                    backendContext.namespace, backendContext.parBucketName, null, listPrefix,
                                    request.getNextPageToken().orElse(null), null);
                    String nextPageToken = osc.getNextStartWith();
                    // construct the list of parMds
                    List<PreAuthenticatedRequestMetadata> parMds =
                            StreamSupport.stream(osc.getObjects().spliterator(), false)
                                    .map(objSummary -> ParConversionUtils.fromObjectSummary(objSummary, jsonSerDe))
                                    .collect(Collectors.toList());

                    LOG.debug("Got {} pars from listing for request {}", parMds.size(), request);

                    //Setting properties in the context which will be used by the Service Logger.
                    //Setting these after the backend has listed par objects so that parBucket can be overritten with
                    //userBucket
                    WSRequestContext.get(request.getContext()).setBucketOcid(userBucket.getOcid());
                    WSRequestContext.get(request.getContext()).setBucketCreator(userBucket.getCreationUser());
                    WSRequestContext.get(request.getContext()).setBucketLoggingStatus(BucketOptionsUtil
                            .getBucketLoggingStatusFromOptions(userBucket.getOptions()));

                    return new ParPaginatedList<>(parMds, nextPageToken);
                } catch (NoSuchBucketException e) {
                    // PAR bucket not found, which means user has not created any PARs for the bucket yet
                    return new ParPaginatedList<>(new ArrayList<>(), null);
                }

            });
        }
    }

    private CompletableFuture<PreAuthenticatedRequestMetadata> getParHelper(WSTenantBucketInfo userBucket,
                                                                            BackendParContext backendContext,
                                                                            BackendParId backendParId) {

        backendContext.parId = backendParId.toFullParId(userBucket.getImmutableResourceId()).toString();
        LOG.debug("Reading PAR from store namespace: {}, bucket: {}, parId: {}", backendContext.namespace,
                backendContext.bucketName, backendContext.parId);

        return getObjBackend.getParStorageObject(
                backendContext.routingContext,
                backendContext.namespace,
                backendContext.parBucketName,
                backendContext.parId)
                .thenApply(so -> ParConversionUtils.fromObjectMetadata(
                        BackendConversions.wsStorageObjectSummaryToObjectMetadata(so, kms), jsonSerDe))
                .whenComplete((parMd, exception) -> {
                    WSRequestContext wsRequestContext = WSRequestContext.get(backendContext.routingContext);
                    wsRequestContext.setObjectLevelAuditMode(userBucket.getObjectLevelAuditMode());
                    wsRequestContext.setBucketLoggingStatus(BucketOptionsUtil
                            .getBucketLoggingStatusFromOptions(userBucket.getOptions()));
                    wsRequestContext.setBucketOcid(userBucket.getOcid());
                    wsRequestContext.setBucketCreator(userBucket.getCreationUser());
                });
    }

    private WSTenantBucketInfo authAgainstUserBucket(MetricScope scope, BackendParContext backendContext,
                                                     ParOperationAuth parOperationAuth) {
        // we should do this only on the Vert.x worker thread
        VertxUtil.assertOnVertxWorkerThread();
        MetricScope childScope = scope.child("PreAuthenticatedRequestBackend:authAgainstUserBucket");
        // first check that the bucket exists
        Validator.validateBucket(backendContext.bucketName);
        WSTenantBucketInfo userBucket = getUserBucket(backendContext, childScope);



        // now attempt to authorize access to the user bucket
        // i.e if the user is trying to create a PUT PAR, then they must have PUT_OBJECT access to the bucket
        Optional<AuthorizationResponse> authResponse = authorizer.authorize(
                WSRequestContext.get(backendContext.routingContext),
                backendContext.authInfo,
                userBucket.getNamespaceKey(),
                userBucket.getBucketName(),
                userBucket.getCompartment(),
                userBucket.getPublicAccessType(),
                parOperationAuth.getOp(),
                userBucket.getKmsKeyId().orElse(null),
                true,
                userBucket.isTenancyDeleted(),
                parOperationAuth.getPermissions()
        );
        if (!authResponse.isPresent()) {
            LOG.debug("Failed to authorize compartment {} bucket {} for PAR operation {}",
                    userBucket.getCompartment(), userBucket.getBucketName(), parOperationAuth);
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(userBucket.getBucketName(),
                    userBucket.getNamespaceKey().getName()));
        }
        return userBucket;
    }

    private WSTenantBucketInfo getUserBucket(BackendParContext backendContext, MetricScope scope) {
        LOG.debug("Getting user bucket, namespace {} bucket {}", backendContext.namespace, backendContext.bucketName);
        return bucketBackend.getBucketMetadataWithCache(
            backendContext.routingContext,
            scope,
            backendContext.namespace,
            backendContext.bucketName, Api.V2);
    }

    @Override
    public CompletableFuture<Void> updateExpiration(String parId, Instant expiration)
            throws ParNotFoundException, BadRequestException {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Void> updateStatus(String parId, PreAuthenticatedRequestMetadata.Status status)
            throws ParNotFoundException {
        throw new NotImplementedException();
    }

    /**
     * Useful inner class that encapsulates all the various details needed when interacting with Backend
     * and BucketBackend for managing PARs. It's just a bag of all the fields needed to use the Backend api.
     */
    private static final class BackendParContext {

        private RoutingContext routingContext;
        private AuthenticationInfo authInfo;
        private String compartmentId;
        private String subjectId;
        private String namespace;
        private String bucketName;
        private String parBucketName;
        private String parId;

        private BackendParContext(RoutingContext context, AuthenticationInfo authInfo) {

            this.routingContext = context;
            this.authInfo = authInfo;
            this.compartmentId = authInfo.getMainPrincipal().getTenantId();
            this.subjectId = authInfo.getMainPrincipal().getSubjectId();

            final HttpServerRequest httpRequest = context.request();
            final WSRequestContext wsRequestContext = WSRequestContext.get(context);
            this.namespace = HttpPathQueryHelpers.getNamespaceName(httpRequest, wsRequestContext);
            this.bucketName = HttpPathQueryHelpers.getBucketName(httpRequest, wsRequestContext);
            this.parBucketName = ParUtil.toParBucketName(namespace);
        }

        @Override
        public String toString() {
            return "BackendParContext{" +
                    ", authInfo=" + authInfo +
                    ", compartmentId='" + compartmentId + '\'' +
                    ", subjectId='" + subjectId + '\'' +
                    ", namespace='" + namespace + '\'' +
                    ", bucketName='" + bucketName + '\'' +
                    ", parBucketName='" + parBucketName + '\'' +
                    ", parId='" + parId + '\'' +
                    '}';
        }
    }

    /**
     * Private helper class that aids in performing the right authz checks for the right type of par operation.
     * Note that when performing par management operations, the user must have PAR_MANAGE permission.
     */
    private enum ParOperationAuth {

        GetPar(CasperOperation.GET_PAR, CasperPermission.PAR_MANAGE),
        DeletePar(CasperOperation.DELETE_PAR, CasperPermission.PAR_MANAGE),
        ListPars(CasperOperation.LIST_PARS, CasperPermission.PAR_MANAGE),
        CreatePar(CasperOperation.CREATE_PAR, CasperPermission.PAR_MANAGE);

        private final CasperOperation op;
        private final CasperPermission[] permissions;

        ParOperationAuth(CasperOperation op, CasperPermission... permissions) {
            this.op = op;
            this.permissions = permissions;
        }

        public CasperOperation getOp() {
            return op;
        }

        public CasperPermission[] getPermissions() {
            return permissions;
        }

        @Override
        public String toString() {
            return "ParOperationAuth{" +
                    "op=" + op +
                    ", permissions=" + Arrays.toString(permissions) +
                    '}';
        }
    }

    private Throwable unwrapException(Throwable t) {
        if (t == null) {
            return new RuntimeException("unexpected");
        }

        if (t instanceof CompletionException) {
            return t.getCause();
        } else {
            return t;
        }
    }
}
