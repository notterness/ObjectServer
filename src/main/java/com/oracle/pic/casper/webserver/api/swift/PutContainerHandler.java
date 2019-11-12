package com.oracle.pic.casper.webserver.api.swift;

import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.AuthorizationException;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.NotAuthenticatedException;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.BucketCreate;
import com.oracle.pic.casper.webserver.api.model.BucketUpdate;
import com.oracle.pic.casper.webserver.api.model.exceptions.BucketAlreadyExistsException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Vert.x HTTP handler to create a new container or update metadata on an existing container.
 * <p>
 * A PUT operation is idempotent, it either creates a container or updates an existing one as appropriate.
 */
public class PutContainerHandler extends SyncBodyHandler {

    private final Authenticator authenticator;
    private final BucketBackend bucketBackend;
    private final TenantBackend tenantBackend;
    private final SwiftResponseWriter responseWriter;
    private final ResourceControlledMetadataClient metadataClient;
    private final EmbargoV3 embargoV3;

    public PutContainerHandler(Authenticator authenticator,
                               BucketBackend bucketBackend,
                               TenantBackend tenantBackend,
                               SwiftResponseWriter responseWriter,
                               ResourceControlledMetadataClient metadataClient,
                               CountingHandler.MaximumContentLength maximumContentLength,
                               EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.bucketBackend = bucketBackend;
        this.tenantBackend = tenantBackend;
        this.responseWriter = responseWriter;
        this.metadataClient = metadataClient;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {

    }

    /**
     * Attempt to create a new container, or update an existing container
     * <p>
     * The flow is as follows:
     * - Attempt to retrieve the tenantOcid from the backend using the request.namespace
     * - if the namespace doesn't exist but a compartment-id was passed in as a header, attempt to use that to
     * retrieve the tenant from Identity
     * - Reject the request if a tenantId still hasn't been found.
     * - If a tenantId has been found, authenticate the request
     * - ensure that the Namespace exists by calling the (idempotent) TenantDb.getOrCreateNamespace
     * - set the compartment-id by using the value from the header if present, else use the default Swift CompartmentId
     * - Attempt to create a bucket with these parameters
     * - If that operation fails because of an existing Bucket, attempt to update that bucket but set the compartmentid
     *   appropriately: only set a compartment-id for an update if it's present in the headers.
     *
     * The failure modes are:
     * - A tenantId cannot be retrieved from either the request namespace, or the compartment-id in the header
     * - The request cannot be authenticated
     * </p>
     *
     * @param context The vertx routing context
     */
    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        MetricsHandler.addMetrics(context, WebServerMetrics.SWIFT_PUT_CONTAINER_BUNDLE);
        WSRequestContext.setOperationName("swift", getClass(), context, CasperOperation.CREATE_BUCKET);

        final HttpServerRequest request = context.request();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();

        // Setting the visa to allow for re-entries, as the backend call makes a couple of authorize calls
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final String namespace = SwiftHttpPathHelpers.getNamespace(request, wsRequestContext);
        final String containerName = SwiftHttpPathHelpers.getContainerName(request, wsRequestContext);
        final String compartmentIdFromHeader = request.getHeader(SwiftHeaders.COMPARTMENT_ID_HEADER);

        EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.Swift)
                .setOperation(CasperOperation.CREATE_BUCKET)
                .setNamespace(namespace)
                .setBucket(containerName)
                .build();
        embargoV3.enter(embargoV3Operation);

        try {

            // Attempt to retrieve the tenantOcid using the namespace from the request
            final NamespaceKey namespaceKey = new NamespaceKey(Api.V2, namespace);
            final Optional<MdsNamespace> namespaceInfo = tenantBackend.getNamespace(context, scope, namespaceKey);
            String tenantOcid = namespaceInfo.map(MdsNamespace::getTenantOcid).orElse(null);

            // if tenantBackend can't find a namespace to use, attempt to look up tenantId by using the compartmentId
            // This could happen if the customer has used Casper in a different region, but not in this region (implying
            // they don't have a namespace in this region, but have a (possibly) valid compartmentId)
            if (tenantOcid == null && compartmentIdFromHeader != null) {
                final Optional<Tenant> tenant =
                        metadataClient.getTenantByCompartmentId(compartmentIdFromHeader, namespace);

                // Reject if the tenant is still missing, or the namespaces don't match up, or no id present
                if (!tenant.isPresent() || !tenant.get().getNamespace().equalsIgnoreCase(namespace) ||
                        tenant.get().getId() == null) {
                    throw new NotAuthenticatedException("Put Container is limited to Authenticated users");
                }

                tenantOcid = tenant.get().getId();
                tenantBackend.getOrCreateNamespace(context, scope, namespaceKey, tenantOcid);
            }

            // Reject if we still don't have a tenantOcid
            if (tenantOcid == null) {
                throw new NotAuthenticatedException("Put Container is limited to Authenticated users");
            }

            WSRequestContext.get(context).setResourceTenantOcid(tenantOcid);

            final String bodySha256 = Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes());
            // A valid tenancy found, attempt to authenticate
            final AuthenticationInfo authInfo =
                    authenticator.authenticateSwiftOrCavage(context, bodySha256, namespace, tenantOcid);

            // use compartmentId specified in the header, else use the default compartment for the tenant
            final String compartmentId;
            if (compartmentIdFromHeader != null) {
                compartmentId = compartmentIdFromHeader;
            } else {
                compartmentId = namespaceInfo.map(MdsNamespace::getDefaultSwiftCompartment)
                        .orElse(tenantOcid);
            }

            // Attempt to first create a bucket, and then attempt an update if it's found to already exist
            final String createdBy = authInfo.getMainPrincipal().getSubjectId();
            final BucketCreate bucketCreate = new BucketCreate(
                    namespace, containerName, compartmentId, createdBy,
                    SwiftHttpHeaderHelpers.getContainerMetadataHeadersForPut(request));
            try {
                Validator.validateBucket(bucketCreate.getBucketName());
                bucketBackend.createBucket(context, authInfo, bucketCreate, Api.V2);
            } catch (AuthorizationException | BucketAlreadyExistsException ex) {
                // Replace the 'empty' values in the headers with nulls, as that's what updateBucketPartially expects
                final Map<String, String> updatedMetadata = new HashMap<>(bucketCreate.getMetadata());
                updatedMetadata.replaceAll(
                        (k, v) -> (v.isEmpty()) ? null : v
                );

                wsRequestContext.setOperation(CasperOperation.UPDATE_BUCKET);

                // Only update compartment-id if its present in the headers
                final BucketUpdate bucketUpdate = BucketUpdate.builder()
                        .namespace(bucketCreate.getNamespaceName())
                        .compartmentId(compartmentIdFromHeader)
                        .bucketName(bucketCreate.getBucketName())
                        .metadata(updatedMetadata)
                        .isMissingMetadata(false)
                        .build();

                bucketBackend.updateBucketPartially(context, authInfo, bucketUpdate, null);
            }

            responseWriter.writeCreatedResponse(context);

        } catch (Exception ex) {
            throw SwiftHttpExceptionHelpers.rewrite(context, ex);
        }
    }
}
