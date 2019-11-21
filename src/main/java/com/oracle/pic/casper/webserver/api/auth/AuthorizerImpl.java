package com.oracle.pic.casper.webserver.api.auth;

import com.google.api.client.util.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.oracle.pic.casper.common.config.failsafe.FailSafeConfig;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.exceptions.AuthDataPlaneServerException;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoContext;
import com.oracle.pic.casper.webserver.auth.dataplane.AuthMetrics;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.webserver.auth.limits.Limits;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.webserver.auth.CasperResourceKind;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.common.TaggingClientHelper;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidTagsException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NamespaceDeletedException;
import com.oracle.pic.casper.webserver.api.ratelimit.Embargo;
import com.oracle.pic.casper.webserver.api.sg.ServiceGateway;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.limit.ResourceTicket;
import com.oracle.pic.casper.webserver.limit.ResourceType;
import com.oracle.pic.casper.webserver.server.WebServerAuths;
import com.oracle.pic.casper.webserver.util.TestTenants;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.commons.metadata.entities.Compartment;
import com.oracle.pic.identity.authentication.ClaimType;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.error.AuthClientException;
import com.oracle.pic.identity.authentication.error.AuthServerUnavailableException;
import com.oracle.pic.identity.authorization.common.FixedVariableNames;
import com.oracle.pic.identity.authorization.permissions.ActionKind;
import com.oracle.pic.identity.authorization.permissions.ContextVariableFactory;
import com.oracle.pic.identity.authorization.permissions.MandatoryVariableFactory;
import com.oracle.pic.identity.authorization.permissions.OptionalVariableFactory;
import com.oracle.pic.identity.authorization.permissions.Permission;
import com.oracle.pic.identity.authorization.sdk.AssociationAuthorizationRequest;
import com.oracle.pic.identity.authorization.sdk.AssociationAuthorizationResponse;
import com.oracle.pic.identity.authorization.sdk.AssociationVerificationResult;
import com.oracle.pic.identity.authorization.sdk.AuthorizationClient;
import com.oracle.pic.identity.authorization.sdk.AuthorizationRequest;
import com.oracle.pic.identity.authorization.sdk.AuthorizationRequestFactory;
import com.oracle.pic.identity.authorization.sdk.error.AuthorizationClientException;
import com.oracle.pic.identity.authorization.sdk.response.AuthorizationResponseResult;
import com.oracle.pic.kms.internal.crypto.sdk.auth.KeyDelegatePermission;
import com.oracle.pic.tagging.client.tag.TagSet;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AuthorizerImpl implements Authorizer {
    private static final Logger LOG = LoggerFactory.getLogger(AuthorizerImpl.class);

    /**
     * Identity variable name to do matching against a subnet
     * https://confluence.oci.oraclecorp.com/display/PLAT/SUBNET+TYPE+VARIABLE
     */
    public static final String IP_ADDRESS_VARIABLE = "request.ipv4.ipaddress";
    public static final String REQUEST_KMSKEY_ID = "request.kms-key.id";

    private static final String IDENTITY_REQUEST_ID_KEY = "identityRequestId";

    // CASPER-7056: Support new IP based policy in Casper
    private static final String REQUEST_NETWORK_SOURCE_TYPE = "request.networkSource.type";
    private static final String REQUEST_NETWORK_SOURCE_VCN_ID = "request.networkSource.vcnId";
    private static final String REQUEST_NETWORK_SOURCE_IP = "request.networkSource.ipAddr";

    //CASPER-5633 transitioning to new service name; this is the old client with "Casper" service name
    private final AuthorizationClient oldAuthorizationClient;
    //this is the new client with "CasperObo" service name
    private final AuthorizationClient newAuthorizationClient;
    private final ResourceControlledMetadataClient metadataClient;
    private final DecidingKeyManagementService kms;
    private final Embargo embargo;

    private final ServiceGateway serviceGateway;

    private final Limits limits;

    private final TestTenants testTenants;

    private final String serviceName;

    private final RetryPolicy retryPolicy;

    private final ResourceLimiter resourceLimiter;

    public AuthorizerImpl(AuthorizationClient oldAuthorizationClient,
                          AuthorizationClient newAuthorizationClient,
                          ResourceControlledMetadataClient metadataClient,
                          DecidingKeyManagementService kms,
                          Limits limits,
                          Embargo embargo,
                          ServiceGateway serviceGateway,
                          TestTenants testTenants,
                          String serviceName,
                          FailSafeConfig failSafeConfig,
                          ResourceLimiter resourceLimiter) {
        this.oldAuthorizationClient = oldAuthorizationClient;
        this.newAuthorizationClient = newAuthorizationClient;
        this.metadataClient = metadataClient;
        this.kms = kms;
        this.embargo = embargo;
        this.serviceGateway = serviceGateway;
        this.testTenants = testTenants;
        this.limits = limits;
        this.serviceName = serviceName;
        this.retryPolicy = failSafeConfig.getRetryPolicy();
        this.resourceLimiter = resourceLimiter;
    }

    AuthorizerImpl(AuthorizationClient oldAuthorizationClient,
                   AuthorizationClient newAuthorizationClient,
                   ResourceControlledMetadataClient metadataClient,
                   DecidingKeyManagementService kms,
                   Limits limits,
                   Embargo embargo,
                   ServiceGateway serviceGateway,
                   TestTenants testTenants,
                   String serviceName,
                   ResourceLimiter resourceLimiter) {
        this(
            oldAuthorizationClient,
            newAuthorizationClient,
            metadataClient,
            kms,
            limits,
            embargo,
            serviceGateway,
            testTenants,
            serviceName,
            FailSafeConfig.NONE,
            resourceLimiter);
    }

    private boolean isSuspended(CasperOperation operation, String compartmentId, String namespace) {
        /*
         * This is a short-term fix to enable testing for billing/metering for OOW 2017. This makes it so that
         * Tenancies marked as "suspended" in our identity system have authorization fail for "create"
         * operations. The "suspended" flag currently means that the customer is over a quota, so we continue to
         * allow the tenancy to read, update and delete their data.
         */
        if (operation.isCreateOp()) {
            final Optional<Tenant> tenant = metadataClient.getTenantByCompartmentId(compartmentId, namespace);
            // The auth data plane does not know about this compartment ID, which means it is probably not a
            // real compartment. In that case, rather than returning a 500 error to the customer, we'll treat
            // this as a failed authorization (since you cannot authorize against an unknown compartment).
            return tenant.map(tenant1 -> limits.isSuspended(tenant1.getId())).orElse(true);
        }

        return false;
    }

    @Override
    public Optional<AuthorizationResponse> authorize(WSRequestContext context,
                                                     AuthenticationInfo authInfo,
                                                     @Nullable NamespaceKey namespaceKey,
                                                     @Nullable String bucketName,
                                                     String compartmentId,
                                                     BucketPublicAccessType bucketAccessType,
                                                     CasperOperation operation,
                                                     String kmsKeyId,
                                                     boolean authorizeAll,
                                                     boolean tenancyDeleted,
                                                     CasperPermission... permissions) {
        final long startTime = System.nanoTime();
        final Optional<AuthorizationResponse> response;
        try {
            // should never be executed from an event loop
            VertxUtil.assertOnNonVertxEventLoop();

            AuthMetrics.WEB_SERVER_AUTHZ.getRequests().inc();
            response = doAuthorization(
                    context,
                    authInfo,
                    namespaceKey,
                    bucketName,
                    compartmentId,
                    bucketAccessType,
                    operation,
                    null, // tagging operation
                    null, // newTagSet
                    null, // oldTagSet
                    new KmsKeyUpdateAuth(kmsKeyId, null, false),
                    authorizeAll,
                    permissions);

            if (!response.isPresent()) {
                AuthMetrics.WEB_SERVER_AUTHZ.getClientErrors().inc();
            }

            response.ifPresent(r -> context.setTagSlug(r.getTagSlug()));
            if (response.isPresent() && tenancyDeleted) {
                throw new NamespaceDeletedException("This Tenancy is Deleted");
            } else {
                return response;
            }
        } catch (AuthorizationClientException | AuthClientException | NamespaceDeletedException e) {
            AuthMetrics.WEB_SERVER_AUTHZ.getClientErrors().inc();
            throw e;
        } catch (Throwable throwable) {
            AuthMetrics.WEB_SERVER_AUTHZ.getServerErrors().inc();
            throw throwable;
        } finally {
            AuthMetrics.WEB_SERVER_AUTHZ.getOverallLatency().update(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<AuthorizationResponse> authorize(AuthenticationInfo authInfo,
                                                     @Nullable NamespaceKey namespaceKey,
                                                     @Nullable String bucketName,
                                                     String compartmentId,
                                                     BucketPublicAccessType bucketAccessType,
                                                     CasperOperation operation,
                                                     String kmsKeyId,
                                                     boolean authorizeAll,
                                                     boolean tenancyDeleted,
                                                     MetricScope rootScope,
                                                     Set<CasperPermission> permissions,
                                                     EmbargoContext embargoContext,
                                                     String vcnId,
                                                     String vcnDebugId,
                                                     String namespace) {
        final long startTime = System.nanoTime();
        final Optional<AuthorizationResponse> response;
        try {
            // should never be executed from an event loop
            VertxUtil.assertOnNonVertxEventLoop();

            AuthMetrics.WEB_SERVER_AUTHZ.getRequests().inc();
            response = doAuthorization(rootScope,
                    authInfo,
                    namespaceKey,
                    bucketName,
                    compartmentId,
                    bucketAccessType,
                    operation,
                    null, // tagging operation
                    null, // newTagSet
                    null, // oldTagSet
                    new KmsKeyUpdateAuth(kmsKeyId, null, false),
                    authorizeAll,
                    permissions, namespace, vcnId, vcnDebugId, embargoContext);

            if (!response.isPresent()) {
                AuthMetrics.WEB_SERVER_AUTHZ.getClientErrors().inc();
            }

            if (response.isPresent() && tenancyDeleted) {
                throw new NamespaceDeletedException("This Tenancy is Deleted");
            } else {
                return response;
            }
        } catch (AuthorizationClientException | AuthClientException | NamespaceDeletedException e) {
            AuthMetrics.WEB_SERVER_AUTHZ.getClientErrors().inc();
            throw e;
        } catch (Throwable throwable) {
            AuthMetrics.WEB_SERVER_AUTHZ.getServerErrors().inc();
            throw throwable;
        } finally {
            AuthMetrics.WEB_SERVER_AUTHZ.getOverallLatency().update(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<AuthorizationResponse> authorizeWithTags(WSRequestContext context,
                                                   AuthenticationInfo authInfo,
                                                   @Nullable NamespaceKey namespaceKey,
                                                   @Nullable String bucketName,
                                                   String compartmentId,
                                                   @Nullable BucketPublicAccessType bucketAccessType,
                                                   CasperOperation operation,
                                                   @Nullable TaggingOperation taggingOperation,
                                                   @Nullable TagSet newTagSet,
                                                   @Nullable TagSet oldTagSet,
                                                   KmsKeyUpdateAuth kmsKeyUpdateAuth,
                                                   boolean authorizeAll,
                                                   boolean tenancyDeleted,
                                                   CasperPermission... permissions) {
        final long startTime = System.nanoTime();
        final Optional<AuthorizationResponse> response;
        try {
            // should never be executed from an event loop
            VertxUtil.assertOnNonVertxEventLoop();

            AuthMetrics.WEB_SERVER_AUTHZ.getRequests().inc();
            response = doAuthorization(
                    context,
                    authInfo,
                    namespaceKey,
                    bucketName,
                    compartmentId,
                    bucketAccessType,
                    operation,
                    taggingOperation,
                    newTagSet,
                    oldTagSet,
                    kmsKeyUpdateAuth,
                    authorizeAll,
                    permissions);

            if (!response.isPresent()) {
                AuthMetrics.WEB_SERVER_AUTHZ.getClientErrors().inc();
            }
            //Perform a sanity check that the `tagSet` if not null
            //can be safely decoded using our client -- this is to future proof problems where Identity may allow
            //tags that Casper can never decode ex. https://jira-sd.mc1.oracleiaas.com/browse/ID-7770
            //this sanity check is after the doAuthorization, so Identity has a chance to control the status code
            //for failed TagSets
            response.flatMap(AuthorizationResponse::getTagSet).ifPresent(tagSet -> {
                try {
                    if (!TaggingClientHelper.extractTagSet(TaggingClientHelper.toByteArray(tagSet))
                            .equals(tagSet)) {
                        throw new InvalidTagsException("The provided tags could not be validated.");
                    }
                } catch (Throwable t) {
                    LOG.debug("Invalid newTagSet {} was trying to be set", tagSet, t);
                    throw new InvalidTagsException("The provided tags could not be validated.");
                }
            });

            response.ifPresent(r -> context.setTagSlug(r.getTagSlug()));
            if (response.isPresent() && tenancyDeleted) {
                throw new NamespaceDeletedException("This Tenancy is Deleted");
            } else {
                return response;
            }
        } catch (InvalidTagsException | AuthorizationClientException | AuthClientException |
                NamespaceDeletedException e) {
            AuthMetrics.WEB_SERVER_AUTHZ.getClientErrors().inc();
            throw e;
        }  catch (Throwable throwable) {
            AuthMetrics.WEB_SERVER_AUTHZ.getServerErrors().inc();
            throw throwable;
        } finally {
            AuthMetrics.WEB_SERVER_AUTHZ.getOverallLatency().update(System.nanoTime() - startTime);
        }
    }

    private Optional<AuthorizationResponse> doAuthorization(WSRequestContext context,
                                                  AuthenticationInfo authInfo,
                                                  @Nullable NamespaceKey namespaceKey,
                                                  @Nullable String bucketName,
                                                  String compartmentId,
                                                  BucketPublicAccessType bucketAccessType,
                                                  CasperOperation op,
                                                  TaggingOperation taggingOperation,
                                                  TagSet newTagSet,
                                                  TagSet oldTagSet,
                                                  KmsKeyUpdateAuth kmsKeyUpdateAuth,
                                                  boolean authorizeAll,
                                                  CasperPermission... permissions) {
        context.getCommonRequestContext().getMetricScope()
            .annotate("compartmentId", compartmentId)
            .annotate("operation", op.getSummary());

        //Place compartmentId in routing context for metering and log it
        context.setCompartmentID(compartmentId);

        //Place the compartment name in routing context for audit and log.
        try {
            final Optional<Compartment> compartment = metadataClient.getCompartment(compartmentId,
                    context.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE));
            context.setCompartment(compartment.orElse(null));
        } catch (Throwable t) {
            LOG.error("Exception trying to get compartment {} from metadata client", t, compartmentId);
        }

        //Check our embargos first to avoid hitting authorization service
        context.getVisa().ifPresent(visa -> embargo.customs(context.getCommonRequestContext(), visa, authInfo, op));

        //Place tenantOCID in the context for metering to use
        context.setRequestorTenantOcid(authInfo.getMainPrincipal().getTenantId());

        // TODO(jfriedly):  Audit usage of the V1_USER and remove it if possible
        if (authInfo == AuthenticationInfo.SUPER_USER || authInfo == AuthenticationInfo.V1_USER) {
            LOG.debug("Super or V1 user by-passing authz for operation '{}', using permissions '{}'.", op, permissions);
            return Optional.of(new AuthorizationResponse(null, newTagSet, true,
                new HashSet<>(Arrays.asList(permissions))));
        }

        final String tenantOcid = authInfo.getMainPrincipal().getTenantId();

        final boolean bucketAllowsPublicAccess = bucketAccessType != null &&
            PublicOperations.bucketAllowsPublicAccess(op, namespaceKey, bucketName, bucketAccessType);

        if (bucketAllowsPublicAccess) {
            return Optional.of(new AuthorizationResponse(null, newTagSet, true,
                new HashSet<>(Arrays.asList(permissions))));

        } else if (authInfo == AuthenticationInfo.ANONYMOUS_USER) {
            LOG.debug("Anonymous user rejected for operation {} against bucket {} in namespace {}",
                op, bucketName, namespaceKey);
            return Optional.empty();
        }

        final String namespace = context.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE);
        if (isSuspended(op, compartmentId, namespace)) {
            LOG.debug("The tenant is suspended, and has failed authorization");
            return Optional.empty();
        }

        final MetricScope rootScope = context.getCommonRequestContext().getMetricScope().getRoot();

        final String vcnID = serviceGateway.chooseVcnID(
            context.getVcnID().orElse(null), context.getVcnDebugID().orElse(null), tenantOcid).orElse(null);
        final String vcnOCID = serviceGateway.mappingFromVcnID(vcnID).orElse(null);

        if (vcnID != null) {
            rootScope.annotate("vcnHeader", vcnID);
            if (vcnOCID != null) {
                rootScope.annotate("vcnOcid", vcnOCID);
            }
        }

        if (vcnID != null && vcnOCID == null) {
            LOG.warn("VCN ID {} was present, but there was no mapping for it.", vcnID);
            return Optional.empty();
        }

        //CASPER-5633 We are trying to move to new service name, but we shouldn't fail requests targeting the old name
        final boolean useOldServiceName = useOldServiceName(authInfo);
        if (useOldServiceName) {
            LOG.warn("AuthZ using old service name '{}'.", WebServerAuths.OLD_AUTHORIZATION_SERVICE_NAME);
            WebServerMetrics.OLD_SERVICE_NAME_REQUESTS.inc();
            return makeAuthRequest(oldAuthorizationClient, WebServerAuths.OLD_AUTHORIZATION_SERVICE_NAME, context,
                authInfo, namespaceKey, bucketName, compartmentId, op, taggingOperation, newTagSet,
                oldTagSet, kmsKeyUpdateAuth, authorizeAll, vcnOCID, permissions);
        } else {
            return makeAuthRequest(newAuthorizationClient, serviceName, context, authInfo, namespaceKey, bucketName,
                compartmentId, op, taggingOperation, newTagSet, oldTagSet, kmsKeyUpdateAuth, authorizeAll,
                vcnOCID, permissions);
        }
    }

    private Optional<AuthorizationResponse> doAuthorization(MetricScope rootScope,
            AuthenticationInfo authInfo,
                                                            @Nullable NamespaceKey namespaceKey,
                                                            @Nullable String bucketName,
                                                            String compartmentId,
                                                            BucketPublicAccessType bucketAccessType,
                                                            CasperOperation op,
                                                            TaggingOperation taggingOperation,
                                                            TagSet newTagSet,
                                                            TagSet oldTagSet,
                                                            KmsKeyUpdateAuth kmsKeyUpdateAuth,
                                                            boolean authorizeAll,
                                                            Set<CasperPermission> permissions,
                                                            String namespace,
                                                            String vcnId,
                                                            String vcnDebugId,
                                                            EmbargoContext embargoContext) {
        rootScope
                .annotate("compartmentId", compartmentId)
                .annotate("operation", op.getSummary());

        //Place the compartment name in routing context for audit and log.
//        try {
//            final Optional<Compartment> compartment = metadataClient.getCompartment(compartmentId,
//                    context.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE));
//            context.setCompartment(compartment.orElse(null));
//        } catch (Throwable t) {
//            LOG.error("Exception trying to get compartment {} from metadata client", t, compartmentId);
//        }

        //Check our embargos first to avoid hitting authorization service
        embargo.customs(rootScope, embargoContext, authInfo, op);

        // TODO(jfriedly):  Audit usage of the V1_USER and remove it if possible
        if (authInfo == AuthenticationInfo.SUPER_USER || authInfo == AuthenticationInfo.V1_USER) {
            LOG.debug("Super or V1 user by-passing authz for operation '{}', using permissions '{}'.", op, permissions);
            return Optional.of(new AuthorizationResponse(null, newTagSet, true, permissions));
        }

        final String tenantOcid = authInfo.getMainPrincipal().getTenantId();

        final boolean bucketAllowsPublicAccess = bucketAccessType != null &&
                PublicOperations.bucketAllowsPublicAccess(op, namespaceKey, bucketName, bucketAccessType);

        if (bucketAllowsPublicAccess) {
            return Optional.of(new AuthorizationResponse(null, newTagSet, true, permissions));

        } else if (authInfo == AuthenticationInfo.ANONYMOUS_USER) {
            LOG.debug("Anonymous user rejected for operation {} against bucket {} in namespace {}",
                    op, bucketName, namespaceKey);
            return Optional.empty();
        }

        if (isSuspended(op, compartmentId, namespace)) {
            LOG.debug("The tenant is suspended, and has failed authorization");
            return Optional.empty();
        }

        final String vcnID = serviceGateway.chooseVcnID(
                vcnId, vcnDebugId, tenantOcid).orElse(null);
        final String vcnOCID = serviceGateway.mappingFromVcnID(vcnID).orElse(null);

        if (vcnID != null) {
            rootScope.annotate("vcnHeader", vcnID);
            if (vcnOCID != null) {
                rootScope.annotate("vcnOcid", vcnOCID);
            }
        }

        if (vcnID != null && vcnOCID == null) {
            LOG.warn("VCN ID {} was present, but there was no mapping for it.", vcnID);
            return Optional.empty();
        }

        //CASPER-5633 We are trying to move to new service name, but we shouldn't fail requests targeting the old name
        final boolean useOldServiceName = useOldServiceName(authInfo);
        if (useOldServiceName) {
            LOG.warn("AuthZ using old service name '{}'.", WebServerAuths.OLD_AUTHORIZATION_SERVICE_NAME);
            WebServerMetrics.OLD_SERVICE_NAME_REQUESTS.inc();
            return makeAuthRequest(oldAuthorizationClient, WebServerAuths.OLD_AUTHORIZATION_SERVICE_NAME, rootScope,
                    authInfo, namespaceKey, bucketName, compartmentId, op, taggingOperation, newTagSet,
                    oldTagSet, kmsKeyUpdateAuth, authorizeAll, vcnOCID, null, namespace, permissions);
        } else {
            return makeAuthRequest(newAuthorizationClient, serviceName, rootScope, authInfo, namespaceKey, bucketName,
                    compartmentId, op, taggingOperation, newTagSet, oldTagSet, kmsKeyUpdateAuth, authorizeAll,
                    vcnOCID, null, namespace, permissions);
        }
    }

    /**
     * CASPER-5633 We are trying to move to new service name, but we shouldn't fail requests targeting the old "Casper"
     * name.  This method decides which service name to use for this request.
     */
    private static boolean useOldServiceName(AuthenticationInfo authInfo) {
        switch (authInfo.getRequestKind()) {
            case USER:
            case INSTANCE:
            case SERVICE:
                //always use new service name for normal auth
                return false;
            case OBO:
            case DELEGATION:
                final Principal userPrincipal = authInfo.getUserPrincipal().get();
                //targetServiceNamesStr should look like
                final Optional<String> targetServiceNamesStr =
                    userPrincipal.getClaimValue(ClaimType.TARGET_SVC_NAMES.value());
                //fallback to old service name if not present
                if (!targetServiceNamesStr.isPresent()) {
                    LOG.warn("Target service name claim not found in user principal in {} request",
                        authInfo.getRequestKind());
                    return true;
                }
                //targetServiceNamesStr looks like ["Casper","SomeServiceName"]
                //use old service name if the string "Casper" (with quote) is in there
                return targetServiceNamesStr.get().toLowerCase(Locale.US)
                    .contains("\"" + WebServerAuths.OLD_AUTHORIZATION_SERVICE_NAME.toLowerCase(Locale.US) + "\"");
            default:
                throw new RuntimeException("Unknown auth request kind " + authInfo.getRequestKind());
        }
    }

    private Optional<AuthorizationResponse> makeAuthRequest(AuthorizationClient authorizationClient,
                                                            String serviceName,
                                                            WSRequestContext context,
                                                            AuthenticationInfo authInfo,
                                                            NamespaceKey namespaceKey,
                                                            String bucketName,
                                                            String compartmentId,
                                                            CasperOperation op,
                                                            TaggingOperation taggingOperation,
                                                            TagSet newTagSet,
                                                            TagSet oldTagSet,
                                                            KmsKeyUpdateAuth kmsKeyUpdateAuth,
                                                            boolean authorizeAll,
                                                            String vcnOCID,
                                                            CasperPermission... permissions) {
        final MetricScope rootScope = context.getCommonRequestContext().getMetricScope().getRoot();
        final AuthorizationRequest request = applyTags(
            AuthorizationRequestFactory.newRequest(
                serviceName,
                authInfo.getRequestKind(),
                authInfo.getUserPrincipal().orElse(null),
                authInfo.getServicePrincipal().orElse(null)),
            taggingOperation,
            newTagSet,
            oldTagSet);

        request.setActionKind(op.getActionKind());

        final String[] resourceKinds = Arrays.stream(permissions).map(CasperPermission::getResourceKind)
                .filter(Objects::nonNull)
                .map(CasperResourceKind::getName)
                .distinct()
                .toArray(String[]::new);
        //Add associations for the operations being used
        //https://jira.oci.oraclecorp.com/browse/CASPER-3017
        //https://confluence.oci.oraclecorp.com/display/~dmvogel/Authorization+Mark+Three+for+Service+Owners
        //https://confluence.oci.oraclecorp.com/display/~hailuo/cross+tenancy+policy
        request.addVariable(OptionalVariableFactory.resourceKinds(resourceKinds));

        for (CasperPermission permission : permissions) {
            request.addPermission(Permission.get(permission.name()));
        }

        //cross tenancy support _mostly_ just works, but there were complications for certain bad cases (associations)
        //which require additional work.
        //However, cross-tenancy support to date has been somewhat user hostile (due to security needs):
        //you need to write `endorse` & `admit`, but the ability to write `endorse` statements was on a whitelisted
        //tenancy basis; and to make a cross-tenancy request, you must include a header specifying
        //the ocid of the cross-tenancy target.
        //https://confluence.oci.oraclecorp.com/display/~dmvogel/Authority+of+Association+-+November+2017 talks about
        //the certain bad cases and how to resolve.
        //What we want to do to make generally available cross-tenancy less user hostile is to explicitly indicate
        //the requests which are not bad cases (unfortunate).
        // That’s the OPERATION_ASSOCIATION_REVIEWED thing;
        // “I affirm that this API is okay for cross-tenancy purposes”, and then the header is no longer required.
        request.addVariable(
            ContextVariableFactory.getTrue(FixedVariableNames.OPERATION_ASSOCIATION_REVIEWED.toString()));

        request.addCompartmentId(compartmentId);
        request.addVariable(MandatoryVariableFactory.operation(op.getSummary()));

        //Annotate the identity request id
        rootScope.annotate(IDENTITY_REQUEST_ID_KEY, request.getRequestId());
        final String tenantOcid = authInfo.getMainPrincipal().getTenantId();

        /*
         * This is the X_REAL_IP of the client if the request originated through a loadbalancer or the client's
         * ip directly otherwise
         * https://confluence.oci.oraclecorp.com/display/PLAT/SUBNET+TYPE+VARIABLE
         */
        final String ipAddress = context
            .getDebugIP()
            .filter(ip -> tenantCanUseDebugIP(ip, tenantOcid))
            .orElse(context.getRealIP());
        request.addVariable(ContextVariableFactory.subnet(IP_ADDRESS_VARIABLE, ipAddress));

        // To be deprecated
        // Include a 'NULL' VCN ID in Identity context when traffic is not coming from Service Gateway
        // For more details, see https://jira.oci.oraclecorp.com/browse/CASPER-4714
        if (vcnOCID != null) {
            request.addVariable(ContextVariableFactory.entity(ServiceGateway.VCN_IDENTITY_VARIABLE, vcnOCID));
        } else {
            request.addVariable(ContextVariableFactory.entity(ServiceGateway.VCN_IDENTITY_VARIABLE, "NULL"));
        }

        // For more info, refer: https://jira.oci.oraclecorp.com/browse/CASPER-7056
        if (vcnOCID != null) {
            request.addVariable(ContextVariableFactory.entity(REQUEST_NETWORK_SOURCE_TYPE, "vcn"));
            request.addVariable(ContextVariableFactory.entity(REQUEST_NETWORK_SOURCE_VCN_ID, vcnOCID));
        } else {
            request.addVariable(ContextVariableFactory.entity(REQUEST_NETWORK_SOURCE_TYPE, "public"));
        }
        request.addVariable(ContextVariableFactory.entity(REQUEST_NETWORK_SOURCE_IP, ipAddress));

        final String kmsKeyId = kmsKeyUpdateAuth.getKmsKeyUpdate();
        if (!Strings.isNullOrEmpty(kmsKeyId)) {
            // TODO: will replace "request.kms-key.id" with the new variable defined in FixedVariableNames
            request.addVariable(ContextVariableFactory.entity(REQUEST_KMSKEY_ID, kmsKeyId));
        }

        if (bucketName != null) {
            request.addVariable(ContextVariableFactory.string("target.bucket.name", bucketName));
        }

        authInfo.getUserPrincipal().ifPresent(pr ->
                LOG.debug("Authorizing user '{}' to access compartment '{}' for operation '{}', " +
                        "using permissions '{}'.", pr.getSubjectId(), compartmentId, op, permissions));

        authInfo.getServicePrincipal().ifPresent(pr ->
            LOG.debug("Authorizing service '{}' to access compartment '{}' for operation '{}', " +
                    "using permissions '{}'.", pr.getSubjectId(), compartmentId, op, permissions));

        final Map<String, KeyDelegatePermission> keyDelegatePermissions = kmsKeyUpdateAuth.getKeyDelegatePermissions();
        Preconditions.checkState(keyDelegatePermissions.size() <= 2,
                "Support at most two KeyDelegatePermission.");
        if (kms != null && !keyDelegatePermissions.isEmpty() && kmsKeyUpdateAuth.isPerformAssociationAuthz()) {
            /* How do we authorize operations that want to associate or disassociate keys with bucket?
             * We can NOT directly authorize those operation with identity, instead we first call KMS with information
             * about the key association/deassociation. KMS returns the AuthorizationRequest which should be passed to
             * identity.

             * This means we can end up with up to 3 AuthorizationRequests for bucket operations. If we are changing
             * the key associated with a bucket we have to request:
             *   1. KEY_DISASSOCIATE for the old key.
             *   2. KEY_ASSOCIATE for the new key.
             *   3. BUCKET_UPDATE to change the bucket.
             * The process for this is:
             *   1. We create an AuthorizationRequest for the operation being performed (i.e. UPDATE_BUCKET).
             *   2. The KmsKeyUpdateAuth object determines the additional set of key permissions to request.
             *   3. We pass the key permissions we need to the KMS getKeyDelegateAuthRequest method, which returns the
             *      AuthorizationRequests that should be passed to identity.
             *   4. All of the AuthorizationRequests are passed to the Identity makeAuthorizationCall method, which does
             *     the AuthZ.
             * This means we can get back multiple responses from the AuthZ call. We have to loop through the responses
             * to make sure that ALL of them are allowed.
             */
            final List<AuthorizationRequest> requests = new ArrayList<>();
            for (Map.Entry<String, KeyDelegatePermission> entry : keyDelegatePermissions.entrySet()) {
                AuthorizationRequest kmsAuthZRequest = kms.getRemoteKms().getKeyDelegateAuthRequest(
                        request, entry.getKey(), entry.getValue());
                if (kmsAuthZRequest.getActionKind() == ActionKind.NOT_DEFINED) {
                    kmsAuthZRequest.setActionKind(op.getActionKind());
                }
                requests.add(kmsAuthZRequest);

                //Annotate the identity request id for the KMS authz request
                rootScope.annotate(IDENTITY_REQUEST_ID_KEY, kmsAuthZRequest.getRequestId());
            }

            requests.add(request);
            AssociationAuthorizationResponse associationAuthZResponse;
            try {
                associationAuthZResponse = makeAssociationAuthZCallWithRetryAndMetrics(
                        authorizationClient, requests,
                        context.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE));
            } catch (AuthServerUnavailableException e) {
                throw new AuthDataPlaneServerException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                        "Auth server unavailable", e);
            }

            if (associationAuthZResponse.getAssociationResult() != AssociationVerificationResult.SUCCESS) {
                LOG.debug("Association Authorization failed when trying to create bucket {} with kmsKey {} " +
                    "in compartment {}", bucketName, kmsKeyId, compartmentId);

                return Optional.empty();
            }
            // Associate new kmsKey response, dissociate old kmsKey response, bucket[create/update] response
            final List<com.oracle.pic.identity.authorization.sdk.AuthorizationResponse> responses =
                    associationAuthZResponse.getAuthorizationResponses();
            Preconditions.checkState(responses.size() == 2 || responses.size() == 3,
                                     "Either 2 or 3 authorization responses should return.");
            Preconditions.checkState(responses.size() == requests.size(),
                    "Authorization responses should match the requests.");

            Optional<AuthorizationResponse> response = parseAuthZResponse(requests.get(0), responses.get(0), rootScope,
                    op, namespaceKey, bucketName, authorizeAll);
            for (int i = 1; i < requests.size(); i++) {
                final AuthorizationRequest req = requests.get(i);
                final com.oracle.pic.identity.authorization.sdk.AuthorizationResponse resp = responses.get(i);
                response = response.flatMap(ignored ->
                    parseAuthZResponse(req, resp, rootScope, op, namespaceKey, bucketName, authorizeAll));
            }
            return response;
        }

        /*
         * https://jira.oci.oraclecorp.com/browse/CASPER-3265
         * Identity wants to now control the service code and messaging returned to clients for failed AuthZ calls.
         * This will now potentially change the service code & status code for AuthZ calls --
         * API board & Security team are both aware
         */
        final com.oracle.pic.identity.authorization.sdk.AuthorizationResponse response;
        try {
            response = makeAuthZCallWithRetryAndMetrics(
                    authorizationClient, request,
                    context.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE));
        } catch (AuthServerUnavailableException e) {
            throw new AuthDataPlaneServerException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                "Auth server unavailable", e);
        }

        return parseAuthZResponse(request, response, rootScope, op, namespaceKey, bucketName, authorizeAll);
    }

    private Optional<AuthorizationResponse> makeAuthRequest(AuthorizationClient authorizationClient,
                                                            String serviceName,
                                                            MetricScope rootScope,
                                                            AuthenticationInfo authInfo,
                                                            NamespaceKey namespaceKey,
                                                            String bucketName,
                                                            String compartmentId,
                                                            CasperOperation op,
                                                            TaggingOperation taggingOperation,
                                                            TagSet newTagSet,
                                                            TagSet oldTagSet,
                                                            KmsKeyUpdateAuth kmsKeyUpdateAuth,
                                                            boolean authorizeAll,
                                                            String vcnOCID,
                                                            String ipAddress,
                                                            String namespace,
                                                            Set<CasperPermission> permissions) {
        final AuthorizationRequest request = applyTags(
                AuthorizationRequestFactory.newRequest(
                        serviceName,
                        authInfo.getRequestKind(),
                        authInfo.getUserPrincipal().orElse(null),
                        authInfo.getServicePrincipal().orElse(null)),
                taggingOperation,
                newTagSet,
                oldTagSet);

        request.setActionKind(op.getActionKind());

        final String[] resourceKinds = permissions.stream().map(CasperPermission::getResourceKind)
                .filter(Objects::nonNull)
                .map(CasperResourceKind::getName)
                .distinct()
                .toArray(String[]::new);
        //Add associations for the operations being used
        //https://jira.oci.oraclecorp.com/browse/CASPER-3017
        //https://confluence.oci.oraclecorp.com/display/~dmvogel/Authorization+Mark+Three+for+Service+Owners
        //https://confluence.oci.oraclecorp.com/display/~hailuo/cross+tenancy+policy
        request.addVariable(OptionalVariableFactory.resourceKinds(resourceKinds));

        for (CasperPermission permission : permissions) {
            request.addPermission(Permission.get(permission.name()));
        }

        //cross tenancy support _mostly_ just works, but there were complications for certain bad cases (associations)
        //which require additional work.
        //However, cross-tenancy support to date has been somewhat user hostile (due to security needs):
        //you need to write `endorse` & `admit`, but the ability to write `endorse` statements was on a whitelisted
        //tenancy basis; and to make a cross-tenancy request, you must include a header specifying
        //the ocid of the cross-tenancy target.
        //https://confluence.oci.oraclecorp.com/display/~dmvogel/Authority+of+Association+-+November+2017 talks about
        //the certain bad cases and how to resolve.
        //What we want to do to make generally available cross-tenancy less user hostile is to explicitly indicate
        //the requests which are not bad cases (unfortunate).
        // That’s the OPERATION_ASSOCIATION_REVIEWED thing;
        // “I affirm that this API is okay for cross-tenancy purposes”, and then the header is no longer required.
        request.addVariable(
                ContextVariableFactory.getTrue(FixedVariableNames.OPERATION_ASSOCIATION_REVIEWED.toString()));

        request.addCompartmentId(compartmentId);
        request.addVariable(MandatoryVariableFactory.operation(op.getSummary()));

        //Annotate the identity request id
        rootScope.annotate(IDENTITY_REQUEST_ID_KEY, request.getRequestId());
        final String tenantOcid = authInfo.getMainPrincipal().getTenantId();

        request.addVariable(ContextVariableFactory.subnet(IP_ADDRESS_VARIABLE, ipAddress));

        // To be deprecated
        // Include a 'NULL' VCN ID in Identity context when traffic is not coming from Service Gateway
        // For more details, see https://jira.oci.oraclecorp.com/browse/CASPER-4714
        if (vcnOCID != null) {
            request.addVariable(ContextVariableFactory.entity(ServiceGateway.VCN_IDENTITY_VARIABLE, vcnOCID));
        } else {
            request.addVariable(ContextVariableFactory.entity(ServiceGateway.VCN_IDENTITY_VARIABLE, "NULL"));
        }

        // For more info, refer: https://jira.oci.oraclecorp.com/browse/CASPER-7056
        if (vcnOCID != null) {
            request.addVariable(ContextVariableFactory.entity(REQUEST_NETWORK_SOURCE_TYPE, "vcn"));
            request.addVariable(ContextVariableFactory.entity(REQUEST_NETWORK_SOURCE_VCN_ID, vcnOCID));
        } else {
            request.addVariable(ContextVariableFactory.entity(REQUEST_NETWORK_SOURCE_TYPE, "public"));
        }
        request.addVariable(ContextVariableFactory.entity(REQUEST_NETWORK_SOURCE_IP, ipAddress));

        final String kmsKeyId = kmsKeyUpdateAuth.getKmsKeyUpdate();
        if (!Strings.isNullOrEmpty(kmsKeyId)) {
            // TODO: will replace "request.kms-key.id" with the new variable defined in FixedVariableNames
            request.addVariable(ContextVariableFactory.entity(REQUEST_KMSKEY_ID, kmsKeyId));
        }

        if (bucketName != null) {
            request.addVariable(ContextVariableFactory.string("target.bucket.name", bucketName));
        }

        authInfo.getUserPrincipal().ifPresent(pr ->
                LOG.debug("Authorizing user '{}' to access compartment '{}' for operation '{}', " +
                        "using permissions '{}'.", pr.getSubjectId(), compartmentId, op, permissions));

        authInfo.getServicePrincipal().ifPresent(pr ->
                LOG.debug("Authorizing service '{}' to access compartment '{}' for operation '{}', " +
                        "using permissions '{}'.", pr.getSubjectId(), compartmentId, op, permissions));

        final Map<String, KeyDelegatePermission> keyDelegatePermissions = kmsKeyUpdateAuth.getKeyDelegatePermissions();
        Preconditions.checkState(keyDelegatePermissions.size() <= 2,
                "Support at most two KeyDelegatePermission.");
        if (kms != null && !keyDelegatePermissions.isEmpty() && kmsKeyUpdateAuth.isPerformAssociationAuthz()) {
            /* How do we authorize operations that want to associate or disassociate keys with bucket?
             * We can NOT directly authorize those operation with identity, instead we first call KMS with information
             * about the key association/deassociation. KMS returns the AuthorizationRequest which should be passed to
             * identity.

             * This means we can end up with up to 3 AuthorizationRequests for bucket operations. If we are changing
             * the key associated with a bucket we have to request:
             *   1. KEY_DISASSOCIATE for the old key.
             *   2. KEY_ASSOCIATE for the new key.
             *   3. BUCKET_UPDATE to change the bucket.
             * The process for this is:
             *   1. We create an AuthorizationRequest for the operation being performed (i.e. UPDATE_BUCKET).
             *   2. The KmsKeyUpdateAuth object determines the additional set of key permissions to request.
             *   3. We pass the key permissions we need to the KMS getKeyDelegateAuthRequest method, which returns the
             *      AuthorizationRequests that should be passed to identity.
             *   4. All of the AuthorizationRequests are passed to the Identity makeAuthorizationCall method, which does
             *     the AuthZ.
             * This means we can get back multiple responses from the AuthZ call. We have to loop through the responses
             * to make sure that ALL of them are allowed.
             */
            final List<AuthorizationRequest> requests = new ArrayList<>();
            for (Map.Entry<String, KeyDelegatePermission> entry : keyDelegatePermissions.entrySet()) {
                AuthorizationRequest kmsAuthZRequest = kms.getRemoteKms().getKeyDelegateAuthRequest(
                        request, entry.getKey(), entry.getValue());
                if (kmsAuthZRequest.getActionKind() == ActionKind.NOT_DEFINED) {
                    kmsAuthZRequest.setActionKind(op.getActionKind());
                }
                requests.add(kmsAuthZRequest);

                //Annotate the identity request id for the KMS authz request
                rootScope.annotate(IDENTITY_REQUEST_ID_KEY, kmsAuthZRequest.getRequestId());
            }

            requests.add(request);
            AssociationAuthorizationResponse associationAuthZResponse;
            try {
                associationAuthZResponse = makeAssociationAuthZCallWithRetryAndMetrics(
                        authorizationClient, requests, namespace);
            } catch (AuthServerUnavailableException e) {
                throw new AuthDataPlaneServerException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                        "Auth server unavailable", e);
            }

            if (associationAuthZResponse.getAssociationResult() != AssociationVerificationResult.SUCCESS) {
                LOG.debug("Association Authorization failed when trying to create bucket {} with kmsKey {} " +
                        "in compartment {}", bucketName, kmsKeyId, compartmentId);

                return Optional.empty();
            }
            // Associate new kmsKey response, dissociate old kmsKey response, bucket[create/update] response
            final List<com.oracle.pic.identity.authorization.sdk.AuthorizationResponse> responses =
                    associationAuthZResponse.getAuthorizationResponses();
            Preconditions.checkState(responses.size() == 2 || responses.size() == 3,
                    "Either 2 or 3 authorization responses should return.");
            Preconditions.checkState(responses.size() == requests.size(),
                    "Authorization responses should match the requests.");

            Optional<AuthorizationResponse> response = parseAuthZResponse(requests.get(0), responses.get(0), rootScope,
                    op, namespaceKey, bucketName, authorizeAll);
            for (int i = 1; i < requests.size(); i++) {
                final AuthorizationRequest req = requests.get(i);
                final com.oracle.pic.identity.authorization.sdk.AuthorizationResponse resp = responses.get(i);
                response = response.flatMap(ignored ->
                        parseAuthZResponse(req, resp, rootScope, op, namespaceKey, bucketName, authorizeAll));
            }
            return response;
        }

        /*
         * https://jira.oci.oraclecorp.com/browse/CASPER-3265
         * Identity wants to now control the service code and messaging returned to clients for failed AuthZ calls.
         * This will now potentially change the service code & status code for AuthZ calls --
         * API board & Security team are both aware
         */
        final com.oracle.pic.identity.authorization.sdk.AuthorizationResponse response;
        try {
            response = makeAuthZCallWithRetryAndMetrics(
                    authorizationClient, request, namespace);
        } catch (AuthServerUnavailableException e) {
            throw new AuthDataPlaneServerException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                    "Auth server unavailable", e);
        }

        return parseAuthZResponse(request, response, rootScope, op, namespaceKey, bucketName, authorizeAll);
    }

    private interface AuthSupplier<T> {
        T get() throws AuthServerUnavailableException;
    }

    private com.oracle.pic.identity.authorization.sdk.AuthorizationResponse makeAuthZCallWithRetryAndMetrics(
            AuthorizationClient authorizationClient,
            AuthorizationRequest request,
            String namespace) throws AuthServerUnavailableException {
        return authorizeWithRetryAndMetrics(() -> authorizationClient.makeAuthorizationCall(request), namespace);
    }

    private AssociationAuthorizationResponse makeAssociationAuthZCallWithRetryAndMetrics(
            AuthorizationClient authorizationClient,
            List<AuthorizationRequest> authorizationRequests,
            String namespace) throws AuthServerUnavailableException  {
        final AssociationAuthorizationRequest request = AssociationAuthorizationRequest.of(authorizationRequests);
        return authorizeWithRetryAndMetrics(() -> authorizationClient.makeAuthorizationCall(request), namespace);
    }

    private <T> T authorizeWithRetryAndMetrics(AuthSupplier<T> callable, String namespace)
            throws AuthServerUnavailableException {
        final StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        final String context = stackTraceElements[2].getClassName() + "::" + stackTraceElements[2].getMethodName();
        try {
            return Failsafe
                    .with(this.retryPolicy)
                    .get(() -> {
                        try (ResourceTicket resourceTicket =
                                     resourceLimiter.acquireResourceTicket(namespace, ResourceType.IDENTITY, context)) {
                            return this.authorizeWithMetrics(callable);
                        }
                    });
        } catch (FailsafeException e) {
            if (e.getCause() instanceof AuthServerUnavailableException) {
                throw (AuthServerUnavailableException) e.getCause();
            }
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw e;
        }
    }

    private <T> T authorizeWithMetrics(AuthSupplier<T> callable) throws AuthServerUnavailableException {
        final long startTime = System.nanoTime();
        try {
            AuthMetrics.IDENTITY_AUTHZ.getRequests().inc();
            return callable.get();
        } catch (AuthorizationClientException | AuthClientException e) {
            AuthMetrics.IDENTITY_AUTHZ.getClientErrors().inc();
            throw e;
        } catch (Throwable throwable) {
            AuthMetrics.IDENTITY_AUTHZ.getServerErrors().inc();
            throw throwable;
        } finally {
            AuthMetrics.IDENTITY_AUTHZ.getOverallLatency().update(System.nanoTime() - startTime);
        }
    }

    private Optional<AuthorizationResponse> parseAuthZResponse(
            AuthorizationRequest request,
            com.oracle.pic.identity.authorization.sdk.AuthorizationResponse response,
            MetricScope rootScope,
            CasperOperation op,
            NamespaceKey namespaceKey,
            String bucketName,
            boolean authorizeAll) {
        AuthorizationResponseResult responseResult = response.getAuthorizationResponseResult();
        AuthorizationResponseResult.AuthorizationResponseErrorCode errorCode = responseResult.getErrorCode();
        Response.Status status = responseResult.getStatus();
        String errorMessage = "";

        final Optional<AuthorizationResponse> authorizationResponse;
        switch (responseResult.getErrorCategory()) {
            case EMPTY:
                errorMessage = "EmptyAuthorizer authorization response for request: " + response.getRequestId();
                authorizationResponse = Optional.empty();
                break;
            case TAG_AUTHORIZATION_OR_NOT_EXIST_ERROR:
                errorMessage = "Tag authorization failed for: " + response.getTagErrorMessage().orElse("")
                               + " for request: " + response.getRequestId();
                authorizationResponse = Optional.of(new AuthorizationResponse(null, null, false,
                    response.getRequestId(), response.getTagErrorMessage().orElse(null)));
                AuthMetrics.WEB_SERVER_AUTHZ.getClientErrors().inc();
                break;
            case TAG_VALIDATION_ERROR:
                errorMessage = "Tag validation failed for: " + response.getTagErrorMessage().orElse("")
                               + " for request: " + response.getRequestId();
                authorizationResponse = Optional.of(new AuthorizationResponse(null, null, false,
                    response.getRequestId(), response.getTagErrorMessage().orElse(null)));
                AuthMetrics.WEB_SERVER_AUTHZ.getClientErrors().inc();
                break;
            default:
                // use authZ method you are currently using which may be one of the followings:
                //     response.authorizeAllPermissions()
                //     response.authorizeAnyPermissions()
                //     response.authorizeSetOfPermissions()
                Supplier<Boolean> isAuthorizedFunction = authorizeAll ?
                    response::authorizeAllPermissions : response::authorizeAnyPermission;

                if (isAuthorizedFunction.get()) {
                    // authorized operation so return success to continue the operation
                    return Optional.of(AuthorizationResponse.fromSdkResponse(response));
                } else {
                    Set<Permission> requestPermissions = request.getPermissions();
                    Sets.SetView<Permission> diff = Sets.difference(requestPermissions, response.getPermissions());
                    String diffPermissionErrorMessage =
                        "Authorization failed for missing permissions: " +
                        diff.stream().map(Object::toString).collect(Collectors.joining(","));
                    LOG.debug("AuthZ operation {} against bucket {} in namespace {} rejected: {}",
                              op, bucketName, namespaceKey, diffPermissionErrorMessage);
                    errorCode = AuthorizationResponseResult.AuthorizationResponseErrorCode.NOT_AUTHORIZED_OR_NOT_FOUND;
                    status = Response.Status.NOT_FOUND;
                    errorMessage = "Not authorized or not found";
                    authorizationResponse = Optional.empty();
                }
        }

        //Annotate the identity request id for any failing request
        rootScope.annotate("identityRequestId", response.getRequestId());

        /*
         * Lets annotate the identity error message & status code for posterity --
         * The change to pass through Identity's message & status code were introduced as part of
         * https://jira.oci.oraclecorp.com/browse/CASPER-3265
         * We've discovered that the number of clients whom might experience breaking changes on account of the
         * different HTTP status (including DBaaS) is to great.
         * We have therefore re-introduced our previous custom status handling outside of AuthorizerImpl
         */
        rootScope.annotate("identityErrorCode", errorCode.getErrorMessage());
        rootScope.annotate("identityStatusCode", status.getStatusCode());
        rootScope.annotate("identityErrorMessage", errorMessage);
        //Commented out as we have backed out this feature
        //throw new AuthorizationException(status.getStatusCode(), errorCode.getErrorMessage(), errorMessage);
        return authorizationResponse;
    }

    private AuthorizationRequest applyTags(AuthorizationRequest request,
                                           TaggingOperation taggingOperation,
                                           TagSet newTagSet,
                                           TagSet oldTagSet) {
        if (taggingOperation == null) {
            return request;
        }
        if (taggingOperation == TaggingOperation.SET_NEW_TAGS) {
            request = AuthorizationRequestFactory.setNewTags(request, TaggingClientHelper.toByteArray(newTagSet));
        } else if (taggingOperation == TaggingOperation.SET_CHANGE_TAGS) {
            request = AuthorizationRequestFactory.setChangeTags(request,
                    TaggingClientHelper.toByteArray(oldTagSet),
                    TaggingClientHelper.toByteArray(newTagSet));
        } else if (taggingOperation == TaggingOperation.SET_PURGE_TAGS) {
            request.addVariable(ContextVariableFactory.string(
                    FixedVariableNames.OPERATION_TAGGING_DEFINEDTAGS_EXCLUSIVE, "true"));
            request = AuthorizationRequestFactory.setPurgeTags(request, TaggingClientHelper.toByteArray(oldTagSet));
        } else if (taggingOperation == TaggingOperation.SET_EXISTING_TAGS) {
            request = AuthorizationRequestFactory.setExistingTags(request, TaggingClientHelper.toByteArray(oldTagSet));
        }
        return request;
    }

    /**
     * A predicate that evaluates to true if and only if the provided OCID can use the provided debug IP.
     * @param debugIP The debug IP.
     * @param tenantOCID The OCID to check.
     * @return True of the OCID can use the debug IP and false otherwise.
     */
    private boolean tenantCanUseDebugIP(String debugIP, String tenantOCID) {
        return testTenants.tenantName(tenantOCID)
                .map(name -> {
                    LOG.info("Using IP {} for test tenant {}", debugIP, name);
                    return true;
                })
                .orElse(false);
    }
}
