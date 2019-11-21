package com.oracle.pic.casper.webserver.api.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.oracle.pic.casper.common.config.failsafe.AuthDataPlaneFailSafeConfig;
import com.oracle.pic.casper.common.config.failsafe.FailSafeConfig;
import com.oracle.pic.casper.common.config.v2.AuthDataPlaneClientConfiguration;
import com.oracle.pic.casper.common.exceptions.InternalServerErrorException;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.vertx.HttpServerUtil;
import com.oracle.pic.casper.webserver.auth.dataplane.AuthMetrics;
import com.oracle.pic.casper.webserver.auth.dataplane.SwiftAuthenticator;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.webserver.limit.ResourceLimitHelper;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.limit.ResourceType;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.commons.metadata.entities.User;
import com.oracle.pic.identity.authentication.AuthenticatorClient;
import com.oracle.pic.identity.authentication.AuthenticatorError;
import com.oracle.pic.identity.authentication.Claim;
import com.oracle.pic.identity.authentication.ClaimType;
import com.oracle.pic.identity.authentication.Constants;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.SecurityContext;
import com.oracle.pic.identity.authentication.SecurityContextImpl;
import com.oracle.pic.identity.authentication.error.AuthClientException;
import com.oracle.pic.identity.authentication.signedRequest.AuthHeaderParser;
import com.oracle.pic.identity.authentication.signedRequest.SignedSignatureInfo;
import com.oracle.pic.identity.authorization.sdk.AuthorizationRequest;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import net.jodah.failsafe.Failsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * An {@link Authenticator} that makes a request to the internal OPC authentication service.
 */
public class AuthenticatorImpl implements Authenticator {
    private static final Logger LOG = LoggerFactory.getLogger(AuthenticatorImpl.class);

    private final AuthenticatorClient authenticatorClient;
    private final AuthHeaderParser authHeaderParser;
    private final FailSafeConfig failSafeConfig;
    private final ExecutorService executor;
    private final TimeLimiter timeLimiter;
    private final ResourceControlledMetadataClient metadataClient;
    private final SwiftAuthenticator swiftAuthenticator;
    private final JWTAuthenticator jwtAuthenticator;
    private final ResourceLimiter resourceLimiter;

    public AuthenticatorImpl(AuthenticatorClient authenticatorClient,
                             ResourceControlledMetadataClient metadataClient,
                             AuthDataPlaneClientConfiguration config,
                             SwiftAuthenticator swiftAuthenticator,
                             JWTAuthenticator jwtAuthenticator,
                             ResourceLimiter resourceLimiter) {
        this.authenticatorClient = authenticatorClient;
        this.jwtAuthenticator = jwtAuthenticator;
        this.authHeaderParser = new AuthHeaderParser();
        this.failSafeConfig = new AuthDataPlaneFailSafeConfig(config.getFailSafeConfiguration());
        this.metadataClient = metadataClient;
        this.executor = Executors.newCachedThreadPool();
        this.timeLimiter = SimpleTimeLimiter.create(executor);
        this.swiftAuthenticator = swiftAuthenticator;
        this.resourceLimiter = resourceLimiter;
    }

    /**
     * The various authorization scheme we support
     */
    enum AuthorizationType {

        /**
         * https://tools.ietf.org/html/rfc7617
         */
        BASIC("Basic"),

        /**
         * https://tools.ietf.org/html/draft-cavage-http-signatures-10
         */
        SIGNATURE("Signature"),

        /**
         * https://tools.ietf.org/html/rfc6750
         */
        BEARER("Bearer");

        private final String label;

        AuthorizationType(String label) {
            this.label = label;
        }

        public boolean is(String other) {
            return label.equalsIgnoreCase(other);
        }

        public static @Nullable AuthorizationType from(String label) {
            for (AuthorizationType type : values()) {
                if (type.is(label)) {
                    return type;
                }
            }
            return null;
        }
    }

    @Override
    public AuthenticationInfo authenticate(RoutingContext context) {
        return wrap(context, () -> {
            final HttpServerRequest request = context.request();
            final URI uri;
            try {
                uri = URI.create(request.absoluteURI());
            } catch (Exception e) {
                throw new NotAuthenticatedException(e.getMessage());
            }
            final ImmutableMap<String, List<String>> headers = HttpServerUtil.getHeaders(request);
            final WSRequestContext wsRequestContext = WSRequestContext.get(context);
            return ResourceLimitHelper.withResourceControl(resourceLimiter, ResourceType.IDENTITY,
                    wsRequestContext.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE),
                    () -> {
                final SecurityContext securityContext = authenticatorClient.authenticate(
                        request.method().name(),
                        uri,
                        headers,
                        Optional.empty());

                return authenticateInternal(context, securityContext, AuthenticationMode.V2);
            });
        });
    }

    @Override
    public AuthenticationInfo authenticate(RoutingContext context, String bodySha256) {
        return wrap(context, () -> executeAuthenticate(context, bodySha256));
    }

    private AuthenticationInfo executeAuthenticate(RoutingContext context, String bodySha256) {
        final HttpServerRequest request = context.request();

        final URI uri = URI.create(request.absoluteURI());
        final ImmutableMap<String, List<String>> headers = HttpServerUtil.getHeaders(request);
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        return ResourceLimitHelper.withResourceControl(resourceLimiter, ResourceType.IDENTITY,
                wsRequestContext.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE),
                () -> {
            final SecurityContext securityContext = authenticatorClient.authenticate(
                    request.method().name(),
                    uri,
                    headers,
                    Optional.ofNullable(bodySha256));

            return authenticateInternal(context, securityContext, AuthenticationMode.V2);
        });
    }

    @Override
    public AuthenticationInfo authenticatePutObject(RoutingContext context) {
        return wrap(context, () -> executeAuthenticatePutObject(context));
    }

    @Override
    public AuthenticationInfo authenticatePutObject(javax.ws.rs.core.HttpHeaders headersOrig, String uriString, String namespace, String method, MetricScope metricScope) {
        return wrap(headersOrig, () -> executeAuthenticatePutObject(headersOrig, uriString, namespace, method, metricScope));
    }


    private AuthenticationInfo executeAuthenticatePutObject(RoutingContext context) {
        final HttpServerRequest request = context.request();
        final URI uri = URI.create(request.absoluteURI());
        final ImmutableMap<String, List<String>> headers = HttpServerUtil.getHeaders(request);
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        return ResourceLimitHelper.withResourceControl(resourceLimiter, ResourceType.IDENTITY,
                wsRequestContext.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE),
                () -> {
            final SecurityContext securityContext = authenticatorClient.authenticateCasper(
                    request.method().name(),
                    uri,
                    headers,
                    Optional.empty());

            return authenticateInternal(context, securityContext, AuthenticationMode.V2);
        });
    }

    private AuthenticationInfo executeAuthenticatePutObject(javax.ws.rs.core.HttpHeaders headersOrig, String uriString, String namespace, String method, MetricScope metricScope) {
        final URI uri = URI.create(uriString);
        final ImmutableMap<String, List<String>> headers =
                new ImmutableMap.Builder<String, List<String>>().putAll(headersOrig.getRequestHeaders()).build();

        return ResourceLimitHelper.withResourceControl(resourceLimiter, ResourceType.IDENTITY,
                namespace,
                () -> {
                    final SecurityContext securityContext = authenticatorClient.authenticateCasper(
                            method,
                            uri,
                            headers,
                            Optional.empty());

                    return authenticateInternal(headersOrig, metricScope, securityContext, AuthenticationMode.V2, namespace);
                });
    }

    private AuthenticationInfo executeAuthenticateSwift(RoutingContext context, String namespace,
                                                        @Nullable String tenantOcid) {
        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        return ResourceLimitHelper.withResourceControl(resourceLimiter, ResourceType.IDENTITY,
                wsRequestContext.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE),
                () -> {
                    final Principal principal;
                    try {
                        principal = swiftAuthenticator.authenticate(
                                tenantOcid, namespace, HttpServerUtil.getHeaders(request));
                    } catch (com.oracle.pic.casper.webserver.auth.dataplane.exceptions.NotAuthenticatedException ex) {
                        throw new NotAuthenticatedException(ex.getMessage(), ex);
                    }
                    final SecurityContext sc = SecurityContextImpl.success(principal);
                    return authenticateInternal(context, sc, AuthenticationMode.SWIFT);
                });
    }

    @Override
    public AuthenticationInfo authenticateSwiftOrCavage(RoutingContext context,
                                                        @Nullable String bodySha256,
                                                        String namespace,
                                                        @Nullable String tenantOcid) {
        return authenticateAllowedTypes(context, bodySha256, namespace, tenantOcid, false,
                ImmutableList.of(AuthorizationType.BASIC, AuthorizationType.SIGNATURE));
    }

    @Override
    public AuthenticationInfo authenticateSwiftOrCavageForPutObject(RoutingContext context,
                                                                    String namespace,
                                                                    @Nullable String tenantOcid) {
        return authenticateAllowedTypes(context, null, namespace, tenantOcid, true,
                ImmutableList.of(AuthorizationType.BASIC, AuthorizationType.SIGNATURE));
    }

    @Override
    public AuthenticationInfo authenticateCavageOrJWT(RoutingContext context, @Nullable String bodySha256) {
        return authenticateAllowedTypes(context, bodySha256, null, null, false,
                ImmutableList.of(AuthorizationType.BEARER, AuthorizationType.SIGNATURE));
    }

    private AuthenticationInfo authenticateAllowedTypes(RoutingContext context,
                                                        @Nullable String bodySha256,
                                                        @Nullable String namespace,
                                                        @Nullable String tenantOcid,
                                                        boolean isPutObject,
                                                        List<AuthorizationType> allowedTypes) {
        return wrap(context, () -> {
            final String authorization = context.request().headers().get(HttpHeaders.AUTHORIZATION);
            int idx = authorization.indexOf(' ');
            if (idx <= 0) {
                LOG.debug("Malformed Authorization header {}", authorization);
                throw new NotAuthenticatedException(null);
            }
            AuthorizationType type = AuthorizationType.from(authorization.substring(0, idx));
            if (type == null) {
                LOG.debug("Unknown Authorization type {}", authorization);
                throw new NotAuthenticatedException(null);
            }

            if (!allowedTypes.contains(type)) {
                throw new NotAuthenticatedException("Authorization type: " + type + " not allowed.");
            }

            switch (type) {
                case BASIC:
                    return executeAuthenticateSwift(context, namespace, tenantOcid);
                case SIGNATURE:
                    if (isPutObject) {
                        return executeAuthenticatePutObject(context);
                    }
                    return executeAuthenticate(context, bodySha256);
                case BEARER:
                    //bearer tokens are a nice way for internal engineers
                    //to authenticate as an IAM user without providing a full cavage signature.
                    final String jwtEncoded = authorization.substring(idx + 1);
                    return jwtAuthenticator.authenticate(jwtEncoded);
                default:
                    throw new IllegalStateException("This should never be hit.");
            }
        });
    }

    private AuthenticationInfo wrap(RoutingContext routingContext, Supplier<AuthenticationInfo> supplier) {
        long startTime = System.nanoTime();
        try {
            if (!routingContext.request().headers().contains(HttpHeaders.AUTHORIZATION)) {
                LOG.debug("request from anonymous user.");
                return AuthenticationInfo.ANONYMOUS_USER;
            }
            AuthMetrics.AUTHENTICATION.getRequests().inc();
            AuthenticationInfo response = Failsafe.with(failSafeConfig.getRetryPolicy())
                    .get(supplier::get);
            return response;
        } catch (NotAuthenticatedException ex) {
            AuthMetrics.AUTHENTICATION.getClientErrors().inc();
            throw ex;
        } catch (Throwable throwable) {
            AuthMetrics.AUTHENTICATION.getServerErrors().inc();
            // rethrow the exception
            throw throwable;
        } finally {
            AuthMetrics.AUTHENTICATION.getOverallLatency().update(System.nanoTime() - startTime);
        }
    }

    private AuthenticationInfo wrap(javax.ws.rs.core.HttpHeaders headers, Supplier<AuthenticationInfo> supplier) {
        long startTime = System.nanoTime();
        try {
            if (!headers.getRequestHeaders().containsKey(HttpHeaders.AUTHORIZATION.toString())) {
                LOG.debug("request from anonymous user.");
                return AuthenticationInfo.ANONYMOUS_USER;
            }
            AuthMetrics.AUTHENTICATION.getRequests().inc();
            AuthenticationInfo response = Failsafe.with(failSafeConfig.getRetryPolicy())
                    .get(supplier::get);
            return response;
        } catch (NotAuthenticatedException ex) {
            AuthMetrics.AUTHENTICATION.getClientErrors().inc();
            throw ex;
        } catch (Throwable throwable) {
            AuthMetrics.AUTHENTICATION.getServerErrors().inc();
            // rethrow the exception
            throw throwable;
        } finally {
            AuthMetrics.AUTHENTICATION.getOverallLatency().update(System.nanoTime() - startTime);
        }
    }

    protected AuthenticationInfo authenticateInternal(RoutingContext routingContext,
                                                      SecurityContext securityContext,
                                                      AuthenticationMode authenticationMode) {
        if (!securityContext.isSuccess()) {
            final AuthenticatorError error = securityContext.error()
                    .orElse(AuthenticatorError.AUTH_SERVICE_UNAVAILABLE);
            final String errorMsg = securityContext.errorMessage() == null ?
                    "Authentication error" : securityContext.errorMessage();
            WSRequestContext.getCommonRequestContext(routingContext).getMetricScope()
                .annotate("authErrorMessage", errorMsg);

            switch (error) {
                case MISSING_AUTHENTICATION_INFO:
                    throw new NotAuthenticatedException("The Authorization header must be present");

                case EXPIRED_AUTHENTICATION_INFO:
                case DATE_OUTSIDE_CLOCK_SKEW:
                    throw new NotAuthenticatedException(
                            "The Authorization header has a date that is either too early or too late," +
                                    " check that your local clock is correct");

                case SIGNATURE_NOT_VALID:
                case INVALID_AUTHENTICATION_INFO:
                    throw new NotAuthenticatedException(errorMsg);

                case AUTH_SERVICE_UNAVAILABLE:
                default:
                    LOG.error("A request to the identity data plane service failed: {}",
                            securityContext.errorMessage());
                    throw new InternalServerErrorException("Internal Error");
            }
        }

        final WSRequestContext wsContext = WSRequestContext.get(routingContext);
        final Principal principal = securityContext.principal().orElseThrow(
                () -> new AssertionError("Authentication response was successful, but no principal returned"));

        final AuthenticationInfo authInfo;
        // make sure that we only perform different modes of authentication for V2 API only
        if (authenticationMode == AuthenticationMode.V2) {
            switch (principal.getType()) {
                case USER:
                    //A user under the tenancy. The subject id is the user OCID
                    wsContext.setPrincipal(principal);
                    final String userOcid = principal.getSubjectId();
                    //Set the User in the routine context for audit and log.
                    try {
                        Optional<User> user = metadataClient.getUser(userOcid,
                                wsContext.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE));
                        wsContext.setUser(user.orElse(null));
                    } catch (Throwable t) {
                        LOG.error("Exception trying to get user {} from metadata client", t, userOcid);
                    }
                    authInfo = new AuthenticationInfo(principal, null,
                            AuthorizationRequest.Kind.USER);
                    break;
                case RESOURCE:
                    // Treat a resource principal the same as an instance principal for authentication
                case INSTANCE:
                    if (routingContext.request().headers().contains(Principal.OPC_OBO_HEADER)) {
                        authInfo = authenticateObo(routingContext.request(), principal, authHeaderParser,
                                AuthorizationRequest.Kind.DELEGATION);
                        wsContext.setPrincipal(authInfo.getUserPrincipal().orElse(null));
                    } else {
                        //An actual compute instance, either bare metal or VM. The subject id is the instance id
                        wsContext.setPrincipal(principal);
                        authInfo = new AuthenticationInfo(principal, null,
                                AuthorizationRequest.Kind.INSTANCE);
                    }
                    break;
                case SERVICE:
                    if (routingContext.request().headers().contains(Principal.OPC_OBO_HEADER)) {
                        //A OCI service, running in or outside service enclave.
                        //The user id is service-name/certificate-fingerprint
                        //We therefore set the SUBJECT_CONTEXT_KEY to that of the OBO principal
                        authInfo = authenticateObo(routingContext.request(), principal, authHeaderParser,
                                AuthorizationRequest.Kind.OBO);
                        wsContext.setPrincipal(authInfo.getUserPrincipal().orElse(null));
                    } else {
                        //A OCI service, running in or outside service enclave.
                        //The user id is service-name/certificate-fingerprint
                        wsContext.setPrincipal(principal);
                        authInfo = new AuthenticationInfo(null, principal, AuthorizationRequest.Kind.SERVICE);
                    }
                    break;
                default:
                    LOG.warn("Unexpected principal type: {}", principal);
                    throw new NotAuthenticatedException("Authentication Failed");
            }
        } else {
            // for other type of APIs, we fallback to user authentication only
            authInfo = new AuthenticationInfo(principal, null, AuthorizationRequest.Kind.USER);
        }

        logRequesterDetails(principal,
                WSRequestContext.getMetricScope(routingContext),
                wsContext);

        return authInfo;
    }

    protected AuthenticationInfo authenticateInternal(javax.ws.rs.core.HttpHeaders headers,
                                                      MetricScope metricScope,
                                                      SecurityContext securityContext,
                                                      AuthenticationMode authenticationMode,
                                                      String namespace) {
        if (!securityContext.isSuccess()) {
            final AuthenticatorError error = securityContext.error()
                    .orElse(AuthenticatorError.AUTH_SERVICE_UNAVAILABLE);
            final String errorMsg = securityContext.errorMessage() == null ?
                    "Authentication error" : securityContext.errorMessage();
            metricScope.annotate("authErrorMessage", errorMsg);

            switch (error) {
                case MISSING_AUTHENTICATION_INFO:
                    throw new NotAuthenticatedException("The Authorization header must be present");

                case EXPIRED_AUTHENTICATION_INFO:
                case DATE_OUTSIDE_CLOCK_SKEW:
                    throw new NotAuthenticatedException(
                            "The Authorization header has a date that is either too early or too late," +
                                    " check that your local clock is correct");

                case SIGNATURE_NOT_VALID:
                case INVALID_AUTHENTICATION_INFO:
                    throw new NotAuthenticatedException(errorMsg);

                case AUTH_SERVICE_UNAVAILABLE:
                default:
                    LOG.error("A request to the identity data plane service failed: {}",
                            securityContext.errorMessage());
                    throw new InternalServerErrorException("Internal Error");
            }
        }

        //final WSRequestContext wsContext = WSRequestContext.get(routingContext);
        final Principal principal = securityContext.principal().orElseThrow(
                () -> new AssertionError("Authentication response was successful, but no principal returned"));

        final AuthenticationInfo authInfo;
        // make sure that we only perform different modes of authentication for V2 API only
        if (authenticationMode == AuthenticationMode.V2) {
            switch (principal.getType()) {
                case USER:
                    authInfo = new AuthenticationInfo(principal, null,
                            AuthorizationRequest.Kind.USER);
                    break;
                case RESOURCE:
                    // Treat a resource principal the same as an instance principal for authentication
                case INSTANCE:
                    if (headers.getRequestHeaders().containsKey(Principal.OPC_OBO_HEADER)) {
                        authInfo = authenticateObo(headers, principal, authHeaderParser,
                                AuthorizationRequest.Kind.DELEGATION);
                    } else {
                        //An actual compute instance, either bare metal or VM. The subject id is the instance id
                        authInfo = new AuthenticationInfo(principal, null,
                                AuthorizationRequest.Kind.INSTANCE);
                    }
                    break;
                case SERVICE:
                    if (headers.getRequestHeaders().containsKey(Principal.OPC_OBO_HEADER)) {
                        //A OCI service, running in or outside service enclave.
                        //The user id is service-name/certificate-fingerprint
                        //We therefore set the SUBJECT_CONTEXT_KEY to that of the OBO principal
                        authInfo = authenticateObo(headers, principal, authHeaderParser,
                                AuthorizationRequest.Kind.OBO);
                    } else {
                        //A OCI service, running in or outside service enclave.
                        //The user id is service-name/certificate-fingerprint
                        authInfo = new AuthenticationInfo(null, principal, AuthorizationRequest.Kind.SERVICE);
                    }
                    break;
                default:
                    LOG.warn("Unexpected principal type: {}", principal);
                    throw new NotAuthenticatedException("Authentication Failed");
            }
        } else {
            // for other type of APIs, we fallback to user authentication only
            authInfo = new AuthenticationInfo(principal, null, AuthorizationRequest.Kind.USER);
        }

        logRequesterDetails(principal,
                metricScope,
                namespace);

        return authInfo;
    }

    private void logRequesterDetails(Principal principal, MetricScope metricScope, WSRequestContext wsContext) {
        Map<String, String> annos = new HashMap<>();
        annos.put("tenantId", principal.getTenantId());
        annos.put("subjectId", principal.getSubjectId());

        final String namespace = wsContext.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE);

        // Log tenant name for the specified tenant Ocid.
        // This is a time-limited operation to avoid affecting latency.
        Optional<Tenant> tenantOptional;
        try {
            tenantOptional = timeLimiter.callWithTimeout(
                    () -> metadataClient.getTenantByCompartmentId(principal.getTenantId(), namespace),
                    10, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            tenantOptional = Optional.empty();
        }

        final String tenantName = tenantOptional.map(Tenant::getName).orElse("UnknownTenant");
        wsContext.setTenantName(tenantName);
        annos.put("tenantName", tenantName);

        metricScope.annotate("requester", annos);
    }

    private void logRequesterDetails(Principal principal, MetricScope metricScope, String namespace) {
        Map<String, String> annos = new HashMap<>();
        annos.put("tenantId", principal.getTenantId());
        annos.put("subjectId", principal.getSubjectId());

        // Log tenant name for the specified tenant Ocid.
        // This is a time-limited operation to avoid affecting latency.
        Optional<Tenant> tenantOptional;
        try {
            tenantOptional = timeLimiter.callWithTimeout(
                    () -> metadataClient.getTenantByCompartmentId(principal.getTenantId(), namespace),
                    10, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            tenantOptional = Optional.empty();
        }

        final String tenantName = tenantOptional.map(Tenant::getName).orElse("UnknownTenant");
        annos.put("tenantName", tenantName);

        metricScope.annotate("requester", annos);
    }

    /**
     * For an OBO request, the request is expected to be signed by a service certificate and the request must also
     * contains "opc-obo-token" header which must also be signed. This token is an encoded JWT token which
     * will be validated via {@link com.oracle.pic.identity.authentication.token.TokenVerifierImpl}. The first part
     * has already been validate in {@link #authenticate} method. In this method, we will check the validity of the
     * OBO token only. If the verification passes, it means that the service caller has the correct token and should
     * be able to act on behalf of a user.
     *
     * At the last step, we also verify that the OBO token actually belongs to the service principal. This prevents
     * the token which is owned by one service to be used by another service.
     *
     * @param request          the request
     * @param servicePrincipal the service principal
     * @return the security context
     */
    @VisibleForTesting
    AuthenticationInfo authenticateObo(HttpServerRequest request,
                                       Principal servicePrincipal,
                                       AuthHeaderParser parser,
                                       AuthorizationRequest.Kind kind) {
        final List<String> oboHeaders = request.headers().getAll(Principal.OPC_OBO_HEADER);

        // there must be only 1 opc-obo-token value
        if (oboHeaders.size() != 1) {
            throw new NotAuthenticatedException(
                    "Authentication failed because there was more than one opc-obo-token header in the request");
        }

        // opc-obo-token must be signed
        final List<String> authHeaders = request.headers().getAll(Constants.AUTHORIZATION_HEADER);

        if (authHeaders.size() != 1) {
            throw new NotAuthenticatedException(
                    "Authentication failed because there was more than one Authorization header in the request");
        }

        final SignedSignatureInfo ssInfo = parser.parseAuthenticationHeader(authHeaders.get(0))
                .orElseThrow(() -> new NotAuthenticatedException("Authentication Failed"));
        if (ssInfo.getHeaderNames().stream().noneMatch(h -> h.equalsIgnoreCase(Principal.OPC_OBO_HEADER))) {
            throw new NotAuthenticatedException(
                    "Authentication failed because the opc-obo-token header was not signed");
        }

        final String oboTokenHeader = oboHeaders.get(0);
        final SecurityContext securityContext;
        try {
            securityContext = authenticatorClient.authenticateOboToken(oboTokenHeader);
        } catch (AuthClientException ex) {
            throw new NotAuthenticatedException(ex.getMessage());
        }

        if (!securityContext.isSuccess()) {
            throw new NotAuthenticatedException(securityContext.error().map(AuthenticatorError::toString)
                    .orElse("Authentication Failed"));
        }

        // Add the obo token to the principal, in case this service decides to get another obo token in
        // exchange for this one.
        final Claim oboToken = new Claim(ClaimType.OBO_TOKEN.value(), oboTokenHeader, Constants.HEADER_CLAIM_ISSUER);
        final Principal oboPrincipal = securityContext.principal().orElseThrow(
                () -> new AssertionError("Authentication response was successful, but no principal returned"));

        // note that the principal here is not immutable and securityContext.principal().get() returns the actual
        // value instead of a copy of it. So when we do this, we actually modify the original principal object
        oboPrincipal.getClaims().add(oboToken);

        // Verify that this obo token is owned by the caller service principal
        verifyOwnerTenantId(servicePrincipal, oboPrincipal);

        return new AuthenticationInfo(oboPrincipal, servicePrincipal, kind);
    }

    /**
     * For an OBO request, the request is expected to be signed by a service certificate and the request must also
     * contains "opc-obo-token" header which must also be signed. This token is an encoded JWT token which
     * will be validated via {@link com.oracle.pic.identity.authentication.token.TokenVerifierImpl}. The first part
     * has already been validate in {@link #authenticate} method. In this method, we will check the validity of the
     * OBO token only. If the verification passes, it means that the service caller has the correct token and should
     * be able to act on behalf of a user.
     *
     * At the last step, we also verify that the OBO token actually belongs to the service principal. This prevents
     * the token which is owned by one service to be used by another service.
     *
     * @param headers          the headers
     * @param servicePrincipal the service principal
     * @return the security context
     */
    @VisibleForTesting
    AuthenticationInfo authenticateObo(javax.ws.rs.core.HttpHeaders headers,
                                       Principal servicePrincipal,
                                       AuthHeaderParser parser,
                                       AuthorizationRequest.Kind kind) {
        final List<String> oboHeaders = headers.getRequestHeader(Principal.OPC_OBO_HEADER);

        // there must be only 1 opc-obo-token value
        if (oboHeaders.size() != 1) {
            throw new NotAuthenticatedException(
                    "Authentication failed because there was more than one opc-obo-token header in the request");
        }

        // opc-obo-token must be signed
        final List<String> authHeaders =  headers.getRequestHeader(Constants.AUTHORIZATION_HEADER);

        if (authHeaders.size() != 1) {
            throw new NotAuthenticatedException(
                    "Authentication failed because there was more than one Authorization header in the request");
        }

        final SignedSignatureInfo ssInfo = parser.parseAuthenticationHeader(authHeaders.get(0))
                .orElseThrow(() -> new NotAuthenticatedException("Authentication Failed"));
        if (ssInfo.getHeaderNames().stream().noneMatch(h -> h.equalsIgnoreCase(Principal.OPC_OBO_HEADER))) {
            throw new NotAuthenticatedException(
                    "Authentication failed because the opc-obo-token header was not signed");
        }

        final String oboTokenHeader = oboHeaders.get(0);
        final SecurityContext securityContext;
        try {
            securityContext = authenticatorClient.authenticateOboToken(oboTokenHeader);
        } catch (AuthClientException ex) {
            throw new NotAuthenticatedException(ex.getMessage());
        }

        if (!securityContext.isSuccess()) {
            throw new NotAuthenticatedException(securityContext.error().map(AuthenticatorError::toString)
                    .orElse("Authentication Failed"));
        }

        // Add the obo token to the principal, in case this service decides to get another obo token in
        // exchange for this one.
        final Claim oboToken = new Claim(ClaimType.OBO_TOKEN.value(), oboTokenHeader, Constants.HEADER_CLAIM_ISSUER);
        final Principal oboPrincipal = securityContext.principal().orElseThrow(
                () -> new AssertionError("Authentication response was successful, but no principal returned"));

        // note that the principal here is not immutable and securityContext.principal().get() returns the actual
        // value instead of a copy of it. So when we do this, we actually modify the original principal object
        oboPrincipal.getClaims().add(oboToken);

        // Verify that this obo token is owned by the caller service principal
        verifyOwnerTenantId(servicePrincipal, oboPrincipal);

        return new AuthenticationInfo(oboPrincipal, servicePrincipal, kind);
    }

    private void verifyOwnerTenantId(Principal servicePrincipal, Principal oboPrincipal) {
        Optional<String> owner = oboPrincipal.getClaimValue(ClaimType.OWNER);
        if (owner.isPresent()) {
            if (owner.get().equalsIgnoreCase(servicePrincipal.getTenantId())) {
                return;
            }
        }
        throw new NotAuthenticatedException("Authentication failed");
    }

    enum AuthenticationMode {
        V2,
        SWIFT
    }
}
