package com.oracle.pic.casper.webserver.api.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.AccessDeniedException;
import com.oracle.pic.casper.common.exceptions.AuthDataPlaneServerException;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.vertx.HttpServerUtil;
import com.oracle.pic.casper.webserver.auth.dataplane.S3SigningKeyClient;
import com.oracle.pic.casper.webserver.auth.dataplane.exceptions.IncorrectRegionException;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SigningKey;
import com.oracle.pic.casper.webserver.api.auth.sigv4.SIGV4Authorization;
import com.oracle.pic.casper.webserver.api.auth.sigv4.SIGV4Signer;
import com.oracle.pic.casper.webserver.api.auth.sigv4.SIGV4Utils;
import com.oracle.pic.casper.webserver.api.s3.S3HttpException;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.limit.ResourceTicket;
import com.oracle.pic.casper.webserver.limit.ResourceType;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.commons.metadata.entities.User;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.PrincipalType;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class S3AuthenticatorImpl implements S3Authenticator {

    private static final Logger LOG = LoggerFactory.getLogger(S3AuthenticatorImpl.class);

    /**
     * Signing key provider for requests.
     */
    private final S3SigningKeyClient s3SigningKeyClient;

    /**
     * Used to calculate request signatures.
     */
    private final SIGV4Signer signer;

    private final Clock clock;

    private final String expectedRegion;

    private final String expectedService;

    private final ResourceControlledMetadataClient metadataClient;

    private final ResourceLimiter resourceLimiter;

    @VisibleForTesting
    S3AuthenticatorImpl(S3SigningKeyClient s3SigningKeyClient, Clock clock, String expectedRegion,
                        ResourceControlledMetadataClient metadataClient, ResourceLimiter resourceLimiter) {
        this.s3SigningKeyClient = Preconditions.checkNotNull(s3SigningKeyClient);
        this.signer = new SIGV4Signer();
        this.clock = Preconditions.checkNotNull(clock);
        this.expectedRegion = Preconditions.checkNotNull(expectedRegion);
        this.expectedService = "s3";
        this.metadataClient = metadataClient;
        this.resourceLimiter = resourceLimiter;
    }

    private S3AuthenticatorImpl(S3SigningKeyClient s3SigningKeyClient, Clock clock, ConfigRegion region,
                                ResourceControlledMetadataClient metadataClient, ResourceLimiter resourceLimiter) {
        this(s3SigningKeyClient, clock,
            ((region == ConfigRegion.LOCAL || region == ConfigRegion.LGL) ? ConfigRegion.R1_STABLE : region)
                .getFullName(), metadataClient, resourceLimiter);
    }

    public S3AuthenticatorImpl(S3SigningKeyClient s3SigningKeyClient, ResourceControlledMetadataClient metadataClient,
                               ResourceLimiter resourceLimiter) {
        this(s3SigningKeyClient, Clock.systemDefaultZone(), ConfigRegion.fromSystemProperty(),
                metadataClient, resourceLimiter);
    }

    @Override
    public AuthenticationInfo authenticate(RoutingContext context, String bodySha256) {
        final HttpServerRequest request = context.request();
        final String httpMethod = request.method().name();
        final String path = request.path();
        Map<String, String> queryParameters = HttpServerUtil.getQueryParams(request);
        final Map<String, List<String>> headers = HttpServerUtil.getHeaders(request);

        final String authHeader = SIGV4Utils.getHeader(headers, HttpHeaders.AUTHORIZATION);
        final String algorithmParam = queryParameters.get(SIGV4Authorization.X_AMZ_ALGORITHM);
        final SIGV4Authorization authorizationFromRequest;
        final Instant requestTime;
        if (authHeader == null && algorithmParam == null) {
            //neither present, treat as anonymous user
            return AuthenticationInfo.ANONYMOUS_USER;
        } else if (authHeader != null && algorithmParam != null) {
            // both present
            throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT,
                    "Only the X-Amz-Algorithm query parameter or the Authorization header can be used", path);
        } else if (authHeader != null) {
            // use headers
            authorizationFromRequest = SIGV4Authorization.fromAuthorizationHeader(authHeader);
            SIGV4Utils.verifyHeadersAreSigned(SIGV4Utils.REQUIRED_HEADERS, authorizationFromRequest.getSignedHeaders());
            requestTime = SIGV4Utils.getRequestTimeFromHeaders(headers, authorizationFromRequest.getSignedHeaders());
            final Instant now = clock.instant();
            final Duration age = Duration.between(requestTime, now).abs();
            if (age.compareTo(SIGV4Utils.MAX_REQUEST_AGE) > 0) {
                final String error = String.format(
                        "Request has expired. Request date = %s, current time = %s",
                        requestTime,
                        now);
                throw new AccessDeniedException(error);
            }
        } else {
            //use query params
            authorizationFromRequest = SIGV4Authorization.fromQueryParameters(queryParameters);
            SIGV4Utils.verifyHeadersAreSigned(SIGV4Utils.REQUIRED_HEADERS_QUERY_STRING,
                    authorizationFromRequest.getSignedHeaders());
            requestTime = SIGV4Utils.parseHeaderTime(queryParameters.get(SIGV4Utils.X_AMZ_DATE));
            //check whether presigned request is expired
            Instant expireTime = SIGV4Utils.getExpireTime(queryParameters.get(SIGV4Utils.X_AMZ_DATE),
                    queryParameters.get(SIGV4Authorization.X_AMZ_EXPIRES));
            if (expireTime.isBefore(clock.instant())) {
                throw new AccessDeniedException("Request has expired.");
            }
            // For query parameter requests we have to make some changes
            // First, use a special hash for the body
            bodySha256 = SIGV4Utils.UNSIGNED_PAYLOAD;
            // Second, we cannot include the signature in the query parameters
            // when creating the canonical request
            queryParameters = new HashMap<>(queryParameters);
            queryParameters.remove(SIGV4Authorization.X_AMZ_SIGNATURE);
        }

        final String secretKeyId = authorizationFromRequest.getSecretKeyId();
        final String region = authorizationFromRequest.getRegion();
        final Instant date = authorizationFromRequest.getDate();
        final String service = authorizationFromRequest.getService();

        //us-east-1 region string works for all regions
        if (!expectedRegion.equals(region) && !SIGV4Utils.US_EAST_REGION_STRING.equals(region)) {
            throw new IncorrectRegionException("The authorization header is malformed; the region " + region +
                    " is wrong; expecting " + expectedRegion, expectedRegion, region);
        }

        // do not check date because signing date may be different from request date

        if (!expectedService.equals(service)) {
            throw new NotAuthenticatedException("Authentication service '" + service + "' is incorrect.");
        }
        // get signing key from identity
        final SigningKey signingKey;
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        final String stackContext = stackTraceElements[1].getClassName() + "::" + stackTraceElements[1].getMethodName();
        try (ResourceTicket resourceTicket = resourceLimiter.acquireResourceTicket(
                wsRequestContext.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE),
                ResourceType.IDENTITY, stackContext)) {
            signingKey = s3SigningKeyClient.getSigningKey(secretKeyId, region, SIGV4Utils.formatDate(date), service);
        } catch (AuthDataPlaneServerException ex) {
            if (ex.getStatusCode() == HttpResponseStatus.NOT_FOUND) {
                throw new NotAuthenticatedException("Authentication failed", ex);
            }
            throw ex;
        }

        final Map<String, List<String>> filteredHeaders =
                SIGV4Utils.filterHeaders(headers, authorizationFromRequest.getSignedHeaders());

        final Map<String, String> normalizedHeaders = SIGV4Utils.normalizeHeaders(filteredHeaders);

        final String canonicalRequest = SIGV4Utils.canonicalRequest(httpMethod, path, queryParameters,
                normalizedHeaders, bodySha256);

        final String stringToSign = this.signer.stringToSign(requestTime, region, service, canonicalRequest);

        final String expectedSignature = this.signer.calculateAuthorizationSignature(stringToSign,
                signingKey.getKey());
        if (!areSignaturesEqual(expectedSignature, authorizationFromRequest.getSignature())) {
            LOG.error("HTTP method => {}", httpMethod);
            LOG.error("URI path => {}", path);
            LOG.error("query parameters => {}", queryParameters.toString());
            LOG.error("headers => {}", headers.toString());
            LOG.error("bodySHA256 => {}", bodySha256);
            LOG.error("Canonical request => {}", canonicalRequest.replace("\n", "\\n"));
            LOG.error("String to sign => {}", stringToSign.replace("\n", "\\n"));
            LOG.error("Request signature => {}", authorizationFromRequest.getSignature());
            LOG.error("Expected signature => {}", expectedSignature.substring(0, 4) + "...");
            final String error = String.format("Signature mismatch. Signature %s is not correct.",
                    authorizationFromRequest.getSignature());
            throw new NotAuthenticatedException(error);
        }
        final Principal principal = signingKey.getPrincipal();
        wsRequestContext.setPrincipal(
                principal == null ? AuthenticationInfo.ANONYMOUS_USER.getMainPrincipal() : principal);
        //Set the User in the routine context for audit and log.
        if (principal != null && principal.getType() == PrincipalType.USER) {
            final String userOcid = principal.getSubjectId();
            try {
                Optional<User> user = metadataClient.getUser(userOcid,
                        wsRequestContext.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE));
                wsRequestContext.setUser(user.orElse(null));
            } catch (Throwable t) {
                LOG.error("Exception trying to get user {} from metadata client", t, userOcid);
            }
        }
        return principal == null ? AuthenticationInfo.ANONYMOUS_USER : AuthenticationInfo.fromPrincipal(principal);
    }

    /**
     * Compare two signatures to see if they are equal. This is a constant-time
     * comparison to prevent timing attacks.
     */
    static boolean areSignaturesEqual(String a, String b) {
        if (a.length() != b.length()) {
            return false;
        }

        int x = 0;
        for (int i = 0; i < a.length(); ++i) {
            x |= a.charAt(i) ^ b.charAt(i);
        }

        return (x == 0);
    }
}
