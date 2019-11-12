package com.oracle.pic.casper.webserver.api.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Resources;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.PrincipalImpl;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;


/**
 * A JWT Authenticator.
 * JWT - Json Web Token - https://tools.ietf.org/html/rfc7519
 * JWK - Json Web Key - https://tools.ietf.org/html/rfc7517
 * JWS - Json Web Signature - https://tools.ietf.org/html/rfc7515
 *
 * This authenticator is designed to work with JWT provided by operator-access-token service.
 * <code>export BEARER_TOKEN=$(ssh operator-access-token.svc.ad1.r1 "generate --mode jwt")</code>
 *
 * The operator-access-token uses your SSH credentials to find the matching BOAT user in the bmc_operator_access
 * tenancy. Here is an example for a JWT returned:
 * <code>
 * {
 *  alg: "RS256",
 *  typ: "JWT",
 *  kid: "fdcr624pxNqTd2gu6ca/cix93s5z1+H9nYB5O8bDn0g="
 * }.
 * {
 *  sub: "ocid1.user.region1..aaaaaaaalkqeugew6btm3krdhlkzgthvyx7ptn27phfyuylba4vghbdtjxhq",
 *  iss: "jit-token-generator",
 *  exp: 1547895542,
 *  iat: 1547852342,
 *  nbf: 1547852312,
 *  tenant: "ocid1.tenancy.region1..aaaaaaaa6gqokctiy6pncv6jooomauqibkkhduaohvikdrwi6ze2n5o5v3kq",
 *  name: "fmzakari"
 * }.
 * [signature]
 * </code>
 *
 * The JWT is then appended with a signature that is created using the RSA keys defined in the JWK format.
 * By verifying the signature we can prove that the JWT must have been crafted by someone with access to the operator
 * access service. Only <b>operator-access-token</b> hosts have the matching RSA private keys for the public keys
 * defined in the jwks.json file.
 *
 * This authenticator will allow users to authenticate via curl.
 * <code>
 * export BEARER_TOKEN=$(ssh operator-access-token.svc.ad1.r1 "generate --mode jwt")
 * curl -H "Content-Type: application/json" -H "Authorization: Bearer ${BEARER_TOKEN}" http://casperv2.svc.ad1.r1/n/
 * </code>
 */
public class JWTAuthenticator {

    /**
     * The static hardcoded list of JWKS for each region.
     * This file was copied from authproxy which stores their JWK
     * https://bitbucket.oci.oraclecorp.com/projects/SECINF/repos/authproxy/browse/config/jwks.json
     */
    private static final String JWKS_RESOURCE_FILE = "jwks.json";

    private final JWKSet jwks;

    private final Clock clock;

    @VisibleForTesting
    protected JWTAuthenticator(JWKSet jwks, Clock clock) {
        this.jwks = jwks;
        this.clock = clock;
    }

    public static JWTAuthenticator preloadFromResourceFile() {
        try {
            final String json = Resources.toString(Resources.getResource(JWKS_RESOURCE_FILE),
                    StandardCharsets.UTF_8);
            final JWKSet jwkSet = JWKSet.parse(json);
            return new JWTAuthenticator(jwkSet, Clock.systemUTC());
        } catch (Throwable t) {
            throw new RuntimeException("Failure initializing the JWTAuthenticator.", t);
        }
    }

    public AuthenticationInfo authenticate(String jwtEncoded) {
        try {
            final SignedJWT jwt = SignedJWT.parse(jwtEncoded);
            final JWTClaimsSet claims = jwt.getJWTClaimsSet();
            final String userOcid = Optional.ofNullable(claims.getSubject())
                    .orElseThrow(() -> new NotAuthenticatedException("No subject in JWT claims present."));
            final String tenantOcid = Optional.ofNullable(claims.getStringClaim("tenant"))
                    .orElseThrow(() -> new NotAuthenticatedException("No tenant in JWT claims present."));
            final String keyId = Optional.ofNullable(jwt.getHeader().getKeyID())
                    .orElseThrow(() -> new NotAuthenticatedException("The JWT is missing a keyId."));

            final Instant now = clock.instant();
            final Optional<Instant> notBefore = Optional.ofNullable(claims.getNotBeforeTime()).map(Date::toInstant);
            final Optional<Instant> expiration = Optional.ofNullable(claims.getExpirationTime()).map(Date::toInstant);

            if (expiration.isPresent() && expiration.get().isBefore(now)) {
                throw new NotAuthenticatedException("The JWT has expired.");
            } else if (notBefore.isPresent() && notBefore.get().isAfter(now)) {
                throw new NotAuthenticatedException("The JWT is not valid yet.");
            }

            final RSAKey jwk = (RSAKey) jwks.getKeyByKeyId(keyId);
            final JWSVerifier verifier = new RSASSAVerifier(jwk.toPublicJWK());

            boolean verify = jwt.verify(verifier);
            if (!verify) {
                throw new NotAuthenticatedException("Could not verify the JWT");
            }
            final Principal principal = new PrincipalImpl(tenantOcid, userOcid);
            return AuthenticationInfo.fromPrincipal(principal);

        } catch (Exception t) {
            throw new NotAuthenticatedException("Could not authenticate.", t);
        }

    }

}
