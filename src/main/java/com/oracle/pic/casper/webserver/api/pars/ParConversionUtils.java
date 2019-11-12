package com.oracle.pic.casper.webserver.api.pars;

import com.google.common.annotations.VisibleForTesting;
import com.oracle.pic.casper.common.json.JsonSerDe;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.model.CreatePreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.PreAuthenticatedRequestMetadata;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;
import com.oracle.pic.identity.authentication.Claim;
import com.oracle.pic.identity.authentication.ClaimType;
import com.oracle.pic.identity.authentication.Constants;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.PrincipalImpl;
import com.oracle.pic.identity.authentication.TokenType;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * Utility class that provide functions for conversions between PAR objects
 *
 * Terminology:
 * - customerParId: verifierId + ':' + Optional<objectName>
 * - backendParId: Optional<objectName> + '#' + verifierId
 * - fullParId: bucketId + '#' + Optional<objectName> + '#' + verifierId
 */
public final class ParConversionUtils {

    /**
     *  special keys used to store all PAR related metadata in the PAR object metadata in Casper
     */
    enum Keys {
        op, version, bucketName, parObjectName, name, status, expiration, identity
    }

    private ParConversionUtils() {

    }

    static PreAuthenticatedRequestMetadata backendToCustomerParMd(PreAuthenticatedRequestMetadata parMd) {
        String customerParId = new BackendParId(parMd.getParId()).toCustomerParId().toString();
        return parMd.cloneAndOverrideParId(customerParId);
    }

    // map of all PAR attrs that we need to store in our backend. Note that this map is the actual PAR data
    static Map<String, String> toMetadataMap(CreatePreAuthenticatedRequestRequest request, JsonSerDe jsonSerDe) {

        Map<String, String> map = new HashMap<>();
        map.put(Keys.op.name(), String.valueOf(request.getAccessType().getValue()));
        map.put(Keys.status.name(), String.valueOf(PreAuthenticatedRequestMetadata.Status.Active.getValue()));
        map.put(Keys.version.name(), String.valueOf(PreAuthenticatedRequestMetadata.Version.V1.getValue()));
        map.put(Keys.bucketName.name(), request.getBucketName());
        map.put(Keys.parObjectName.name(), request.getObjectName().orElse(""));
        map.put(Keys.name.name(), request.getName());
        map.put(Keys.expiration.name(), String.valueOf(request.getTimeExpires().toEpochMilli()));
        map.put(Keys.identity.name(), jsonSerDe.toJson(createNewPrincipalForDelegation(
                trimPrincipal(request.getAuthInfo().getMainPrincipal()))));
        return map;
    }

    /**
     * For the delegation usecase the tokenType claim in the principal is changed to OBO.
     *
     * We are only passing one principal for both OBOs and Delegation with Pars and identity expects two principals
     * for OBO/Delegation.
     * This is to get around auth service check for delegate principal (servicePrincipal object).
     * More information in this ticket : https://jira-sd.mc1.oracleiaas.com/browse/IOS-10927
     *
     * For all other usecases return the original principal
     * @param principal
     */
    @VisibleForTesting
     static Principal createNewPrincipalForDelegation(Principal principal) {
        Optional<String> tokenType = principal.getClaimValue(ClaimType.TOKEN_TYPE);
        if (tokenType.isPresent() && tokenType.get().equalsIgnoreCase(TokenType.DELEGATION.value())) {
            Principal delPrincipal = PrincipalImpl.of(principal);
            Optional<String> claimIssuer = delPrincipal.getClaims().stream()
                    .filter(claim -> claim.getKey().equalsIgnoreCase(ClaimType.TOKEN_TYPE.value()))
                    .findFirst()
                    .map(claim -> {
                        delPrincipal.getClaims().remove(claim);
                        return claim.getIssuer();
                    });
            delPrincipal.getClaims().add(new Claim(ClaimType.TOKEN_TYPE.value(), TokenType.OBO.value(),
                    claimIssuer.get()));
            //add a custom claim to signify that casper made this change. Auth service will ignore this
            delPrincipal.getClaims().add(new Claim("casper", "del->obo", claimIssuer.get()));
            return delPrincipal;
        }
        return principal;
    }

    // helper method to marshall a PAR from object metadata retrieved from the backing Casper store.
    static PreAuthenticatedRequestMetadata fromObjectMetadata(ObjectMetadata objectMetadata, JsonSerDe jsonSerDe) {

        return fromMetadataMap(objectMetadata.getMetadata(), objectMetadata.getObjectName(),
                objectMetadata.getCreationTime(), jsonSerDe);
    }

    // helper method to marshall a PAR from object summary retrieved from the backing Casper store.
    static PreAuthenticatedRequestMetadata fromObjectSummary(ObjectSummary objectSummary, JsonSerDe jsonSerDe) {
        return fromMetadataMap(objectSummary.getMetadata(), objectSummary.getName(), objectSummary.getTimeCreated(),
                jsonSerDe);
    }

    private static PreAuthenticatedRequestMetadata fromMetadataMap(Map<String, String> map, String fullParId,
                                                           Date timeCreated, JsonSerDe jsonSerDe) {
        return new PreAuthenticatedRequestMetadata(
                new FullParId(fullParId).toBackendParId().toString(),
                map.get(Keys.name.name()),
                PreAuthenticatedRequestMetadata.AccessType.parseAccessType(map.get(Keys.op.name())),
                map.get(Keys.bucketName.name()),
                map.get(Keys.parObjectName.name()).isEmpty() ? null : map.get(Keys.parObjectName.name()),
                PreAuthenticatedRequestMetadata.Version.parseVersion(map.get(Keys.version.name())),
                PreAuthenticatedRequestMetadata.Status.parseStatus(map.get(Keys.status.name())),
                AuthenticationInfo.fromPrincipal(jsonSerDe.fromJson(map.get(Keys.identity.name()), Principal.class)),
                Instant.ofEpochMilli(timeCreated.getTime()),
                Instant.ofEpochMilli(Long.parseLong(map.get(Keys.expiration.name()))));
    }

    private static final List<String> CLAIM_KEYS_TO_IGNORE =
            Arrays.asList(ClaimType.JWK.value(),
                    Constants.AUTHORIZATION_HEADER,
                    ClaimType.CURRENT_TOKEN_ID.value(),
                    ClaimType.TARGET_TENANT_ID.value(),
                    ClaimType.TARGET_TENANT_IDS.value(),
                    ClaimType.ISSUED_AT.value(),
                    ClaimType.EXP.value(),
                    ClaimType.TENANT.value(),
                    ClaimType.CALL_CHAIN.value(),
                    ClaimType.ISSUER.value(),
                    ClaimType.NBF.value(),
                    ClaimType.OBO_TOKEN.value(),
                    ClaimType.AUDIENCE.value(),
                    ClaimType.SESSION_EXPIRATION.value(),
                    ClaimType.SUBJECT.value())
                .stream().map(c -> c.toLowerCase()).collect(Collectors.toList());

    @VisibleForTesting
    static Principal trimPrincipal(Principal longPrincipal) {
        Principal shortPrincipal = PrincipalImpl.of(longPrincipal);

        longPrincipal.getClaims().forEach(claim -> {
            if (CLAIM_KEYS_TO_IGNORE.contains(claim.getKey().toLowerCase())
                || claim.getIssuer().equalsIgnoreCase(Constants.HEADER_CLAIM_ISSUER)
                || claim.getIssuer().equalsIgnoreCase("header")) {
                // Remove claims that are listed above and claims that were generated from headers
                // Due to backward compatibility, I am adding both old and new values for the header
                // claim issuer to be safe. Old value was "header", now it is just "h".
                // Header claims are only needed when a service wants to get an OBO token using this
                // principal object.

                shortPrincipal.getClaims().remove(claim);
            }
        });
        return shortPrincipal;
    }
}
