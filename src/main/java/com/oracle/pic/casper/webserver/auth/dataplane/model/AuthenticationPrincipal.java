package com.oracle.pic.casper.webserver.auth.dataplane.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.PrincipalImpl;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * AuthenticationPrincipal has a method to deserialize a Principal object from an auth service response.
 */
public final class AuthenticationPrincipal {
    private AuthenticationPrincipal() {

    }

    /**
     * The auth service replies to the /v1/authentication URL with the following JSON structure:
     *
     * {
     *     "tenant" : {
     *         "id" : "ocid1.tenancy.region1..aaaaaaaap7uvrzxfpjtijgtwuykipblgof4s5sgh74gpspeyyucyu46lnaea",
     *         "name" : "casperlocalidentity"
     *     },
     *     "user" : {
     *         "id" : "ocid1.user.region1..aaaaaaaawgrh6qhsuu65w4onsrefugicbzfxzmag37wtjtg2g5mb2pl4lixa",
     *         "name" : "opc_casper_us_grp@oracle.com",
     *         "isOTP" : false,
     *         "isMfaActivated" : false,
     *         "isMfaVerified" : false
     *     }
     * }
     *
     * This method only returns the tenant and user IDs, as that is all we require for Casper.
     */
    public static Principal fromJSON(String json, ObjectMapper mapper) {
        final JsonNode root;
        try {
            root = mapper.readTree(json);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        if (root == null || !root.isObject()) {
            throw new AssertionError("The authn principal JSON was either empty or malformed: " + json);
        }

        final JsonNode tenant = root.get("tenant");
        if (tenant == null || !tenant.isObject()) {
            throw new AssertionError(
                    "The authn principal JSON did not have a 'tenant' field or it was not an object: " + json);
        }

        final JsonNode tenantId = tenant.get("id");
        if (tenantId == null || !tenantId.isTextual()) {
            throw new AssertionError(
                    "The authn principal JSON did not have a 'tenant.id' field or it was not text: " + json);
        }

        final JsonNode user = root.get("user");
        if (user == null || !user.isObject()) {
            throw new AssertionError(
                    "The authn principal JSON did not have a 'user' field or it was not an object: " + json);
        }

        final JsonNode userId = user.get("id");
        if (userId == null || !userId.isTextual()) {
            throw new AssertionError(
                    "The authn principal JSON did not have a 'user.id' field or it was not text: " + json);
        }

        return new PrincipalImpl(tenantId.asText(), userId.asText());
    }
}
