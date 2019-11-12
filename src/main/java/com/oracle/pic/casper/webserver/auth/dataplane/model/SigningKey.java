package com.oracle.pic.casper.webserver.auth.dataplane.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Base64;
import com.google.common.base.MoreObjects;
import com.oracle.pic.casper.webserver.auth.dataplane.sigv4.SIGV4SigningKey;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.PrincipalImpl;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * SigningKey encapsulates the SIGv4 signing key and OCI principal information for a request.
 */
public final class SigningKey {

    private final SIGV4SigningKey key;
    private final Principal principal;

    /**
     * Parse a SigningKey from a JSON string.
     *
     * The expected JSON format is:
     *
     *  {
     *      "signingKey": "...",
     *      "principal": {
     *          "tenant": {
     *              "id": "..."
     *          },
     *          "user": {
     *              "id": "..."
     *          }
     *      }
     *  }
     */
    public static SigningKey fromJSON(String json, ObjectMapper mapper) {
        final JsonNode root;
        try {
            root = mapper.readTree(json);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        if (root == null || !root.isObject()) {
            throw new AssertionError("The signing key JSON was either empty or malformed: " + json);
        }

        final JsonNode signingKey = root.get("signingKey");
        if (signingKey == null || !signingKey.isTextual()) {
            throw new AssertionError(
                    "The signing key JSON did not have a 'signingKey' field, or it was not text: " + json);
        }

        final JsonNode principal = root.get("principal");
        if (principal == null || !principal.isObject()) {
            throw new AssertionError(
                    "The signing key JSON did not have a 'principal' field or it was not an object: " + json);
        }

        final JsonNode tenant = principal.get("tenant");
        if (tenant == null || !tenant.isObject()) {
            throw new AssertionError(
                    "The signing key JSON did not have a 'tenant' field or it was not an object: " + json);
        }

        final JsonNode tenantId = tenant.get("id");
        if (tenantId == null || !tenantId.isTextual()) {
            throw new AssertionError(
                    "The signing key JSON did not have a 'tenant.id' field or it was not text: " + json);
        }

        final JsonNode user = principal.get("user");
        if (user == null || !user.isObject()) {
            throw new AssertionError(
                    "The signing key JSON did not have a 'user' field or it was not an object: " + json);
        }

        final JsonNode userId = user.get("id");
        if (userId == null || !userId.isTextual()) {
            throw new AssertionError("The signing key JSON did not have a 'user.id' field or it was not text: " + json);
        }

        return new SigningKey(
                new SIGV4SigningKey(Base64.decodeBase64(signingKey.asText())),
                new PrincipalImpl(tenantId.asText(), userId.asText()));
    }

    public SigningKey(SIGV4SigningKey key, Principal principal) {
        this.key = key;
        this.principal = principal;
    }

    public SIGV4SigningKey getKey() {
        return key;
    }

    public Principal getPrincipal() {
        return principal;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("key", key)
                .add("principal", principal)
                .toString();
    }
}
