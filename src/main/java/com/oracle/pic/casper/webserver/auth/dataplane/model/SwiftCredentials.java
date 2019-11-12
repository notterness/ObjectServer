package com.oracle.pic.casper.webserver.auth.dataplane.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.identity.authentication.Principal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class SwiftCredentials {

    private final List<SwiftHashPassword> hashPasswords;
    private final Principal principal;

    private SwiftCredentials(List<SwiftHashPassword> hashPasswords, Principal principal) {
        this.hashPasswords = hashPasswords;
        this.principal = principal;
    }

    public List<SwiftHashPassword> getHashPasswords() {
        return hashPasswords;
    }

    public Principal getPrincipal() {
        return principal;
    }

    public static SwiftCredentials fromJSON(String json, ObjectMapper mapper) {
        final JsonNode root;
        try {
            root = mapper.readTree(json);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        if (root == null || !root.isObject()) {
            throw new AssertionError(String.format("The Swift credentials JSON was either empty " +
                    "or malformed: %s", json));
        }

        final JsonNode hashPasswords = root.get("hashPasswords");
        if (hashPasswords == null || !hashPasswords.isArray()) {
            throw new AssertionError(String.format("The Swift credentials JSON did not have a 'hashPasswords' " +
                    "field or it was not an array: %s", json));
        }

        List<SwiftHashPassword> hashPasswordList = StreamSupport.stream(hashPasswords.spliterator(), false).map(
                jsonNode -> SwiftHashPassword.fromJSON(jsonNode, json)
        ).collect(Collectors.toList());


        final JsonNode authenticationPrincipal = root.get("principal");
        if (authenticationPrincipal == null || !authenticationPrincipal.isObject()) {
            throw new AssertionError(String.format("The Swift credentials JSON did not have a 'principal' field " +
                    "or it was not an object: %s", json));
        }

        Principal principal = AuthenticationPrincipal.fromJSON(authenticationPrincipal.toString(), mapper);
        return new SwiftCredentials(hashPasswordList, principal);
    }

}
