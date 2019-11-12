package com.oracle.pic.casper.webserver.auth.dataplane.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;

/**
 * SwiftAuthRequests contains methods for serializing a Swift basic auth request to JSON that is suitable for requests
 * to the identity data plane service.
 *
 * The JSON format is:
 *
 *  {
 *      "userName": "...",
 *      "password": "...",
 *      "tenantName": "....",
 *      "tenantOCID": "...."
 *  }
 *
 * Either the "tenantName" or "tenantOCID" field will be present, but never both.
 */
public final class SwiftAuthRequests {
    private SwiftAuthRequests() {

    }

    /**
     * Serialize the username, password and tenant info to a JSON string.
     *
     * If tenantOCID is null, only the "tenantName" field will appear in the JSON, otherwise only the "tenantId" field
     * will appear.
     */
    public static String toJSON(AuthUserAndPass authUserAndPass,
                                String tenantName,
                                @Nullable String tenantOCID,
                                ObjectMapper mapper) {
        final ObjectNode json = mapper.createObjectNode();
        json.put("userName", authUserAndPass.getUsername());
        json.put("password", authUserAndPass.getPassword());

        if (tenantOCID == null) {
            json.put("tenantName", tenantName);
        } else {
            json.put("tenantId", tenantOCID);
        }

        try {
            return mapper.writeValueAsString(json);
        } catch (JsonProcessingException ex) {
            throw new AssertionError("Failed to write a Swift basic auth request as JSON", ex);
        }
    }
}
