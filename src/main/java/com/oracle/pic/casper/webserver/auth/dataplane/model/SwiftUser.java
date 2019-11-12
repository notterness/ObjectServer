package com.oracle.pic.casper.webserver.auth.dataplane.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.MoreObjects;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Identify a Swift user. Users are only unique inside of a tenancy so we need to know both the tenancy and
 * the username. The tenancy can be identified either by OCID or name.
 */
public class SwiftUser {

    private final String tenantOCID;
    private final String tenantName;
    private final String userName;

    public SwiftUser(@Nullable String tenantOCID,
                     String tenantName,
                     String userName) {
        this.tenantOCID = tenantOCID;
        this.tenantName = tenantName;
        this.userName = userName;
    }

    public String getTenantOCID() {
        return tenantOCID;
    }

    public String getTenantName() {
        return tenantName;
    }

    public String getUserName() {
        return userName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SwiftUser that = (SwiftUser) o;
        return Objects.equals(tenantOCID, that.tenantOCID) &&
                Objects.equals(tenantName, that.tenantName) &&
                Objects.equals(userName, that.userName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenantOCID, tenantName, userName);
    }

    public static String toJSON(SwiftUser swiftUser, ObjectMapper mapper) {
        final ObjectNode json = mapper.createObjectNode();
        json.put("userName", swiftUser.getUserName());

        if (swiftUser.getTenantOCID() == null) {
            json.put("tenantName", swiftUser.getTenantName());
        } else {
            json.put("tenantId", swiftUser.getTenantOCID());
        }

        try {
            return mapper.writeValueAsString(json);
        } catch (JsonProcessingException ex) {
            throw new AssertionError("Failed to write a Swift user as JSON", ex);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("tenantOCID", tenantOCID)
                .add("tenantName", tenantName)
                .add("userName", userName)
                .toString();
    }
}
