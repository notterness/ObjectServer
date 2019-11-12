package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.identity.authentication.Principal;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Class that encapsulates all data members of a pre authenticated request (PAR)
 */
public class PreAuthenticatedRequestMetadata {

    /**
     * Enum for pre authenticated request status
     */
    public enum Status {
        Active(1), Revoked(2);

        private final int value;

        Status(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static Status parseStatus(String status) {
            return parseStatus(Integer.parseInt(status));
        }

        public static Status parseStatus(int status) {
            switch (status) {
                case 1: return Status.Active;
                case 2: return Status.Revoked;
                default: throw new IllegalArgumentException("invalid argument for status " + status);
            }
        }
    }

    /**
     * Type of operation that is permitted by the pre-authenticated request
     */
    public enum AccessType {
        ObjectRead(1, CasperOperation.HEAD_OBJECT, CasperOperation.GET_OBJECT),
        ObjectWrite(2, CasperOperation.PUT_OBJECT, CasperOperation.CREATE_MULTIPART_UPLOAD,
                CasperOperation.UPLOAD_PART, CasperOperation.COMMIT_MULTIPART_UPLOAD,
                CasperOperation.ABORT_MULTIPART_UPLOAD),
        AnyObjectWrite(3, CasperOperation.PUT_OBJECT, CasperOperation.CREATE_MULTIPART_UPLOAD,
                CasperOperation.UPLOAD_PART, CasperOperation.COMMIT_MULTIPART_UPLOAD,
                CasperOperation.ABORT_MULTIPART_UPLOAD),
        ObjectReadWrite(4, CasperOperation.HEAD_OBJECT, CasperOperation.GET_OBJECT, CasperOperation.PUT_OBJECT,
                CasperOperation.CREATE_MULTIPART_UPLOAD, CasperOperation.UPLOAD_PART,
                CasperOperation.COMMIT_MULTIPART_UPLOAD, CasperOperation.ABORT_MULTIPART_UPLOAD);

        private final int value;
        private final HashSet<CasperOperation> casperOps;

        AccessType(int value, CasperOperation... ops) {
            this.value = value;
            this.casperOps = new HashSet<>(Arrays.asList(ops));
        }

        public int getValue() {
            return value;
        }

        public static AccessType parseAccessType(String op) {
            return parseAccessType(Integer.parseInt(op));
        }

        public Set<CasperOperation> getCasperOps() {
            return casperOps;
        }

        public static AccessType parseAccessType(int op) {
            switch (op) {
                case 1: return AccessType.ObjectRead;
                case 2: return AccessType.ObjectWrite;
                case 3: return AccessType.AnyObjectWrite;
                case 4: return AccessType.ObjectReadWrite;
                default: throw new IllegalArgumentException("invalid argument for operation " + op);
            }
        }
    }

    /**
     * possible versions for PARs, helps when we want to revise the schema for PARs
     */
    public enum Version {
        V1(1);

        private final int value;

        Version(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static Version parseVersion(String v) {
            return parseVersion(Integer.parseInt(v));
        }

        public static Version parseVersion(int v) {
            switch (v) {
                case 1: return Version.V1;
                default: throw new IllegalArgumentException("invalid argument for version " + v);
            }
        }
    }

    // the unique identifier of the par
    @JsonProperty("id")
    private final String parId;

    // the friendly name chosen by the user for help with PAR management
    @JsonProperty("name")
    private final String name;

    // the operation that is permitted by the PAR
    @JsonProperty("accessType")
    private final AccessType accessType;

    // the bucket that this PAR permits access to
    @JsonIgnore
    private String bucketName;

    // optional field - name of object that the PAR permits access to
    @JsonProperty("objectName")
    private final String objectName;

    // the par schema version
    @JsonIgnore
    private Version version;

    // the PAR status i.e ACTIVE, REVOKED etc
    @JsonIgnore
    private Status status;

    // Information about the identity principal
    @JsonIgnore
    private AuthenticationInfo authInfo;

    // PAR creation date
    @JsonProperty("timeCreated")
    private final Instant timeCreated;

    // PAR expiration date
    @JsonProperty("timeExpires")
    private final Instant timeExpires;

    @JsonCreator
    public PreAuthenticatedRequestMetadata(@JsonProperty("id") String parId,
                                           @JsonProperty("name") String name,
                                           @JsonProperty("accessType") AccessType accessType,
                                           @JsonProperty("objectName") String objectName,
                                           @JsonProperty("timeCreated") Instant timeCreated,
                                           @JsonProperty("timeExpires") Instant timeExpires) {
        this.parId = parId;
        this.name = name;
        this.accessType = accessType;
        this.objectName = objectName;
        this.timeCreated = timeCreated;
        this.timeExpires = timeExpires;
    }

    public PreAuthenticatedRequestMetadata(String parId,
                                           String name,
                                           AccessType accessType,
                                           String bucketName,
                                           String objectName,
                                           Version version,
                                           Status status,
                                           AuthenticationInfo authInfo,
                                           Instant timeCreated,
                                           Instant timeExpires) {
        this.parId = parId;
        this.name = name;
        this.accessType = accessType;
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.version = version;
        this.status = status;
        this.authInfo = authInfo;
        this.timeCreated = timeCreated;
        this.timeExpires = timeExpires;
    }

    public String getParId() {
        return parId;
    }

    public String getName() {
        return name;
    }

    public AccessType getAccessType() {
        return accessType;
    }

    public String getBucketName() {
        return bucketName;
    }

    @JsonIgnore
    public Optional<String> getObjectName() {
        return Optional.ofNullable(objectName);
    }

    @JsonProperty("objectName")
    public String getObjectNameField() {
        return objectName;
    }

    public Version getVersion() {
        return version;
    }

    public Status getStatus() {
        return status;
    }

    public AuthenticationInfo getAuthInfo() {
        return authInfo;
    }

    @JsonIgnore
    @VisibleForTesting
    public Principal getPrincipal() {
        return authInfo.getMainPrincipal();
    }

    public Instant getTimeCreated() {
        return timeCreated;
    }

    public Instant getTimeExpires() {
        return timeExpires;
    }

    public PreAuthenticatedRequestMetadata cloneAndOverrideParId(String newParId) {
        return new PreAuthenticatedRequestMetadata(newParId, name, accessType, bucketName, objectName, version, status,
                authInfo, timeCreated, timeExpires);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PreAuthenticatedRequestMetadata that = (PreAuthenticatedRequestMetadata) o;
        return Objects.equals(parId, that.parId) &&
                Objects.equals(name, that.name) &&
                accessType == that.accessType &&
                Objects.equals(bucketName, that.bucketName) &&
                Objects.equals(objectName, that.objectName) &&
                version == that.version &&
                status == that.status &&
                Objects.equals(authInfo, that.authInfo) &&
                Objects.equals(timeCreated, that.timeCreated) &&
                Objects.equals(timeExpires, that.timeExpires);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parId, name, accessType, bucketName, objectName, version, status,
                authInfo.getMainPrincipal().getTenantId(), authInfo.getMainPrincipal().getSubjectId(),
                timeCreated, timeExpires);
    }

    @Override
    public String toString() {
        return "PreAuthenticatedRequest{" +
                "parId='" + parId + '\'' +
                ", name='" + name + '\'' +
                ", accessType=" + accessType +
                ", bucketName=" + bucketName +
                ", objectName=" + objectName +
                ", version='" + version + '\'' +
                ", status=" + status +
                ", authInfo=" + authInfo +
                ", timeCreated=" + timeCreated +
                ", timeExpires=" + timeExpires +
                '}';
    }
}
