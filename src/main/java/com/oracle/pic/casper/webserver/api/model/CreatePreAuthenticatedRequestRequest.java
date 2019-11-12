package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.Preconditions;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Class that encapsulates all request details for creating a pre authenticated request (PAR)
 */
public final class CreatePreAuthenticatedRequestRequest {

    // the routing context of the request
    private final RoutingContext context;

    // the authentication info to send to Identity
    private final AuthenticationInfo authInfo;

    // the unique identifier to the PAR
    private final String verifierId;

    // the operation permitted by the PAR
    private final PreAuthenticatedRequestMetadata.AccessType accessType;

    // the bucket that the PAR operates on
    private final String bucketName;

    // nullable field - the object that the PAR operates on
    private final String objectName;

    // user friendly name useful for PAR management
    private final String name;

    // expiration after which the PAR will not permit access to the resource i.e /b/bucket/o/object
    private final Instant timeExpires;

    private CreatePreAuthenticatedRequestRequest(RoutingContext context,
                                                 AuthenticationInfo authInfo,
                                                 String verifierId,
                                                 PreAuthenticatedRequestMetadata.AccessType accessType,
                                                 String bucketName,
                                                 @Nullable String objectName,
                                                 String name,
                                                 Instant timeExpires) {
        this.context = context;
        this.authInfo = authInfo;
        this.verifierId = verifierId;
        this.accessType = accessType;
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.name = name;
        this.timeExpires = timeExpires;
    }

    public RoutingContext getContext() {
        return context;
    }

    public AuthenticationInfo getAuthInfo() {
        return authInfo;
    }

    public String getVerifierId() {
        return verifierId;
    }

    public PreAuthenticatedRequestMetadata.AccessType getAccessType() {
        return accessType;
    }

    public String getBucketName() {
        return bucketName;
    }

    public Optional<String> getObjectName() {
        return Optional.ofNullable(objectName);
    }

    public String getName() {
        return name;
    }

    public Instant getTimeExpires() {
        return timeExpires;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreatePreAuthenticatedRequestRequest that = (CreatePreAuthenticatedRequestRequest) o;
        return Objects.equals(context, that.context) &&
                Objects.equals(authInfo, that.authInfo) &&
                Objects.equals(verifierId, that.verifierId) &&
                accessType == that.accessType &&
                Objects.equals(bucketName, that.bucketName) &&
                Objects.equals(objectName, that.objectName) &&
                Objects.equals(name, that.name) &&
                Objects.equals(timeExpires, that.timeExpires);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, authInfo, verifierId, accessType, bucketName, objectName, name, timeExpires);
    }

    @Override
    public String toString() {
        return "CreatePreAuthenticatedRequestRequest{" +
                "authInfo=" + authInfo +
                ", verifierId='" + verifierId + '\'' +
                ", accessType=" + accessType +
                ", bucketName='" + bucketName + '\'' +
                ", objectName='" + objectName + '\'' +
                ", name='" + name + '\'' +
                ", timeExpires=" + timeExpires +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private RoutingContext context;
        private AuthenticationInfo authInfo;
        private String verifierId;
        private PreAuthenticatedRequestMetadata.AccessType accessType;
        private String bucketName;
        private String objectName;
        private String name;
        private Instant timeExpires;

        public Builder withContext(RoutingContext routingContext) {
            this.context = routingContext;
            return this;
        }

        public Builder withAuthInfo(AuthenticationInfo authenticationInfo) {
            this.authInfo = authenticationInfo;
            return this;
        }

        public Builder withVerifierId(String id) {
            this.verifierId = id;
            return this;
        }

        public Builder withAccessType(PreAuthenticatedRequestMetadata.AccessType access) {
            this.accessType = access;
            return this;
        }

        public Builder withName(String parName) {
            this.name = parName;
            return this;
        }

        public Builder withBucket(String bucket) {
            this.bucketName = bucket;
            return this;
        }

        public Builder withObject(String objName) {
            this.objectName = objName;
            return this;
        }

        public Builder withTimeExpires(Instant instant) {
            this.timeExpires = instant;
            return this;
        }

        public CreatePreAuthenticatedRequestRequest build() {

            Preconditions.checkNotNull(context, "routing context must not be null");
            Preconditions.checkNotNull(authInfo, "authInfo must not be null");
            Preconditions.checkNotNull(verifierId, "verifier id must not be null");
            Preconditions.checkNotNull(accessType, "accessType must not be null");
            Preconditions.checkNotNull(bucketName, "bucket must not be null");
            Preconditions.checkNotNull(name, "PAR name must not be null");
            Preconditions.checkNotNull(timeExpires, "expiration must not be null");
            Preconditions.checkArgument(timeExpires.isAfter(Instant.now()), "expiration must be set in the future");

            if (objectName != null) {
                Preconditions.checkArgument(
                        !accessType.equals(PreAuthenticatedRequestMetadata.AccessType.AnyObjectWrite),
                        "accessType " + accessType.name() + " not allowed when object name is specified");
            } else {
                Preconditions.checkArgument(
                        accessType.equals(PreAuthenticatedRequestMetadata.AccessType.AnyObjectWrite),
                        "accessType " + accessType.name() + " not allowed when object name is not specified");
            }

            return new CreatePreAuthenticatedRequestRequest(context, authInfo, verifierId, accessType, bucketName,
                    objectName, name, timeExpires);
        }
    }
}
