package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.Preconditions;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.pars.BackendParId;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nullable;
import java.util.Optional;

public final class ListPreAuthenticatedRequestsRequest {

    // the routing context of the request
    private final RoutingContext context;

    // the authentication info to send to Identity
    private final AuthenticationInfo authInfo;

    // the bucket name of the PARs to list
    private final String bucketName;

    // page limit for pagination
    private final int pageLimit;

    // the object name prefix to list by - can be null
    private final String objectNamePrefix;

    // last item in previous page - can be null
    private final String nextPageToken;

    // the parId to lookup - can be null
    private final BackendParId parId;

    private ListPreAuthenticatedRequestsRequest(RoutingContext context,
                                                AuthenticationInfo authInfo,
                                                String bucketName,
                                                int pageLimit,
                                                @Nullable String objectNamePrefix,
                                                @Nullable String nextPageToken,
                                                @Nullable BackendParId parId) {
        Preconditions.checkArgument(pageLimit > 0, "pageLimit must be positive");
        this.context = context;
        this.authInfo = authInfo;
        this.bucketName = bucketName;
        this.pageLimit = pageLimit;
        this.objectNamePrefix = objectNamePrefix;
        this.nextPageToken = nextPageToken;
        this.parId = parId;
    }

    public RoutingContext getContext() {
        return context;
    }

    public AuthenticationInfo getAuthInfo() {
        return authInfo;
    }

    public String getBucketName() {
        return bucketName;
    }

    public int getPageLimit() {
        return pageLimit;
    }

    public Optional<String> getObjectNamePrefix() {
        return Optional.ofNullable(objectNamePrefix);
    }

    public Optional<String> getNextPageToken() {
        return Optional.ofNullable(nextPageToken);
    }

    public Optional<BackendParId> getParId() {
        return Optional.ofNullable(parId);
    }

    @Override
    public String toString() {
        return "ListPreAuthenticatedRequestsRequest{" +
                "authInfo=" + authInfo +
                ", bucketName='" + bucketName + '\'' +
                ", pageLimit=" + pageLimit +
                ", objectNamePrefix='" + objectNamePrefix + '\'' +
                ", nextPageToken='" + nextPageToken + '\'' +
                ", parId='" + parId + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private RoutingContext context;
        private AuthenticationInfo authInfo;
        private String bucketName;
        private int pageLimit;
        private String objectNamePrefix;
        private String nextPageToken;
        private BackendParId parId;

        public Builder withContext(RoutingContext routingContext) {
            this.context = routingContext;
            return this;
        }

        public Builder withAuthInfo(AuthenticationInfo authenticationInfo) {
            this.authInfo = authenticationInfo;
            return this;
        }

        public Builder withBucket(String bucket) {
            this.bucketName = bucket;
            return this;
        }

        public Builder withPageLimit(int limit) {
            this.pageLimit = limit;
            return this;
        }

        public Builder withObjectNamePrefix(String prefix) {
            this.objectNamePrefix = prefix;
            return this;
        }

        public Builder withNextPageToken(String token) {
            this.nextPageToken = token;
            return this;
        }

        public Builder withParId(BackendParId id) {
            this.parId = id;
            return this;
        }

        public ListPreAuthenticatedRequestsRequest build() {

            Preconditions.checkNotNull(context, "routing context must not be null");
            Preconditions.checkNotNull(authInfo, "authInfo must not be null");
            Preconditions.checkNotNull(bucketName, "bucketName must not be null");

            return new ListPreAuthenticatedRequestsRequest(context, authInfo, bucketName, pageLimit, objectNamePrefix,
                    nextPageToken, parId);
        }
    }
}
