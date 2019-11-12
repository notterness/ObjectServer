package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.oracle.pic.casper.common.util.CommonRequestContext;

import java.util.Objects;

/**
 * Class that encapsulates all the details for a pre authenticated request when it is being vended to the user
 * on creation.
 * Note that we use PreAuthenticatedRequestMetadata for accessing all the details of the PAR.
 * However the metadata does not have the nonce, since that is not stored in the Casper store. The resourceURI
 * vended here has the nonce in it and this is used only at PAR creation / provisioning time.
 */
@JsonDeserialize(builder = PreAuthenticatedRequest.Builder.class)
public class PreAuthenticatedRequest {

    // the URI that is used by the PAR user for getting access to the Casper resource i.e /b/bucket/o/object
    @JsonProperty("accessUri")
    private final String resourceUri;

    // the underlying PAR metadata
    @JsonUnwrapped
    @JsonProperty("parMetadata")
    private final PreAuthenticatedRequestMetadata parMetadata;

    public PreAuthenticatedRequest(@JsonProperty("accessUri") String resourceUri,
                                   @JsonProperty("parMetadata") PreAuthenticatedRequestMetadata parMetadata) {
        this.resourceUri = resourceUri;
        this.parMetadata = parMetadata;
    }

    public String getResourceUri() {
        return resourceUri;
    }

    @JsonUnwrapped
    public PreAuthenticatedRequestMetadata getParMetadata() {
        return parMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PreAuthenticatedRequest that = (PreAuthenticatedRequest) o;

        return Objects.equals(resourceUri, that.resourceUri) &&
                Objects.equals(parMetadata, that.parMetadata);

    }

    @Override
    public int hashCode() {
        int result = resourceUri != null ? resourceUri.hashCode() : 0;
        result = 31 * result + (parMetadata != null ? parMetadata.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PreAuthenticatedRequest{" +
                "resourceUri='" + CommonRequestContext.casperSanitize(resourceUri) + '\'' +
                ", parMetadata=" + parMetadata +
                '}';
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        @JsonProperty("accessUri")
        private String resourceUri;
        @JsonUnwrapped
        @JsonProperty("parMetadata")
        private PreAuthenticatedRequestMetadata parMetadata;

        public Builder resourceUri(String uri) {
            this.resourceUri = uri;
            return this;
        }

        public Builder parMetadata(PreAuthenticatedRequestMetadata parMetadata) {
            this.parMetadata = parMetadata;
            return this;
        }

        public PreAuthenticatedRequest build() {
            return new PreAuthenticatedRequest(resourceUri, parMetadata);
        }
    }
}
