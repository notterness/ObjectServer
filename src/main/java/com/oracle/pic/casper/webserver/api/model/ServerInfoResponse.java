package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;

public final class ServerInfoResponse {

    @JsonProperty("region")
    private final String region;

    @JsonProperty("stage")
    private final String stage;

    @JsonProperty("ad")
    private final String availabilityDomain;

    @JsonProperty("application")
    private final String application;

    @JsonProperty("build")
    private final String build;

    @JsonProperty("host")
    private final String hostname;

    @JsonProperty("fixed-jiras")
    private final Collection<String> fixedJiras;

    @JsonCreator
    public ServerInfoResponse(@JsonProperty("region")String region,
                              @JsonProperty("stage")String stage,
                              @JsonProperty("ad") String availabilityDomain,
                              @JsonProperty("application")String application,
                              @JsonProperty("build")String build,
                              @JsonProperty("host") String hostname,
                              @JsonProperty("fixed-jiras") Collection<String> fixedJiras) {
        this.region = region;
        this.stage = stage;
        this.availabilityDomain = availabilityDomain;
        this.application = application;
        this.build = build;
        this.hostname = hostname;
        this.fixedJiras = fixedJiras;
    }

    public String getRegion() {
        return region;
    }

    public String getStage() {
        return stage;
    }

    public String getAvailabilityDomain() {
        return availabilityDomain;
    }

    public String getApplication() {
        return application;
    }

    public String getBuild() {
        return build;
    }

    public String getHostname() {
        return hostname;
    }

    public Collection<String> getFixedJiras() {
        return fixedJiras;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String region;
        private String stage;
        private String availabilityDomain;
        private String application;
        private String build;
        private String hostname;
        private Collection<String> fixedJiras;

        public Builder setRegion(String region) {
            this.region = region;
            return this;
        }

        public Builder setStage(String stage) {
            this.stage = stage;
            return this;
        }

        public Builder setAvailabilityDomain(String availabilityDomain) {
            this.availabilityDomain = availabilityDomain;
            return this;
        }

        public Builder setApplication(String application) {
            this.application = application;
            return this;
        }

        public Builder setBuild(String build) {
            this.build = build;
            return this;
        }

        public Builder setHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder setFixedJiras(Collection<String> fixedJiras) {
            this.fixedJiras = fixedJiras;
            return this;
        }

        public ServerInfoResponse build() {
            return new ServerInfoResponse(
                    region,
                    stage,
                    availabilityDomain,
                    application,
                    build,
                    hostname,
                    fixedJiras
            );
        }
    }
}
