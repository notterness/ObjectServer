package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 * The API Spec
 * https://bitbucket.oci.oraclecorp.com/projects/LUM/repos/hydra-pika-controlplane/browse/hydra-pika-controlplane-api-spec/specs/s2s.cond.yaml?at=public-api
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class UpdateLoggingRequestJson {

    @NotNull
    private final String logId;

    @NotNull
    private final String resource;

    @NotNull
    private final String category;

    @NotNull
    private final String tenantId;

    @JsonCreator
    public UpdateLoggingRequestJson(@JsonProperty(value = "logId", required = true) String logId,
                                    @JsonProperty(value = "resource", required = true) String resource,
                                    @JsonProperty(value = "category", required = true) String category,
                                    @JsonProperty(value = "tenancyId", required = true) String tenantId) {
        this.logId = logId;
        this.resource = resource;
        this.category = category;
        this.tenantId = tenantId;
    }

    public String getLogId() {
        return logId;
    }

    public String getResource() {
        return resource;
    }

    public String getCategory() {
        return category;
    }

    public String getTenantId() {
        return tenantId;
    }
}
