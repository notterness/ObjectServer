package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.Map;

public class UpdateObjectMetadataDetails {

    @NotNull
    private final Map<String, String> metadata;

    @JsonCreator
    public UpdateObjectMetadataDetails(@JsonProperty(value = "metadata", required = true)
                                                   Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

}
