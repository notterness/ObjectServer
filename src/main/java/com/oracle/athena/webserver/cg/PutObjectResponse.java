package com.oracle.pic.casper.webserver.api.backend.putobject.storageserver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class PutObjectResponse {
    @JsonProperty("content-length")
    private final int length;

    @JsonProperty("digest_value")
    private final String digestValue;

    @JsonProperty("digest_algorithm")
    private final String digestAlgorithm;

    @JsonProperty("volumeGeneration")
    private final int volumeGeneration;

    @JsonCreator
    public PutObjectResponse(@JsonProperty("content-length") int length,
                             @JsonProperty("digest_value") String digestValue,
                             @JsonProperty("digest_algorithm") String digestAlgorithm,
                             @JsonProperty("volumeGeneration") int volumeGeneration) {
        this.length = length;
        this.digestValue = digestValue;
        this.digestAlgorithm = digestAlgorithm;
        this.volumeGeneration = volumeGeneration;
    }

    public int getLength() {
        return length;
    }

    public String getDigestValue() {
        return digestValue;
    }

    public String getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public int getVolumeGeneration() {
        return volumeGeneration;
    }
}
