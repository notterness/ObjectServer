package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class ReencryptObjectDetails {
    private final String kmsKeyId;
    public ReencryptObjectDetails(@JsonProperty("kmsKeyId") String kmsKeyId) {
        this.kmsKeyId = kmsKeyId;
    }

    public String getKmsKeyId() {
        return kmsKeyId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReencryptObjectDetails)) return false;
        ReencryptObjectDetails that = (ReencryptObjectDetails) o;
        return Objects.equals(kmsKeyId, that.kmsKeyId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kmsKeyId);
    }
}
