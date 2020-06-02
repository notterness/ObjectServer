package com.webutils.webserver.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.primitives.UnsignedBytes;

import javax.annotation.concurrent.Immutable;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

@Immutable
public final class Digest implements Comparable<Digest> {
    private final DigestAlgorithm algorithm;
    private final byte[] value;

    public Digest(DigestAlgorithm algorithm, byte[] value) {
        if (algorithm.getLengthInBytes() != (long)value.length) {
            throw new InvalidDigestException(String.format("%s must be exactly %s bytes (got %s)", algorithm.name(), algorithm.getLengthInBytes(), value.length));
        } else {
            this.algorithm = algorithm;
            this.value = (byte[])value.clone();
        }
    }

    @JsonCreator
    public static Digest fromBase64Encoded(@JsonProperty("algorithm") DigestAlgorithm algorithm, @JsonProperty("value") String base64Encoded) {
        byte[] value;
        if (algorithm == DigestAlgorithm.NONE) {
            value = new byte[0];
        } else {
            value = Base64.getDecoder().decode(base64Encoded);
        }

        return new Digest(algorithm, value);
    }

    public static Digest fromBase64UrlEncoded(DigestAlgorithm algorithm, String base64UrlEncoded) {
        byte[] value = Base64.getUrlDecoder().decode(base64UrlEncoded);
        return new Digest(algorithm, value);
    }

    @JsonIgnore
    public byte[] getValue() {
        return (byte[])this.value.clone();
    }

    @JsonProperty("value")
    public String getBase64Encoded() {
        return Base64.getEncoder().encodeToString(this.getValue());
    }

    @JsonIgnore
    public String getBase64UrlEncoded() {
        return Base64.getUrlEncoder().encodeToString(this.getValue());
    }

    public DigestAlgorithm getAlgorithm() {
        return this.algorithm;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            Digest digest = (Digest)o;
            return this.algorithm == digest.algorithm && Arrays.equals(this.value, digest.value);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.algorithm, Arrays.hashCode(this.value)});
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("algorithm", this.algorithm).add("value", this.getBase64Encoded()).toString();
    }

    public int compareTo(Digest other) {
        int compare = this.algorithm.compareTo(other.algorithm);
        if (compare == 0) {
            compare = Objects.compare(this.value, other.value, UnsignedBytes.lexicographicalComparator());
        }

        return compare;
    }
}
