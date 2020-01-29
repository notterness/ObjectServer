package com.webutils.webserver.common;

public enum DigestAlgorithm {
    NONE(0),
    MD5(16),
    SHA256(32);

    private final long lengthInBytes;

    private DigestAlgorithm(int lengthInBytes) {
        this.lengthInBytes = (long)lengthInBytes;
    }

    public long getLengthInBytes() {
        return this.lengthInBytes;
    }

    public long getLengthInBase64() {
        double lengthWithoutPadding = 1.3333333333333333D * (double)this.getLengthInBytes();
        return (long)(4.0D * Math.ceil(lengthWithoutPadding / 4.0D));
    }

    public long getLengthInBase16() {
        return this.lengthInBytes * 2L;
    }
}

