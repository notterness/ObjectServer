package com.oracle.pic.casper.webserver.auth.dataplane.sigv4;

import com.google.common.base.Preconditions;

import java.util.Arrays;

/**
 * Wraps a SIGV4 signing key. The key is stored as a byte array and
 * closing the key overwrites the byte array.
 */
public class SIGV4SigningKey {

    /**
     * The signing key.
     */
    private final byte[] key;

    /**
     * Construct a new SIGV4SigningKey. The array that is passed in is copied
     * so it can/should be overwritten after this object is created.
     */
    public SIGV4SigningKey(byte[] key) {
        Preconditions.checkNotNull(key);
        this.key = Arrays.copyOf(key, key.length);
    }

    /**
     * Get the signing key. This returns a copy of the internal state.
     */
    public byte[] getKey() {
        return Arrays.copyOf(key, key.length);
    }

    @Override
    public String toString() {
        // To avoid keys leaking into logs we will not return the entire
        // key here. We do print the first few characters to make debugging
        // easier.
        final StringBuilder sb = new StringBuilder();
        sb.append("SIGV4SigningKey(");
        for (int i = 0; i < Math.min(8, this.key.length); ++i) {
            sb.append(String.format("%02x", this.key[i]));
        }
        sb.append("...)");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SIGV4SigningKey that = (SIGV4SigningKey) o;
        return Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }
}
