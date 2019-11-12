package com.oracle.pic.casper.webserver.api.common;

import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.rest.CommonHeaders;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;

public final class Md5Checksum implements Checksum {

    //the base64 checksum
    private final String base64;

    protected Md5Checksum(String base64) {
        this.base64 = Preconditions.checkNotNull(base64);
    }

    @Override
    public String getBase64() {
        return base64;
    }

    @Override
    public String getHex() {
        return ChecksumHelper.base64ToHex(base64);
    }

    @Override
    public String getQuotedHex() {
        return "\"" + getHex() + "\"";
    }

    @Override
    public void addHeaderToResponseV1(HttpServerResponse response) {
        response.putHeader(CommonHeaders.CASPER_CONTENT_MD5, getBase64());
    }

    @Override
    public void addHeaderToResponseV2Get(HttpServerResponse response) {
        response.putHeader(HttpHeaders.CONTENT_MD5, getBase64());
    }

    @Override
    public void addHeaderToResponseV2Put(HttpServerResponse response) {
        response.putHeader(HttpHeaderHelpers.OPC_CONTENT_MD5_HEADER, getBase64());
    }

    @Override
    public void addHeaderToResponseSwift(HttpServerResponse response) {
        response.putHeader(HttpHeaders.ETAG, getHex());
    }

    @Override
    public void addHeaderToResponseS3(HttpServerResponse response) {
        response.putHeader(HttpHeaders.ETAG, getQuotedHex());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Md5Checksum that = (Md5Checksum) o;

        return base64.equals(that.base64);
    }

    @Override
    public int hashCode() {
        return base64.hashCode();
    }

    @Override
    public String toString() {
        return "Md5Checksum{" +
            "base64='" + base64 + '\'' +
            '}';
    }
}
