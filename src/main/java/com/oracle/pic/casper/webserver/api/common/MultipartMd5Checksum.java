package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.rest.CommonHeaders;
import io.vertx.core.http.HttpServerResponse;
import org.apache.http.HttpHeaders;

public final class MultipartMd5Checksum implements Checksum {

    //the base64 checksum
    private final String base64;

    //number of parts
    private final int partCount;

    protected MultipartMd5Checksum(String base64, int partCount) {
        this.base64 = com.google.common.base.Preconditions.checkNotNull(base64);
        this.partCount = partCount;
    }

    @Override
    public String getBase64() {
        return base64 + "-" + partCount;
    }

    @Override
    public String getHex() {
        return ChecksumHelper.base64ToHex(base64) + "-" + partCount;
    }

    @Override
    public String getQuotedHex() {
        return "\"" + getHex() + "\"";
    }

    @Override
    public void addHeaderToResponseV1(HttpServerResponse response) {
        response.putHeader(CommonHeaders.CASPER_MULTIPART_MD5, getBase64());
    }

    @Override
    public void addHeaderToResponseV2Get(HttpServerResponse response) {
        response.putHeader(CommonHeaders.CASPER_MULTIPART_MD5, getBase64());
    }

    @Override
    public void addHeaderToResponseV2Put(HttpServerResponse response) {
        response.putHeader(CommonHeaders.CASPER_MULTIPART_MD5, getBase64());
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

        MultipartMd5Checksum that = (MultipartMd5Checksum) o;

        if (partCount != that.partCount) return false;
        return base64.equals(that.base64);
    }

    @Override
    public int hashCode() {
        int result = base64.hashCode();
        result = 31 * result + partCount;
        return result;
    }

    @Override
    public String toString() {
        return "MultipartMd5Checksum{" +
            "base64='" + base64 + '\'' +
            ", partCount=" + partCount +
            '}';
    }
}
