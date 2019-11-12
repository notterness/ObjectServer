package com.oracle.pic.casper.webserver.api.common;

import io.vertx.core.http.HttpServerResponse;
import org.apache.commons.codec.DecoderException;

public interface Checksum {

    static Checksum fromBase64(String base64) {
        return new Md5Checksum(base64);
    }

    static Checksum fromHex(String hex) throws DecoderException {
        return new Md5Checksum(ChecksumHelper.hexToBase64(hex));
    }

    static Checksum fromMultipartBase64(String base64, int partCount) {
        return new MultipartMd5Checksum(base64, partCount);
    }

    static Checksum fromMultipartHex(String hex, int partCount) throws DecoderException {
        return new MultipartMd5Checksum(ChecksumHelper.hexToBase64(hex), partCount);
    }

    String getBase64();

    String getHex();

    String getQuotedHex();

    void addHeaderToResponseV1(HttpServerResponse response);

    void addHeaderToResponseV2Get(HttpServerResponse response);

    void addHeaderToResponseV2Put(HttpServerResponse response);

    void addHeaderToResponseSwift(HttpServerResponse response);

    void addHeaderToResponseS3(HttpServerResponse response);
}
