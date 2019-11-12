package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class ClientInfoResponse {
    @JsonProperty("vcnID")
    private final String vcnID;

    @JsonProperty("clientIP")
    private final String clientIP;

    @JsonProperty("mss")
    private final int mss;

    @JsonProperty("requestHeaders")
    private final Map<String, List<String>> requestHeaders;

    @JsonProperty("eagle")
    private final boolean isEagle;

    @JsonCreator
    public ClientInfoResponse(
            @JsonProperty("vcnID") String vcnID,
            @JsonProperty("clientIP") String clientIP,
            @JsonProperty("mss") int mss,
            @JsonProperty("requestHeaders") Map<String, List<String>> requestHeaders,
            @JsonProperty("eagle") boolean isEagle) {
        this.vcnID = vcnID;
        this.clientIP = clientIP;
        this.mss = mss;
        this.requestHeaders = requestHeaders;
        this.isEagle = isEagle;
    }

    public String getVcnID() {
        return vcnID;
    }

    public String getClientIP() {
        return clientIP;
    }

    public int getMss() {
        return mss;
    }

    public Map<String, List<String>> getRequestHeaders() {
        return requestHeaders;
    }

    public boolean isEagle() {
        return isEagle;
    }
}
