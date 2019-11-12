package com.oracle.pic.casper.webserver.auth.dataplane.exceptions;


public class IncorrectRegionException extends RuntimeException {
    /**
     * The expected region of the request
     */
    private final String expectedRegion;
    /**
     * The actual region provided in the request
     */
    private final String region;

    public IncorrectRegionException(String message, String expectedRegion, String region) {
        super(message);
        this.expectedRegion = expectedRegion;
        this.region = region;
    }

    public String getExpectedRegion() {
        return expectedRegion;
    }

    public String getRegion() {
        return region;
    }
}
