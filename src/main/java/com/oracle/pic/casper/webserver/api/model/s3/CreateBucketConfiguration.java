package com.oracle.pic.casper.webserver.api.model.s3;

public class CreateBucketConfiguration {

    private String locationConstraint;

    public void setLocationConstraint(String constraint) {
        this.locationConstraint = constraint;
    }

    public String getLocationConstraint() {
        return locationConstraint;
    }
}
