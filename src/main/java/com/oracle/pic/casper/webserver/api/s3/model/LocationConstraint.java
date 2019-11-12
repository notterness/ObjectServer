package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.oracle.pic.casper.webserver.api.s3.S3XmlResult;

@JacksonXmlRootElement(localName = "LocationConstraint", namespace = "http://s3.amazonaws.com/doc/2006-03-01")
public class LocationConstraint extends S3XmlResult {
    private final String locationConstraint;

    public LocationConstraint(String locationConstraint) {
        this.locationConstraint = locationConstraint;
    }

    @JsonIgnore
    @Override
    public String toString() {
        return "LocationConstraint{" +
                "locationConstraint='" + locationConstraint +
                '}';
    }

    @JsonValue
    public String getLocationConstraint() {
        return this.locationConstraint;
    }
}
