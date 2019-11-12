package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.oracle.pic.casper.webserver.api.s3.S3XmlResult;

@JacksonXmlRootElement(localName = "RequestPaymentConfiguration")
public class BucketRequestPayment extends S3XmlResult {

    private static final String PAYER = "BucketOwner";

    @JsonIgnore
    @Override
    public String toString() {
        return "RequestPaymentConfiguration{" +
                "Payer='" + PAYER +
                '}';
    }

    public String getPayer() {
        return this.PAYER;
    }
}
