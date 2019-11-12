package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.oracle.pic.casper.webserver.api.s3.S3XmlResult;

@JacksonXmlRootElement(localName = "AccelerateConfiguration")
public class BucketAccelerateConfiguration extends S3XmlResult {

    public BucketAccelerateConfiguration() {
    }
}
