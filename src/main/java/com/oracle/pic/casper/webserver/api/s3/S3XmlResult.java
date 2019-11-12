package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

/**
 * Classes that extend {@code S3XmlResult} will be written as xml using an
 * {@link com.fasterxml.jackson.dataformat.xml.XmlMapper}.  The resulting xml must conform to the published S3 API
 * format.  The class and method names have been written carefully, to generate the correct xml and sometimes do not
 * adhere to normal naming conventions.
 */
public abstract class S3XmlResult {
    @JacksonXmlProperty(isAttribute = true, localName = "xmlns")
    protected final String getXmlNamespace() {
        return "http://s3.amazonaws.com/doc/2006-03-01";
    }
}
