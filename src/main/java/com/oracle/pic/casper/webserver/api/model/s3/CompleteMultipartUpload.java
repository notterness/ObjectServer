package com.oracle.pic.casper.webserver.api.model.s3;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.oracle.pic.casper.webserver.api.s3.S3HttpHelpers;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

@SuppressFBWarnings(value = {"NP_UNWRITTEN_FIELD", "UWF_NULL_FIELD"})
public class CompleteMultipartUpload {

    public static final class Part {
        private Integer partNumber;

        @JacksonXmlProperty(localName = "ETag")
        private String etag;

        public Part() {
            partNumber = null;
            etag = null;
        }

        public Integer getPartNumber() {
            return partNumber;
        }

        public String getETag() {
            // Some clients send the ETag as <ETag>"value"</ETag> while others
            // may send <ETag>value</ETag>. To handle both cases we will trim
            // any quotes here.
            return S3HttpHelpers.unquoteIfNeeded(etag);
        }
    }

    @JacksonXmlElementWrapper(useWrapping = false)
    private List<Part> part;

    public CompleteMultipartUpload() {
    }

    public List<Part> getPart() {
        return part;
    }
}
