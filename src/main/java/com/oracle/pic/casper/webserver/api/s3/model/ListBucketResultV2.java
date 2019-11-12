package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.oracle.pic.casper.webserver.api.s3.S3XmlResult;

import java.util.List;

/**
 * Response for GET bucket v2.  See <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html"> for
 * documentation.
 */
@JacksonXmlRootElement(localName = "ListBucketResult")
@JsonPropertyOrder({"Name", "Prefix", "ContinuationToken", "StartAfter", "NextContinuationToken", "KeyCount", "MaxKeys",
    "Delimiter", "IsTruncated", "EncodingType", "Contents", "CommonPrefixes"})
public final class ListBucketResultV2 extends S3XmlResult {

    private final ListBucketResultV1 v1;
    private final String startAfter;
    private final String continuationToken;
    private final String nextContinuationToken;

    public ListBucketResultV2(ListBucketResultV1 v1, String startAfter, String continuationToken,
                              String nextContinuationToken) {
        this.v1 = v1;
        this.startAfter = startAfter;
        this.continuationToken = continuationToken;
        this.nextContinuationToken = nextContinuationToken;
    }

    public String getName() {
        return v1.getName();
    }

    public String getPrefix() {
        return v1.getPrefix();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getDelimiter() {
        return v1.getDelimiter();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getStartAfter() {
        return startAfter;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getContinuationToken() {
        return continuationToken;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getNextContinuationToken() {
        return nextContinuationToken;
    }

    public int getKeyCount() {
        int size = 0;
        List<ListBucketResultV1.Content> contents = (List) getContents();
        List<ListBucketResultV1.Prefix> prefixes = (List) getCommonPrefixes();
        if (contents != null) {
            size += contents.size();
        }
        if (prefixes != null) {
            size += prefixes.size();
        }
        return size;
    }

    public int getMaxKeys() {
        return v1.getMaxKeys();
    }

    public boolean getIsTruncated() {
        return v1.getIsTruncated();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getEncodingType() {
        return v1.getEncodingType();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Iterable<ListBucketResultV1.Content> getContents() {
        return v1.getContents();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Iterable<ListBucketResultV1.Prefix> getCommonPrefixes() {
        return v1.getCommonPrefixes();
    }
}
