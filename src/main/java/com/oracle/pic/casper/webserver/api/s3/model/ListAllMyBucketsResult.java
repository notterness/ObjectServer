package com.oracle.pic.casper.webserver.api.s3.model;


import com.oracle.pic.casper.webserver.api.model.BucketSummary;
import com.oracle.pic.casper.webserver.api.model.PaginatedList;
import com.oracle.pic.casper.webserver.api.s3.S3XmlResult;
import com.oracle.pic.identity.authentication.Principal;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 * When instances of this class are passed to
 * {@link com.fasterxml.jackson.dataformat.xml.XmlMapper#writeValue(java.io.Writer, Object) XmlMapper.writeValue(...)}
 * xml is produced as described by <b>Response Elements</b> on the page
 * <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html">GET Service</a>.</p>
 * <p>
 * See note in {@link S3XmlResult} on class and method names.</p>
 */
public class ListAllMyBucketsResult extends S3XmlResult {

    private final Owner owner;

    private final Buckets buckets;

    public ListAllMyBucketsResult(Principal principal, PaginatedList<BucketSummary> summaries) {
        this.owner = new Owner(principal);
        this.buckets = new Buckets(summaries.getItems().stream().map(summary -> new Bucket(summary))
            .collect(Collectors.toList()));
    }

    //this class is needed for an extra layer of wrapping
    private static final class Buckets {

        private final List<Bucket> bucket;

        Buckets(List<Bucket> bucket) {
            this.bucket = bucket;
        }

        public Iterable<Bucket> getBucket() {
            return bucket;
        }
    }

    public Owner getOwner() {
        return owner;
    }

    public Buckets getBuckets() {
        return buckets;
    }

    private static final class Bucket {

        private final String name;
        private final Date creationDate;

        Bucket(BucketSummary summary) {
            this.name = summary.getBucketName();
            this.creationDate = Date.from(summary.getTimeCreated());
        }

        public String getName() {
            return name;
        }

        public Date getCreationDate() {
            return creationDate;
        }
    }
}
