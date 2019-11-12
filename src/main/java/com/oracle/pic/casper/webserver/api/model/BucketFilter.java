package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonFilter;

@JsonFilter("storageTierBucketFilter")
public interface BucketFilter {
}
