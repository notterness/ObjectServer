package com.oracle.pic.casper.webserver.auth.dataplane;

import com.codahale.metrics.Histogram;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.metrics.MetricsBundle;

public interface AuthMetrics {
    // Requests to Identity

    // S3
    String S3_CACHE_SIZE = "identity.getsigningkey.cache.size";
    MetricsBundle GET_SIGNING_KEY = new MetricsBundle("identity.getsigningkey");

    // SWIFT
    MetricsBundle SWIFT_BASIC_AUTH = new MetricsBundle("identity.swiftbasicauth");
    String SWIFT_CACHE_SIZE = "identity.swiftcredentials.cache.size";
    Histogram SWIFT_CACHE_GET_TIME = Metrics.unaggregatedHistogram("identity.swiftauth.cache.get.time");
    Histogram SWIFT_SCRYPT_TIME = Metrics.unaggregatedHistogram("identity.swiftauth.scrypt.time");
    MetricsBundle SWIFT_CREDENTIALS = new MetricsBundle("identity.swiftcredentials");

    // V2
    MetricsBundle AUTHENTICATION = new MetricsBundle("identity.authentication");

    // AuthZ
    // Metric that tracks responses from Identity.
    MetricsBundle IDENTITY_AUTHZ = new MetricsBundle("identity.authorization");
    // Identity returns list of permissions in the authz call which can be empty or not matching the required ones.
    // This evaluation is done at web server and is tracked using this metric. There can be multiple identity requests
    // for one web server authz request due to retries.
    MetricsBundle WEB_SERVER_AUTHZ = new MetricsBundle("ws.authorization");

    // Cached auth
    MetricsBundle WS_SWIFT_AUTHENTICATION = new MetricsBundle("ws.swift.authentication");
}
