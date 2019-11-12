package com.oracle.pic.casper.webserver.api.swift;

import com.codahale.metrics.Histogram;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.webserver.util.WebServerMetricsBundle;

public class SwiftGetMetricsBundle extends WebServerMetricsBundle {

    private static final String DLO_FIRST_BYTE_LATENCY_SUFFIX = ".latency.dlo.firstbyte";
    private final Histogram dloFirstByteLatency;

    public SwiftGetMetricsBundle(String prefix) {
        super(prefix, true, true);
        dloFirstByteLatency = Metrics.unaggregatedHistogram(prefix + DLO_FIRST_BYTE_LATENCY_SUFFIX);
    }

    public Histogram getDLOFirstByteLatency() {
        return dloFirstByteLatency;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        SwiftGetMetricsBundle that = (SwiftGetMetricsBundle) o;

        return dloFirstByteLatency.equals(that.dloFirstByteLatency);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + dloFirstByteLatency.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SwiftLargeObjectMetricsBundle{" +
            "dloFirstByteLatency=" + dloFirstByteLatency +
            '}';
    }
}
