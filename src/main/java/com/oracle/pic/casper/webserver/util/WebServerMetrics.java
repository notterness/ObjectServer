package com.oracle.pic.casper.webserver.util;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.metrics.LogMetrics;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.metrics.Metrics.LongGauge;
import com.oracle.pic.casper.common.metrics.MetricsBundle;
import com.oracle.pic.casper.common.metrics.PercentageHistogram;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3MetricsBundle;
import com.oracle.pic.casper.webserver.api.auditing.ObjectEvent;
import com.oracle.pic.casper.webserver.api.swift.SwiftGetMetricsBundle;
import com.oracle.pic.telemetry.commons.metrics.model.MetricName;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class WebServerMetrics {

    public static final WebServerMetricsBundle V1_DELETE_OBJECT_BUNDLE = new WebServerMetricsBundle(
            "ws.v1.delete.object");
    public static final WebServerMetricsBundle V1_GET_OBJECT_BUNDLE = new WebServerMetricsBundle(
            "ws.v1.get.object", true, true);
    public static final WebServerMetricsBundle V1_HEAD_OBJECT_BUNDLE = new WebServerMetricsBundle(
            "ws.v1.head.object");
    public static final WebServerMetricsBundle V1_PUT_NS_BUNDLE = new WebServerMetricsBundle(
            "ws.v1.put.ns");
    public static final WebServerMetricsBundle V1_PUT_OBJECT_BUNDLE = new WebServerMetricsBundle(
            "ws.v1.put.object", true, true);
    public static final WebServerMetricsBundle V1_SCAN_NS_BUNDLE = new WebServerMetricsBundle(
            "ws.v1.scan.ns");
    public static final WebServerMetricsBundle V1_SCAN_OBJECTS_BUNDLE = new WebServerMetricsBundle(
            "ws.v1.scan.objects");

    private static final List<WebServerMetricsBundle> V1_BUNDLES_WITH_FIRST_BYTE_LATENCIES = ImmutableList.of(
            V1_GET_OBJECT_BUNDLE,
            V1_PUT_OBJECT_BUNDLE
    );
    private static final List<WebServerMetricsBundle> V1_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES = ImmutableList.of(
            V1_DELETE_OBJECT_BUNDLE,
            V1_HEAD_OBJECT_BUNDLE,
            V1_PUT_NS_BUNDLE,
            V1_SCAN_NS_BUNDLE,
            V1_SCAN_OBJECTS_BUNDLE
    );

    private static final List<WebServerMetricsBundle> V1_GET_BUNDLES_WITH_THROUGHPUT = ImmutableList.of(
            V1_GET_OBJECT_BUNDLE
    );
    private static final List<WebServerMetricsBundle> V1_PUT_BUNDLES_WITH_THROUGHPUT = ImmutableList.of(
            V1_PUT_OBJECT_BUNDLE
    );

    static {
        registerWebServerAggregatedMetrics(
                "ws.v1",
                V1_BUNDLES_WITH_FIRST_BYTE_LATENCIES,
                V1_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES,
                V1_GET_BUNDLES_WITH_THROUGHPUT,
                V1_PUT_BUNDLES_WITH_THROUGHPUT
        );
    }

    /**
     * All exceptions thrown during {@link com.oracle.pic.casper.webserver.api.metering.MeteringHelper} will
     * increment this counter.
     */
    public static final Meter METERING_FAILURES = Metrics.meter("ws.metering.failures");
    public static final Meter METERING_SUCCESS = Metrics.meter("ws.metering.success");

    /**
     * Represents metering attempts where no compartmentId is present.
     * This can occur if the request is one where no authentication is occurring (i.e. swift)
     */
    public static final Meter METERING_UNKNOWN = Metrics.meter("ws.metering.unknowns");

    public static final WebServerMetricsBundle V2_GET_NAMESPACE_COLL_BUNDLE =
            new WebServerMetricsBundle("ws.v2.get.namespace_coll");
    public static final WebServerMetricsBundle V2_GET_NAMESPACE_METADATA_BUNDLE =
            new WebServerMetricsBundle("ws.v2.get.namespace.metadata");
    public static final WebServerMetricsBundle V2_POST_NAMESPACE_METADATA_BUNDLE =
            new WebServerMetricsBundle("ws.v2.post.namespace.metadata");

    public static final WebServerMetricsBundle V2_POST_BUCKET_COLL_BUNDLE =
            new WebServerMetricsBundle("ws.v2.post.bucket_coll");
    public static final WebServerMetricsBundle V2_GET_BUCKET_BUNDLE = new WebServerMetricsBundle("ws.v2.get.bucket");
    public static final WebServerMetricsBundle V2_HEAD_BUCKET_BUNDLE = new WebServerMetricsBundle("ws.v2.head.bucket");
    public static final WebServerMetricsBundle V2_POST_BUCKET_BUNDLE = new WebServerMetricsBundle("ws.v2.post.bucket");
    public static final WebServerMetricsBundle V2_LIST_BUCKETS_BUNDLE =
            new WebServerMetricsBundle("ws.v2.list.buckets");
    public static final WebServerMetricsBundle V2_DELETE_BUCKET_BUNDLE =
            new WebServerMetricsBundle("ws.v2.delete.bucket");

    public static final WebServerMetricsBundle V2_GET_OBJECT_BUNDLE =
            new WebServerMetricsBundle("ws.v2.get.object", true, true);
    public static final WebServerMetricsBundle V2_HEAD_OBJECT_BUNDLE =
            new WebServerMetricsBundle("ws.v2.head.object");
    public static final WebServerMetricsBundle V2_DELETE_OBJECT_BUNDLE =
            new WebServerMetricsBundle("ws.v2.delete.object");
    public static final WebServerMetricsBundle V2_PUT_OBJECT_BUNDLE =
            new WebServerMetricsBundle("ws.v2.put.object", true, true);
    public static final WebServerMetricsBundle V2_LIST_OBJECT_BUNDLE =
            new WebServerMetricsBundle("ws.v2.list.object");
    public static final WebServerMetricsBundle V2_RENAME_OBJECT_BUNDLE =
            new WebServerMetricsBundle("ws.v2.rename.object");
    public static final WebServerMetricsBundle V2_BUCKET_METER_SETTINGS_BUNDLE =
            new WebServerMetricsBundle("ws.v2.bucket.meter_settings");
    public static final WebServerMetricsBundle V2_GET_BUCKET_OPTIONS_BUNDLE =
            new WebServerMetricsBundle("ws.v2.get.bucket.options");
    public static final WebServerMetricsBundle V2_POST_BUCKET_OPTIONS_BUNDLE =
            new WebServerMetricsBundle("ws.v2.post.bucket.options");

    public static final WebServerMetricsBundle V2_POST_OBJECT_UPDATE_BUNDLE =
            new WebServerMetricsBundle("ws.v2.post.object");

    public static final WebServerMetricsBundle V2_CREATE_UPLOAD_BUNDLE =
            new WebServerMetricsBundle("ws.v2.create.upload");
    public static final WebServerMetricsBundle V2_LIST_UPLOADS_BUNDLE =
            new WebServerMetricsBundle("ws.v2.list.uploads");
    public static final WebServerMetricsBundle V2_COMMIT_UPLOAD_BUNDLE =
            new WebServerMetricsBundle("ws.v2.commit.upload");
    public static final WebServerMetricsBundle V2_ABORT_UPLOAD_BUNDLE =
            new WebServerMetricsBundle("ws.v2.abort.upload");
    public static final WebServerMetricsBundle V2_POST_UPLOAD_PART_BUNDLE =
            new WebServerMetricsBundle("ws.v2.post.upload.part", true);
    public static final WebServerMetricsBundle V2_COPY_PART_BUNDLE =
            new WebServerMetricsBundle("ws.v2.put.copy.part", true);
    public static final WebServerMetricsBundle V2_LIST_UPLOAD_PARTS_BUNDLE =
            new WebServerMetricsBundle("ws.v2.list.upload.parts");
    public static final WebServerMetricsBundle V2_RESTORE_OBJECTS_BUNDLE =
            new WebServerMetricsBundle("ws.v2.restore.objects");
    public static final WebServerMetricsBundle V2_ARCHIVE_OBJECTS_BUNDLE =
            new WebServerMetricsBundle("ws.v2.archive.objects");

    public static final WebServerMetricsBundle V2_CREATE_PAR_BUNDLE =
            new WebServerMetricsBundle("ws.v2.create.par");
    public static final WebServerMetricsBundle V2_GET_PAR_BUNDLE =
            new WebServerMetricsBundle("ws.v2.get.par");
    public static final WebServerMetricsBundle V2_DELETE_PAR_BUNDLE =
            new WebServerMetricsBundle("ws.v2.delete.par");
    public static final WebServerMetricsBundle V2_LIST_PAR_BUNDLE =
            new WebServerMetricsBundle("ws.v2.list.par");

    public static final WebServerMetricsBundle V2_CREATE_COPY_BUNDLE =
            new WebServerMetricsBundle("ws.v2.create.copy", true);
    public static final WebServerMetricsBundle V2_CREATE_BULK_RESTORE_BUNDLE =
            new WebServerMetricsBundle("ws.v2.create.bulkrestore", true);
    public static final WebServerMetricsBundle V2_GET_WORK_REQUEST_BUNDLE =
            new WebServerMetricsBundle("ws.v2.get.workrequest", true);
    public static final WebServerMetricsBundle V2_LIST_WORK_REQUESTS_BUNDLE =
            new WebServerMetricsBundle("ws.v2.list.workrequests");
    public static final WebServerMetricsBundle V2_CANCEL_WORK_REQUEST_BUNDLE =
            new WebServerMetricsBundle("ws.v2.cancel.workrequest");
    public static final WebServerMetricsBundle V2_LIST_WORK_REQUEST_LOG_BUNDLE =
            new WebServerMetricsBundle("ws.v2.get.workrequest.log");
    public static final WebServerMetricsBundle V2_LIST_WORK_REQUEST_ERROR_BUNDLE =
            new WebServerMetricsBundle("ws.v2.get.workrequest.error");

    public static final WebServerMetricsBundle V2_PUT_LIFECYCLE_BUNDLE =
            new WebServerMetricsBundle("ws.v2.put.lifecycle", true);
    public static final WebServerMetricsBundle V2_GET_LIFECYCLE_BUNDLE =
            new WebServerMetricsBundle("ws.v2.get.lifecycle", true);
    public static final WebServerMetricsBundle V2_DELETE_LIFECYCLE_BUNDLE =
            new WebServerMetricsBundle("ws.v2.delete.lifecycle");

    public static final WebServerMetricsBundle V2_CREATE_REPLICATION_POLICY_BUNDLE =
            new WebServerMetricsBundle("ws.v2.create.replicationpolicy", true);
    public static final WebServerMetricsBundle V2_GET_REPLICATION_POLICY_BUNDLE =
            new WebServerMetricsBundle("ws.v2.get.replicationpolicy", true);
    public static final WebServerMetricsBundle V2_UPDATE_REPLICATION_POLICY_BUNDLE =
            new WebServerMetricsBundle("ws.v2.update.replicationpolicy", true);
    public static final WebServerMetricsBundle V2_DELETE_REPLICATION_POLICY_BUNDLE =
            new WebServerMetricsBundle("ws.v2.delete.replicationpolicy");
    public static final WebServerMetricsBundle V2_LIST_REPLICATION_POLICIES_BUNDLE =
            new WebServerMetricsBundle("ws.v2.list.replicationpolicy");
    public static final WebServerMetricsBundle V2_LIST_REPLICATION_SOURCES_BUNDLE =
        new WebServerMetricsBundle("ws.v2.list.replicationsource");
    public static final WebServerMetricsBundle V2_REPLICATION_MAKE_BUCKET_WRITABLE_BUNDLE =
            new WebServerMetricsBundle("ws.v2.post.makebucketwritable");

    public static final WebServerMetricsBundle V2_CHECK_OBJECT_BUNDLE =
            new WebServerMetricsBundle("ws.v2.control.checkobject", true);
    public static final WebServerMetricsBundle V2_POST_BUCKET_PURGE_DELETED_TAGS_BUNDLE =
            new WebServerMetricsBundle("ws.v2.post.bucket.purgedeletedtags");

    public static final WebServerMetricsBundle V2_GET_USAGE_BUNDLE =
        new WebServerMetricsBundle("ws.v2.get.usage");

    private static final List<WebServerMetricsBundle> V2_BUNDLES_WITH_FIRST_BYTE_LATENCIES = ImmutableList.of(
            V2_GET_OBJECT_BUNDLE,
            V2_PUT_OBJECT_BUNDLE,
            V2_POST_UPLOAD_PART_BUNDLE,
            V2_COPY_PART_BUNDLE
    );
    private static final List<WebServerMetricsBundle> V2_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES = ImmutableList.of(
            V2_GET_NAMESPACE_COLL_BUNDLE,
            V2_POST_BUCKET_BUNDLE,
            V2_GET_BUCKET_BUNDLE,
            V2_HEAD_BUCKET_BUNDLE,
            V2_POST_BUCKET_BUNDLE,
            V2_LIST_BUCKETS_BUNDLE,
            V2_DELETE_BUCKET_BUNDLE,
            V2_HEAD_OBJECT_BUNDLE,
            V2_DELETE_OBJECT_BUNDLE,
            V2_LIST_OBJECT_BUNDLE,
            V2_RENAME_OBJECT_BUNDLE,
            V2_CREATE_UPLOAD_BUNDLE,
            V2_LIST_UPLOADS_BUNDLE,
            V2_COMMIT_UPLOAD_BUNDLE,
            V2_ABORT_UPLOAD_BUNDLE,
            V2_LIST_UPLOAD_PARTS_BUNDLE,
            V2_CREATE_PAR_BUNDLE,
            V2_GET_PAR_BUNDLE,
            V2_DELETE_PAR_BUNDLE,
            V2_LIST_PAR_BUNDLE,
            V2_RESTORE_OBJECTS_BUNDLE
    );

    private static final List<WebServerMetricsBundle> V2_GET_BUNDLES_WITH_THROUGHPUT = ImmutableList.of(
            V2_GET_OBJECT_BUNDLE
    );

    private static final List<WebServerMetricsBundle> V2_PUT_BUNDLES_WITH_THROUGHPUT = ImmutableList.of(
            V2_PUT_OBJECT_BUNDLE
    );

    static {
        registerWebServerAggregatedMetrics(
                "ws.v2",
                V2_BUNDLES_WITH_FIRST_BYTE_LATENCIES,
                V2_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES,
                V2_GET_BUNDLES_WITH_THROUGHPUT,
                V2_PUT_BUNDLES_WITH_THROUGHPUT
        );
    }

    public static final WebServerMetricsBundle SWIFT_HEAD_ACCOUNT_BUNDLE =
            new WebServerMetricsBundle("ws.swift.head.account");
    public static final WebServerMetricsBundle SWIFT_LIST_CONTAINERS_BUNDLE =
            new WebServerMetricsBundle("ws.swift.list.containers");
    public static final WebServerMetricsBundle SWIFT_POST_CONTAINER_BUNDLE =
            new WebServerMetricsBundle("ws.swift.update.container");
    public static final WebServerMetricsBundle SWIFT_HEAD_CONTAINER_BUNDLE =
            new WebServerMetricsBundle("ws.swift.head.container");
    public static final WebServerMetricsBundle SWIFT_PUT_CONTAINER_BUNDLE =
            new WebServerMetricsBundle("ws.swift.put.container");
    public static final WebServerMetricsBundle SWIFT_DELETE_CONTAINER_BUNDLE =
            new WebServerMetricsBundle("ws.swift.delete.container");
    public static final WebServerMetricsBundle SWIFT_LIST_OBJECTS_BUNDLE =
            new WebServerMetricsBundle("ws.swift.list.objects");
    public static final WebServerMetricsBundle SWIFT_PUT_OBJECT_BUNDLE =
            new WebServerMetricsBundle("ws.swift.put.object", true, true);
    public static final WebServerMetricsBundle SWIFT_GET_OBJECT_BUNDLE =
            new SwiftGetMetricsBundle("ws.swift.get.object");
    public static final WebServerMetricsBundle SWIFT_HEAD_OBJECT_BUNDLE =
            new WebServerMetricsBundle("ws.swift.head.object");
    public static final WebServerMetricsBundle SWIFT_DELETE_OBJECT_BUNDLE =
            new WebServerMetricsBundle("ws.swift.delete.object");
    public static final WebServerMetricsBundle SWIFT_BULK_DELETE_BUNDLE =
            new WebServerMetricsBundle("ws.swift.delete.bulk");

    private static final List<WebServerMetricsBundle> SWIFT_BUNDLES_WITH_FIRST_BYTE_LATENCIES = ImmutableList.of(
            SWIFT_GET_OBJECT_BUNDLE,
            SWIFT_PUT_OBJECT_BUNDLE
    );
    private static final List<WebServerMetricsBundle> SWIFT_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES = ImmutableList.of(
            SWIFT_LIST_CONTAINERS_BUNDLE,
            SWIFT_POST_CONTAINER_BUNDLE,
            SWIFT_HEAD_CONTAINER_BUNDLE,
            SWIFT_LIST_OBJECTS_BUNDLE,
            SWIFT_HEAD_OBJECT_BUNDLE,
            SWIFT_DELETE_OBJECT_BUNDLE,
            SWIFT_HEAD_ACCOUNT_BUNDLE,
            SWIFT_BULK_DELETE_BUNDLE
    );
    private static final List<WebServerMetricsBundle> SWIFT_GET_BUNDLES_WITH_THROUGHPUT = ImmutableList.of(
            SWIFT_GET_OBJECT_BUNDLE
    );
    private static final List<WebServerMetricsBundle> SWIFT_PUT_BUNDLES_WITH_THROUGHPUT = ImmutableList.of(
            SWIFT_PUT_OBJECT_BUNDLE
    );

    static {
        registerWebServerAggregatedMetrics(
                "ws.swift",
                SWIFT_BUNDLES_WITH_FIRST_BYTE_LATENCIES,
                SWIFT_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES,
                SWIFT_GET_BUNDLES_WITH_THROUGHPUT,
                SWIFT_PUT_BUNDLES_WITH_THROUGHPUT
        );
    }

    public static final WebServerMetricsBundle S3_GET_NAMESPACE_BUNDLE =
        new WebServerMetricsBundle("ws.s3.get.namespace");
    public static final WebServerMetricsBundle S3_DELETE_BUCKET_BUNDLE =
        new WebServerMetricsBundle("ws.s3.delete.bucket");
    public static final WebServerMetricsBundle S3_PUT_BUCKET_BUNDLE =
        new WebServerMetricsBundle("ws.s3.put.bucket");
    public static final WebServerMetricsBundle S3_GET_BUCKET_BUNDLE = new WebServerMetricsBundle("ws.s3.get.bucket");
    public static final WebServerMetricsBundle S3_HEAD_BUCKET_BUNDLE = new WebServerMetricsBundle("ws.s3.head.bucket");
    public static final WebServerMetricsBundle S3_PUT_OBJECT_BUNDLE =
        new WebServerMetricsBundle("ws.s3.put.object", true, true);
    public static final WebServerMetricsBundle S3_GET_OBJECT_BUNDLE =
        new WebServerMetricsBundle("ws.s3.get.object", true, true);
    public static final WebServerMetricsBundle S3_COPY_OBJECT_BUNDLE =
            new WebServerMetricsBundle("ws.s3.copy.object", true);
    public static final WebServerMetricsBundle S3_HEAD_OBJECT_BUNDLE =
        new WebServerMetricsBundle("ws.s3.head.object");
    public static final WebServerMetricsBundle S3_DELETE_OBJECT_BUNDLE =
        new WebServerMetricsBundle("ws.s3.delete.object");

    public static final WebServerMetricsBundle S3_CREATE_UPLOAD_BUNDLE =
        new WebServerMetricsBundle("ws.s3.create.upload");
    public static final WebServerMetricsBundle S3_LIST_UPLOADS_BUNDLE =
        new WebServerMetricsBundle("ws.s3.list.uploads");
    public static final WebServerMetricsBundle S3_COMMIT_UPLOAD_BUNDLE =
        new WebServerMetricsBundle("ws.s3.commit.upload");
    public static final WebServerMetricsBundle S3_ABORT_UPLOAD_BUNDLE =
        new WebServerMetricsBundle("ws.s3.abort.upload");
    public static final WebServerMetricsBundle S3_PUT_UPLOAD_PART_BUNDLE =
        new WebServerMetricsBundle("ws.s3.post.upload.part", true);
    public static final WebServerMetricsBundle S3_COPY_PART_BUNDLE =
        new WebServerMetricsBundle("ws.s3.put.copy.part", true);
    public static final WebServerMetricsBundle S3_LIST_UPLOAD_PARTS_BUNDLE =
        new WebServerMetricsBundle("ws.s3.list.upload.parts");
    public static final WebServerMetricsBundle S3_RESTORE_OBJECT_BUNDLE =
        new WebServerMetricsBundle("ws.s3.restore.object");
    public static final WebServerMetricsBundle S3_BULK_DELETE_BUNDLE =
        new WebServerMetricsBundle("ws.s3.bulk.delete");

    private static final List<WebServerMetricsBundle> S3_BUNDLES_WITH_FIRST_BYTE_LATENCIES = ImmutableList.of(
            S3_GET_OBJECT_BUNDLE,
            S3_PUT_OBJECT_BUNDLE,
            S3_PUT_UPLOAD_PART_BUNDLE,
            S3_COPY_PART_BUNDLE
    );
    private static final List<WebServerMetricsBundle> S3_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES = ImmutableList.of(
            S3_GET_NAMESPACE_BUNDLE,
            S3_DELETE_BUCKET_BUNDLE,
            S3_PUT_BUCKET_BUNDLE,
            S3_GET_BUCKET_BUNDLE,
            S3_HEAD_BUCKET_BUNDLE,
            S3_HEAD_OBJECT_BUNDLE,
            S3_DELETE_OBJECT_BUNDLE,
            S3_CREATE_UPLOAD_BUNDLE,
            S3_LIST_UPLOADS_BUNDLE,
            S3_COMMIT_UPLOAD_BUNDLE,
            S3_ABORT_UPLOAD_BUNDLE,
            S3_LIST_UPLOAD_PARTS_BUNDLE,
            S3_RESTORE_OBJECT_BUNDLE
    );

    private static final List<WebServerMetricsBundle> S3_GET_BUNDLES_WITH_THROUGHPUT = ImmutableList.of(
            S3_GET_OBJECT_BUNDLE
    );

    private static final List<WebServerMetricsBundle> S3_PUT_BUNDLES_WITH_THROUGHPUT = ImmutableList.of(
            S3_PUT_OBJECT_BUNDLE
    );

    static {
        registerWebServerAggregatedMetrics(
                "ws.s3",
                S3_BUNDLES_WITH_FIRST_BYTE_LATENCIES,
                S3_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES,
                S3_GET_BUNDLES_WITH_THROUGHPUT,
                S3_PUT_BUNDLES_WITH_THROUGHPUT
        );
    }

    public static final WebServerMetricsBundle ARCHIVE_GET_ENCRYPTED_OBJECT_CHUNK_BUNDLE =
            new WebServerMetricsBundle("ws.archive.get.encryptedobjectchunk", true);
    public static final WebServerMetricsBundle ARCHIVE_PUT_ENCRYPTED_OBJECT_CHUNK_BUNDLE =
            new WebServerMetricsBundle("ws.archive.put.encryptedobjectchunk", true);

    private static final List<WebServerMetricsBundle> ARCHIVE_BUNDLES_WITH_FIRST_BYTE_LATENCIES = ImmutableList.of(
            ARCHIVE_GET_ENCRYPTED_OBJECT_CHUNK_BUNDLE,
            ARCHIVE_PUT_ENCRYPTED_OBJECT_CHUNK_BUNDLE
    );
    // empty for now, but create it anyway as a defensive coding measure.  This way it can't get missed below.
    private static final List<WebServerMetricsBundle> ARCHIVE_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES = ImmutableList.of();

    // empty for now
    private static final List<WebServerMetricsBundle> ARCHIVE_GET_BUNDLES_WITH_THROUGHPUT = ImmutableList.of();
    private static final List<WebServerMetricsBundle> ARCHIVE_PUT_BUNDLES_WITH_THROUGHPUT = ImmutableList.of();

    static {
        registerWebServerAggregatedMetrics(
                "ws.archive",
                ARCHIVE_BUNDLES_WITH_FIRST_BYTE_LATENCIES,
                ARCHIVE_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES,
                ARCHIVE_GET_BUNDLES_WITH_THROUGHPUT,
                ARCHIVE_PUT_BUNDLES_WITH_THROUGHPUT
        );
    }

    /**
     * Register the complete-host availability metric, drawing on all WebServerMetricBundles.
     *
     * If a new WebServerMetricBundle is created, it should be appended to the list below.  If you find one that isn't,
     * send a CR to add it.
     */
    static {
        List<WebServerMetricsBundle> allFirstByteLatencyBundles = Stream.of(
                V1_BUNDLES_WITH_FIRST_BYTE_LATENCIES,
                V2_BUNDLES_WITH_FIRST_BYTE_LATENCIES,
                SWIFT_BUNDLES_WITH_FIRST_BYTE_LATENCIES,
                S3_BUNDLES_WITH_FIRST_BYTE_LATENCIES,
                ARCHIVE_BUNDLES_WITH_FIRST_BYTE_LATENCIES
        ).flatMap(List::stream).collect(Collectors.toList());
        List<WebServerMetricsBundle> allOverallLatencyBundles = Stream.of(
                V1_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES,
                V2_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES,
                SWIFT_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES,
                S3_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES,
                ARCHIVE_BUNDLES_WITHOUT_FIRST_BYTE_LATENCIES
        ).flatMap(List::stream).collect(Collectors.toList());
        List<WebServerMetricsBundle> allGetThroughputBundles = Stream.of(
                V1_GET_BUNDLES_WITH_THROUGHPUT,
                V2_GET_BUNDLES_WITH_THROUGHPUT,
                S3_GET_BUNDLES_WITH_THROUGHPUT,
                SWIFT_GET_BUNDLES_WITH_THROUGHPUT,
                ARCHIVE_GET_BUNDLES_WITH_THROUGHPUT
        ).flatMap(List::stream).collect(Collectors.toList());
        List<WebServerMetricsBundle> allPutThroughputBundles = Stream.of(
                V1_PUT_BUNDLES_WITH_THROUGHPUT,
                V2_PUT_BUNDLES_WITH_THROUGHPUT,
                S3_PUT_BUNDLES_WITH_THROUGHPUT,
                SWIFT_PUT_BUNDLES_WITH_THROUGHPUT,
                ARCHIVE_PUT_BUNDLES_WITH_THROUGHPUT
        ).flatMap(List::stream).collect(Collectors.toList());

        registerWebServerAggregatedMetrics(
                "ws.all",
                allFirstByteLatencyBundles,
                allOverallLatencyBundles,
                allGetThroughputBundles,
                allPutThroughputBundles
        );
    }

    public static final Meter SWIFT_XSS_POTENTIAL_ATTEMPTS = Metrics.meter("ws.swift.xss.potential.attempts");

    public static final Counter AUDIT_SUCCESS = Metrics.counter("ws.audit.success");
    public static final Counter AUDIT_ERROR = Metrics.counter("ws.audit.error");
    public static final Counter ALL_AUDIT = Metrics.aggregatedCounter("ws.audit.all",
        ImmutableList.of(AUDIT_SUCCESS, AUDIT_ERROR));
    public static final EnumMap<ObjectEvent.EventType, Counter> OBJECT_EVENTS_SUCCESS =
        new EnumMap<>(ObjectEvent.EventType.class);
    public static final EnumMap<ObjectEvent.EventType, Counter> OBJECT_EVENTS_ERROR =
        new EnumMap<>(ObjectEvent.EventType.class);
    static {
        for (ObjectEvent.EventType type : ObjectEvent.EventType.values()) {
            OBJECT_EVENTS_SUCCESS.put(type, Metrics.counter(MetricName.of("ws.events",
                ImmutableMap.of("type", type.getName().toLowerCase(), "result", "success"))));
            OBJECT_EVENTS_ERROR.put(type, Metrics.counter(MetricName.of("ws.events",
                ImmutableMap.of("type", type.getName().toLowerCase(), "result", "error"))));
        }
    }

    /**
     * Metrics for eventing
     *
     * TODO: remove after we completely migrate off event V1
     */
    public static final Counter EVENTS_FAILED_TO_ADD = Metrics.counter("ws.eventing.add.failed");
    public static final LongGauge OLDEST_EVENT_AGE_SEC = Metrics.gauge("ws.eventing.oldest");
    public static final MetricsBundle EVENT_SERVICE_CALL = new MetricsBundle("ws.eventing");
    public static final LongGauge EVENTS_NUM_BROKEN_FILE = Metrics.gauge("ws.eventing.broken.file.count");
    public static final Counter EVENTS_UNCAUGHT_ERROR = Metrics.counter("ws.eventing.uncaught.error");
    public static final Counter EVENTS_SENT_SUCCESS = Metrics.counter("ws.eventing.sent.success");
    public static final Counter EVENTS_SENT = Metrics.counter("ws.eventing.sent.");

    /**
     * Metrics for traffic control
     */

    public static final LongGauge TC_BANDWIDTH = Metrics.gauge("ws.traffic.bandwidth");
    public static final LongGauge TC_CONCURRENCY = Metrics.gauge("ws.traffic.concurrency");

    public static final Counter TC_GET_TOTAL = Metrics.counter("ws.traffic.get.total");
    public static final Counter TC_GET_ACCEPT = Metrics.counter("ws.traffic.get.accept");
    public static final Counter TC_GET_REJECT = Metrics.counter("ws.traffic.get.reject");
    public static final PercentageHistogram TC_GET_REJECT_HISTOGRAM = Metrics.percentageHistogram(
            "ws.traffic.get.reject.percentage",
            TC_GET_REJECT,
            TC_GET_TOTAL
    );

    public static final Counter TC_PUT_TOTAL = Metrics.counter("ws.traffic.put.total");
    public static final Counter TC_PUT_ACCEPT = Metrics.counter("ws.traffic.put.accept");
    public static final Counter TC_PUT_REJECT = Metrics.counter("ws.traffic.put.reject");
    public static final PercentageHistogram TC_PUT_REJECT_HISTOGRAM = Metrics.percentageHistogram(
            "ws.traffic.put.reject.percentage",
            TC_PUT_REJECT,
            TC_PUT_TOTAL
    );

    public static final Counter TC_TT_GET_ACCEPT = Metrics.counter("ws.traffic.tenant.get.accept");
    public static final Counter TC_TT_GET_REJECT = Metrics.counter("ws.traffic.tenant.get.reject");
    public static final PercentageHistogram TC_TT_GET_REJECT_HISTOGRAM = Metrics.percentageHistogram(
            "ws.traffic.tenant.get.reject.percentage",
            TC_GET_REJECT,
            TC_GET_TOTAL
    );

    public static final Counter TC_TT_PUT_ACCEPT = Metrics.counter("ws.traffic.tenant.put.accept");
    public static final Counter TC_TT_PUT_REJECT = Metrics.counter("ws.traffic.tenant.put.reject");
    public static final PercentageHistogram TC_TT_PUT_REJECT_HISTOGRAM = Metrics.percentageHistogram(
            "ws.traffic.tenant.put.reject.percentage",
            TC_PUT_REJECT,
            TC_PUT_TOTAL
    );

    public static final Counter TC_UPDATE_ERROR = Metrics.counter("ws.traffic.update.error");
    public static final Counter SBH_UPDATE_ERROR = Metrics.counter("ws.syncbodyhandler.update.error");

    /**
     *  Represents success and failure counts of the metric being added to the common queue
     */
    public static final Meter MONITORING_METRIC_FAILURE = Metrics.meter("ws.monitoring.failures");
    public static final Meter MONITORING_METRIC_SUCCESS = Metrics.meter("ws.monitoring.success");

    /**
     * Metrics for Embargo rules
     */

    // A count of all requests that were 503d due to matching with an Embargo Rule
    @Deprecated
    public static final Counter EMBARGOED_REQUESTS = Metrics.counter("ws.embargo.match");

    // A mapping of every rule that matches to the count of requests that matched that rule
    // These counts are different to EMBARGOED_REQUESTS because a single request can match multiple rules
    // Note: this is a possible "memory leak", in that there's no compaction going on for deleted rules
    // But, this map gets blown away each server bounce, so we should be ok (NAT!)
    @Deprecated
    public static final Map<Long, Counter> EMBARGO_RULE_MATCHES = Maps.newConcurrentMap();

    /*
     * Metrics for Embargo V3 rules
     */

    /**
     * Metrics for enabled EmbargoV3 rules.
     * 1) {@link EmbargoV3MetricsBundle#evaluatedRule()} is called once for every operation.
     * 2) {@link EmbargoV3MetricsBundle#ruleMatched()} is called once if any rule matches.
     * 2) {@link EmbargoV3MetricsBundle#operationBlocked()} is called once if any rule blocks the operation.
     */
    public static final EmbargoV3MetricsBundle EMBARGOV3_ENABLED = new EmbargoV3MetricsBundle("embargov3.enabled");

    /**
     * A mapping of rule ID to rule metrics for enabled rules. Unfortunately
     * there is no way to de-register a metrics after creating them so disabled
     * rules will continue to take up space in this map until server restart.
     *
     * One request can match multiple rules, so the sum of these values
     * can be greater than {@link #EMBARGOV3_ENABLED}.
     */
    public static final Map<Long, EmbargoV3MetricsBundle> EMBARGOV3_ENABLED_RULES = new ConcurrentHashMap<>();

    /**
     * Metrics for dry-run EmbargoV3 rules.
     * 1) {@link EmbargoV3MetricsBundle#evaluatedRule()} is called once for every operation.
     * 2) {@link EmbargoV3MetricsBundle#ruleMatched()} is called once if any rule matches.
     * 2) {@link EmbargoV3MetricsBundle#operationBlocked()} is called once if any rule would block the operation.
     */
    public static final EmbargoV3MetricsBundle EMBARGOV3_DRYRUN = new EmbargoV3MetricsBundle("embargov3.dryrun");

    /**
     * A mapping of rule ID to rule metrics for dry-run rules. Unfortunately
     * there is no way to de-register a metrics after creating them so disabled
     * rules will continue to take up space in this map until server restart.
     *
     * One request can match multiple rules, so the sum of these values
     * can be greater than {@link #EMBARGOV3_DRYRUN}.
     */
    public static final Map<Long, EmbargoV3MetricsBundle> EMBARGOV3_DRYRUN_RULES = new ConcurrentHashMap<>();

    /*
     * Copy metrics
     */

    /**
     * Metrics for copy object request per target region in terms of bytes.
     */
    public static final Map<ConfigRegion, Meter> COPY_REQUEST_BYTES = new EnumMap<>(ConfigRegion.class);
    static {
        for (ConfigRegion region : ConfigRegion.values()) {
            //region names have dashes but those may not go well with metric names, so we take them out
            final String regionWithoutDash = region.getFullName().replaceAll("-", "").toLowerCase();
            COPY_REQUEST_BYTES.put(region, Metrics.meter("ws.copy." + regionWithoutDash + ".request.bytes"));
        }

        //aggregate cross region throughput
        final ConfigRegion localRegion = ConfigRegion.fromSystemProperty();
        Metrics.aggregatedMeter("ws.copy.crossregion.request.bytes", COPY_REQUEST_BYTES.entrySet().stream()
            .filter(entry -> entry.getKey() != localRegion)
            .map(entry -> entry.getValue())
            .collect(Collectors.toList()));

        //aggregate all throughput
        Metrics.aggregatedMeter("ws.copy.all.request.bytes", new ArrayList<>(COPY_REQUEST_BYTES.values()));
    }

    public static final Counter OLD_SERVICE_NAME_REQUESTS = Metrics.counter("ws.auth.oldservicename");

    private WebServerMetrics() {
    }

    /**
     * We need the constants declared in this class to be initialized before we start our telemetry. We can't rely
     * on Java to load the class (as it may not load it before we initialize our telemetry code),
     * so we call this method to force the JVM to load this class (and initialize the constants).
     */
    public static void init() {
        LogMetrics.init();
    }

    /**
     * Registers service-wide metrics aggregated across all of the API calls that have their metrics bundles passed in.
     *
     * The metric objects returned by the calls to {@link Metrics} are unused because aggregated metrics do not need
     * to be updated themselves, they're transparently computed from their member metrics.
     *
     * @param apiPrefix The name of the API, formatted as the prefix that all the aggregated metrics should have.
     * @param bundlesWithFirstByteLatencies PUT and GET object should measure first byte latency, so pass bundles here.
     * @param bundlesWithOnlyOverallLatencies All other metric bundles measure overall latencies.
     * @param getBundlesWithThroughput All get bundles which measure throughput
     * @param putBundlesWithThroughput All put bundles which measure throughput
     */
    private static void registerWebServerAggregatedMetrics(
            String apiPrefix,
            List<WebServerMetricsBundle> bundlesWithFirstByteLatencies,
            List<WebServerMetricsBundle> bundlesWithOnlyOverallLatencies,
            List<WebServerMetricsBundle> getBundlesWithThroughput,
            List<WebServerMetricsBundle> putBundlesWithThroughput) {

        List<WebServerMetricsBundle> allBundles = ImmutableList.copyOf(
                Iterables.concat(bundlesWithFirstByteLatencies, bundlesWithOnlyOverallLatencies));

        List<WebServerMetricsBundle> overallThroughputBundles = ImmutableList.copyOf(
                Iterables.concat(getBundlesWithThroughput, putBundlesWithThroughput));

        List<Histogram> firstByteLatencyHistograms = ImmutableList.copyOf(
                bundlesWithFirstByteLatencies.stream()
                        .map(WebServerMetricsBundle::getFirstByteLatency)
                        .collect(Collectors.toList()));
        List<Histogram> overallLatencyHistograms = ImmutableList.copyOf(
                bundlesWithOnlyOverallLatencies.stream()
                        .map(WebServerMetricsBundle::getOverallLatency)
                        .collect(Collectors.toList()));
        List<Histogram> latencyHistograms = ImmutableList.copyOf(
                Iterables.concat(firstByteLatencyHistograms, overallLatencyHistograms));
        List<Histogram> getThroughputHistograms = ImmutableList.copyOf(
                getBundlesWithThroughput.stream()
                        .map(WebServerMetricsBundle::getThroughput)
                        .collect(Collectors.toList()));
        List<Histogram> putThroughputHistograms = ImmutableList.copyOf(
                putBundlesWithThroughput.stream()
                        .map(WebServerMetricsBundle::getThroughput)
                        .collect(Collectors.toList()));
        List<Histogram> overallThroughputHistograms = ImmutableList.copyOf(
                overallThroughputBundles.stream()
                        .map(WebServerMetricsBundle::getThroughput)
                        .collect(Collectors.toList()));
        List<Histogram> inverseThroughputHistograms = ImmutableList.copyOf(
                overallThroughputBundles.stream()
                        .map(WebServerMetricsBundle::getInverseThroughput)
                        .collect(Collectors.toList()));

        List<Counter> requestCounters = allBundles.stream()
                .map(WebServerMetricsBundle::getRequests)
                .collect(Collectors.toList());
        List<Counter> successCounters = allBundles.stream()
                .map(WebServerMetricsBundle::getSuccesses)
                .collect(Collectors.toList());
        List<Counter> clientErrorCounters = allBundles.stream()
                .map(WebServerMetricsBundle::getClientErrors)
                .collect(Collectors.toList());
        List<Counter> serverErrorCounters = allBundles.stream()
                .map(WebServerMetricsBundle::getServerErrors)
                .collect(Collectors.toList());
        List<Counter> rateLimitCounters = allBundles.stream()
                .map(WebServerMetricsBundle::getRateLimitCounter)
                .collect(Collectors.toList());

        //Updating aggregated availability counter value to add 503 count to avoid embargoed customer requests from
        //affecting the availability metrics
        List<Counter> availabilityCounters = Stream.concat(Stream.concat(successCounters.stream(),
                clientErrorCounters.stream()),
                rateLimitCounters.stream())
                .collect(Collectors.toList());

        final Counter aggregatedRequestCounter = Metrics.aggregatedCounter(
                apiPrefix + MetricsBundle.REQUESTS_SUFFIX, requestCounters);
        final Counter aggregatedSuccessesCounter = Metrics.aggregatedCounter(
                apiPrefix + MetricsBundle.SUCCESSES_SUFFIX, successCounters);
        final Counter aggregatedClientErrorCounter = Metrics.aggregatedCounter(
                apiPrefix + MetricsBundle.CLIENT_ERRORS_SUFFIX, clientErrorCounters);
        final Counter aggregatedServerErrorCounter = Metrics.aggregatedCounter(
                apiPrefix + MetricsBundle.SERVER_ERRORS_SUFFIX, serverErrorCounters);
        final Counter aggregatedAvailabilityCounter = Metrics.aggregatedCounter(
                apiPrefix + MetricsBundle.AVAILABILITY_SUFFIX, availabilityCounters);
        final Counter aggregatedTooBusyCounter =
            Metrics.aggregatedCounter(apiPrefix + WebServerMetricsBundle.SERVER_TOO_BUSY_SUFFIX, rateLimitCounters);

        // TODO(jfriedly):  MetricsBundle should expose a builder so that we don't have to reimplement this by hand.
        // The logic to create percentage histograms from request and error counters is common, regardless of whether
        // or not they're aggregated.
        // Also, WebServerMetricsBundle should probably either be done away with entirely, or it should encapsulate,
        // rather than extend, MetricsBundle.
        Metrics.aggregatedHistogram(apiPrefix + ".latency", latencyHistograms);
        Metrics.percentageHistogram(apiPrefix + MetricsBundle.SUCCESSES_SUFFIX,
                aggregatedSuccessesCounter, aggregatedRequestCounter);
        Metrics.percentageHistogram(apiPrefix + MetricsBundle.CLIENT_ERRORS_SUFFIX,
                aggregatedClientErrorCounter, aggregatedRequestCounter);
        Metrics.percentageHistogram(apiPrefix + MetricsBundle.SERVER_ERRORS_SUFFIX,
                aggregatedServerErrorCounter, aggregatedRequestCounter);
        Metrics.percentageHistogram(apiPrefix + MetricsBundle.AVAILABILITY_SUFFIX,
                aggregatedAvailabilityCounter, aggregatedRequestCounter);
        Metrics.percentageHistogram(apiPrefix + WebServerMetricsBundle.SERVER_TOO_BUSY_SUFFIX,
            aggregatedTooBusyCounter, aggregatedRequestCounter);
        Metrics.aggregatedHistogram(apiPrefix + ".get.throughput", getThroughputHistograms);
        Metrics.aggregatedHistogram(apiPrefix + ".put.throughput", putThroughputHistograms);
        Metrics.aggregatedHistogram(apiPrefix + ".overall.throughput", overallThroughputHistograms);
        Metrics.aggregatedHistogram(apiPrefix + ".overall.inverse.throughput", inverseThroughputHistograms);
    }
}
