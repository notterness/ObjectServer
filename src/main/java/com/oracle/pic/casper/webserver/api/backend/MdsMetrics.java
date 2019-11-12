package com.oracle.pic.casper.webserver.api.backend;

import com.oracle.pic.casper.common.metrics.MetricsBundle;
import com.oracle.pic.casper.mds.common.client.MdsRequestId;
import com.oracle.pic.casper.mds.common.exceptions.MdsException;
import com.oracle.pic.casper.mds.common.exceptions.MdsExceptionClassifier;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Metrics for MDS services.
 */
public final class MdsMetrics {

    public static final MetricsBundle OBJECT_MDS_BUNDLE = new MetricsBundle("mds.object", false);

    public static final MetricsBundle OBJECT_MDS_RENAME_OBJECT = new MetricsBundle("mds.object.rename.object", false);
    public static final MetricsBundle OBJECT_MDS_UPDATE_OBJECT_METADATA =
            new MetricsBundle("mds.object.update.objectmetadata", false);
    public static final MetricsBundle OBJECT_MDS_RESTORE_OBJECT = new MetricsBundle("mds.object.restore.object", false);
    public static final MetricsBundle OBJECT_MDS_ARCHIVE_OBJECT = new MetricsBundle("mds.object.archive.object", false);
    public static final MetricsBundle OBJECT_MDS_LIST_OBJECTS = new MetricsBundle("mds.object.list.objects", false);
    public static final MetricsBundle OBJECT_MDS_LIST_OBJECTS_NAME_ONLY =
            new MetricsBundle("mds.object.list.objects.nameonly", false);
    public static final MetricsBundle OBJECT_MDS_LIST_OBJECTS_S3 =
            new MetricsBundle("mds.object.list.objects.s3", false);
    public static final MetricsBundle OBJECT_MDS_LIST_OBJECTS_BASIC =
            new MetricsBundle("mds.object.list.objects.basic", false);
    public static final MetricsBundle OBJECT_MDS_LIST_V1_OBJECTS =
            new MetricsBundle("mds.object.list.v1.objects", false);
    public static final MetricsBundle OBJECT_MDS_BEGIN_UPLOAD = new MetricsBundle("mds.object.begin.upload", false);
    public static final MetricsBundle OBJECT_MDS_FINISH_UPLOAD = new MetricsBundle("mds.object.finish.upload", false);
    public static final MetricsBundle OBJECT_MDS_LIST_UPLOADED_PARTS =
            new MetricsBundle("mds.object.list.uploadedparts", false);
    public static final MetricsBundle OBJECT_MDS_LIST_UPLOADS =
        new MetricsBundle("mds.object.list.uploads", false);
    public static final MetricsBundle OBJECT_MDS_ABORT_UPLOAD = new MetricsBundle("mds.object.abort.upload", false);
    public static final MetricsBundle OBJECT_MDS_SET_OBJECT_ENCRYPTION_KEY =
            new MetricsBundle("mds.object.set.objectencryptionkey", false);
    public static final MetricsBundle OBJECT_MDS_GET_OBJECT = new MetricsBundle("mds.object.get.object", false);
    public static final MetricsBundle OBJECT_MDS_HEAD_OBJECT = new MetricsBundle("mds.object.head.object", false);
    public static final MetricsBundle OBJECT_MDS_DELETE_OBJECT = new MetricsBundle("mds.object.delete.object", false);
    public static final MetricsBundle OBJECT_MDS_BEGIN_PUT = new MetricsBundle("mds.object.begin.put", false);
    public static final MetricsBundle OBJECT_MDS_FINISH_PUT = new MetricsBundle("mds.object.finish.put", false);
    public static final MetricsBundle OBJECT_MDS_BEGIN_CHUNK = new MetricsBundle("mds.object.begin.chunk", false);
    public static final MetricsBundle OBJECT_MDS_FINISH_CHUNK =
            new MetricsBundle("mds.object.finish.chunk", false);
    public static final MetricsBundle OBJECT_MDS_BEGIN_PART_CHUNK =
            new MetricsBundle("mds.object.begin.partchunk", false);
    public static final MetricsBundle OBJECT_MDS_FINISH_PART_CHUNK =
            new MetricsBundle("mds.object.finish.partchunk", false);
    public static final MetricsBundle OBJECT_MDS_ABORT_PUT =
            new MetricsBundle("mds.object.abortput", false);
    public static final MetricsBundle OBJECT_MDS_BEGIN_PART =
            new MetricsBundle("mds.object.begin.part", false);
    public static final MetricsBundle OBJECT_MDS_FINISH_PART =
            new MetricsBundle("mds.object.finish.part", false);
    public static final MetricsBundle OBJECT_MDS_ABORT_PENDING_PART =
            new MetricsBundle("mds.object.abort.pendingpart", false);
    public static final MetricsBundle OBJECT_MDS_GET_STAT_FOR_BUCKET =
            new MetricsBundle("mds.object.get.statforbucket", false);

    public static final MetricsBundle TENANT_MDS_BUNDLE = new MetricsBundle("mds.tenant", false);

    public static final MetricsBundle TENANT_UPDATE_BUCKET_SHARD_ID =
            new MetricsBundle("mds.tenant.update.bucketshardid", false);
    public static final MetricsBundle TENANT_GET_BUCKET = new MetricsBundle("mds.tenant.get.bucket", false);
    public static final MetricsBundle TENANT_CREATE_BUCKET = new MetricsBundle("mds.tenant.create.bucket", false);
    public static final MetricsBundle TENANT_DELETE_BUCKET = new MetricsBundle("mds.tenant.delete.bucket", false);
    public static final MetricsBundle TENANT_UPDATE_BUCKET = new MetricsBundle("mds.tenant.update.bucket", false);
    public static final MetricsBundle TENANT_LIST_BUCKETS = new MetricsBundle("mds.tenant.list.buckets", false);
    public static final MetricsBundle TENANT_ADD_BUCKET_METER_FLAG_SET =
            new MetricsBundle("mds.tenant.add.bucketmeterflagset", false);
    public static final MetricsBundle TENANT_UPDATE_BUCKET_OPTIONS =
            new MetricsBundle("mds.tenant.update.bucketoptions", false);
    public static final MetricsBundle TENANT_SET_BUCKET_ENCRYPTION_KEY =
            new MetricsBundle("mds.tenant.set.bucketencryptionkey", false);
    public static final MetricsBundle TENANT_SET_BUCKET_UNCACHEABLE =
            new MetricsBundle("mds.tenant.set.bucketuncacheable", false);
    public static final MetricsBundle TENANT_GET_NAMESPACE = new MetricsBundle("mds.tenant.get.namespace", false);
    public static final MetricsBundle TENANT_CREATE_NAMESPACE = new MetricsBundle("mds.tenant.create.namespace", false);
    public static final MetricsBundle TENANT_LIST_NAMESPACES = new MetricsBundle("mds.tenant.list.namespaces", false);
    public static final MetricsBundle TENANT_UPDATE_NAMESPACE = new MetricsBundle("mds.tenant.update.namespace", false);

    public static final MetricsBundle OPERATOR_MDS_BUNDLE = new MetricsBundle("mds.operator", false);

    public static final MetricsBundle OPERATOR_LIST_EMBARGOS_V1 =
        new MetricsBundle("mds.operator.list.embargov1", false);
    public static final MetricsBundle OPERATOR_LIST_DRY_RUN_EMBARGOS_V3 =
        new MetricsBundle("mds.operator.list.embargov3dryrun", false);
    public static final MetricsBundle OPERATOR_LIST_ENABLED_EMBARGOS_V3 =
        new MetricsBundle("mds.operator.list.embargov3enabled", false);
    public static final MetricsBundle OPERATOR_LIST_CONFIG = new MetricsBundle("mds.operator.list.config.keys", false);
    public static final MetricsBundle OPERATOR_GET_CONFIG = new MetricsBundle("mds.operator.get.config.key", false);
    public static final MetricsBundle OPERATOR_GET_SECRET = new MetricsBundle("mds.operator.get.secret.version", false);
    public static final MetricsBundle OPERATOR_GET_USAGE = new MetricsBundle("mds.operator.get.usage", false);

    public static final MetricsBundle WORK_REQUEST_MDS_BUNDLE = new MetricsBundle("mds.workrequest", false);

    public static final MetricsBundle WORK_REQUEST_CREATE = new MetricsBundle("mds.workrequest.create", false);
    public static final MetricsBundle WORK_REQUEST_GET = new MetricsBundle("mds.workrequest.get", false);
    public static final MetricsBundle WORK_REQUEST_LIST = new MetricsBundle("mds.workrequest.list", false);
    public static final MetricsBundle WORK_REQUEST_CANCEL = new MetricsBundle("mds.workrequest.cancel", false);

    public static final MetricsBundle REPLICATION_POLICY_CREATE =
            new MetricsBundle("mds.replicationpolicy.create", true);
    public static final MetricsBundle REPLICATION_POLICY_GET = new MetricsBundle("mds.replicationpolicy.get", true);
    public static final MetricsBundle REPLICATION_POLICY_UPDATE =
            new MetricsBundle("mds.replicationpolicy.update", false);
    public static final MetricsBundle REPLICATION_POLICY_DELETE =
            new MetricsBundle("mds.replicationpolicy.delete", false);
    public static final MetricsBundle REPLICATION_POLICY_LIST = new MetricsBundle("mds.replicationpolicy.list", false);

    private static final RetryPolicy RETRY_POLICY =
        new RetryPolicy().withDelay(50, TimeUnit.MILLISECONDS).withMaxRetries(2).retryOn(e -> {
            if (e instanceof StatusRuntimeException) {
                if (((StatusRuntimeException) e).getStatus().getCode().equals(Code.UNAVAILABLE)) {
                    return ExceptionUtils.getRootCause(e) instanceof java.net.ConnectException;
                }
            }
            return false;
        });

    private static final RetryPolicy IDEMPOTENT_RETRY_POLICY =
        new RetryPolicy().withDelay(50, TimeUnit.MILLISECONDS).withMaxRetries(2).retryOn(e -> {
            if (e instanceof StatusRuntimeException) {
                return ((StatusRuntimeException) e).getStatus().getCode().equals(Code.UNAVAILABLE);
            }
            return false;
        });


    private static final Logger LOG = LoggerFactory.getLogger(MdsMetrics.class);

    private MdsMetrics() {
    }

    /**
     * Helper function to emit metrics for MDS calls. Tracks requests and error rates for both metric bundles.
     * Latency is tracked at the API level.
     *
     * @param <V>
     * @param aggregateBundle
     * @param apiBundle
     * @param isIdempotent - If true, failsafe will retry on Status.UNAVAILABLE. Otherwise only retry on
     *                     an underlying connection refused exception
     * @param callable
     */
    public static <V> V executeWithMetrics(
            MetricsBundle aggregateBundle, MetricsBundle apiBundle, boolean isIdempotent, Callable<V> callable) {
        if (isIdempotent) {
            return Failsafe.with(IDEMPOTENT_RETRY_POLICY)
                    .get(() -> executeWithMetrics(aggregateBundle, apiBundle, callable));
        } else {
            return Failsafe.with(RETRY_POLICY).get(() -> executeWithMetrics(aggregateBundle, apiBundle, callable));
        }
    }

    private static <V> V executeWithMetrics(
        MetricsBundle aggregateBundle,
        MetricsBundle apiBundle,
        Callable<V> callable) {
        final long startTime = System.currentTimeMillis();
        try {
            V result = callable.call();
            aggregateBundle.getSuccesses().inc();
            apiBundle.getSuccesses().inc();
            return result;
        } catch (StatusRuntimeException e) {
            MdsException mdsEx = MdsExceptionClassifier.fromStatusRuntimeException(e);
            if (mdsEx.isServerError()) {
                LOG.error(
                    "Unexpected exception when executing MDS call. requestId: {}",
                    e,
                    e.getTrailers().get(MdsRequestId.REQUEST_ID_KEY));
                aggregateBundle.getServerErrors().inc();
                apiBundle.getServerErrors().inc();
            } else {
                aggregateBundle.getClientErrors().inc();
                apiBundle.getClientErrors().inc();
            }
            throw e;
        } catch (Exception e) {
            LOG.error("Unexpected exception when executing MDS call", e);
            aggregateBundle.getServerErrors().inc();
            apiBundle.getServerErrors().inc();
            throw new RuntimeException(e);
        } finally {
            aggregateBundle.getRequests().inc();
            apiBundle.getRequests().inc();
            final long endTime = System.currentTimeMillis();
            final long elapsedTime = endTime - startTime;
            apiBundle.getOverallLatency().update(elapsedTime);
        }
    }
}
