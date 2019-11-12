package com.oracle.pic.casper.webserver.api.usage;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.TimeLimiter;
import com.oracle.pic.accounts.model.CompartmentQuotas;
import com.oracle.pic.accounts.model.ServiceLimitEntry;
import com.oracle.pic.casper.common.exceptions.StorageLimitExceededException;
import com.oracle.pic.casper.common.exceptions.StorageQuotaExceededException;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.webserver.api.auth.CompartmentTreeRetriever;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.auth.limits.Limits;
import com.oracle.pic.limits.NonKievCompartmentQuotaEvaluator;
import com.oracle.pic.limits.QuotaEvaluationRequest;
import com.oracle.pic.limits.ResourceUsageRetriever;
import com.oracle.pic.limits.entities.CreateResourcePayload;
import com.oracle.pic.limits.entities.EvaluationMode;
import com.oracle.pic.limits.entities.EvaluatorConfig;
import com.oracle.pic.limits.entities.MoveResourcePayload;
import com.oracle.pic.limits.entities.QuotaEvaluationResult;
import com.oracle.pic.limits.entities.Violation;
import com.oracle.pic.limits.entities.exceptions.CompartmentQuotaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class QuotaEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(QuotaEvaluator.class);

    private static final String LIMITS_SERVICE_FAMILY = "object-storage";
    private static final String STORAGE_QUOTA_NAME = "storage-bytes";

    /**
     * Quota evaluator
     */
    private final NonKievCompartmentQuotaEvaluator evaluator;

    /**
     * Compartment usage cache
     */
    private final UsageCache usageCache;

    /**
     * Proxy to impose time limitation on quota evaluation
     */
    private final TimeLimiter timeLimiter;

    /**
     * Tenant ID to namespace mapping cache. This is needed for resource control.
     */
    private final Cache<String, String> namespaceCache;

    /**
     * Quota evaluation will be bounded by this timeout.
     */
    private final Duration timeout;

    public QuotaEvaluator(ResourceControlledMetadataClient metadataClient,
                          Limits limit,
                          UsageCache usageCache,
                          String region,
                          Duration timeout,
                          int poolSize,
                          int queueSize) {
        this.usageCache = usageCache;
        this.namespaceCache = CacheBuilder.newBuilder()
                .maximumSize(2000)
                .build();
        Metrics.gauge("ws.namespaceCache.hitRate", () -> namespaceCache.stats().hitRate());

        final ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        poolSize,
                        poolSize,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(queueSize),
                        new ThreadFactoryBuilder().setNameFormat("QuotaEvaluator-%d").build());

        this.timeLimiter = SimpleTimeLimiter.create(executor);
        this.timeout = timeout;

        try {
            this.evaluator = new NonKievCompartmentQuotaEvaluator(
                    EvaluatorConfig.builder()
                            .identityService(new CompartmentTreeRetriever(metadataClient, namespaceCache))
                            .limitService((request, family, opcGuid) -> {
                                return limit.getQuotasAndServiceLimits(request);
                            })
                            .service(LIMITS_SERVICE_FAMILY) // This is set as family in quota request
                            .region(region) // Full region name
                            .usePreAggregatedUsage(false)
                            .evaluationMode(EvaluationMode.NegativeUnlimited)
                            .metricsEnabled(false)
                            .build()
            );
        } catch (CompartmentQuotaException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void evaluateAddUsage(String namespace,
                                 String tenantId,
                                 String srcCompartmentId,
                                 long size) {
        timeBoundedQuotaEvaluation(namespace, tenantId, srcCompartmentId, null, size, false);
    }

    public void evaluateMoveUsage(String namespace,
                                  String tenantId,
                                  String srcCompartmentId,
                                  String dstCompartmentId,
                                  long size) {
        timeBoundedQuotaEvaluation(namespace, tenantId, srcCompartmentId, dstCompartmentId, size, true);
    }


    /**
     * This is a time-limited operation to avoid affecting latency.
     * @param namespace
     * @param tenantId
     * @param srcCompartmentId
     * @param size
     * @throws Exception
     */
    private void timeBoundedQuotaEvaluation(String namespace,
                                            String tenantId,
                                            String srcCompartmentId,
                                            String dstCompartmentId,
                                            long size,
                                            boolean isMove) {

        Preconditions.checkState(!isMove || dstCompartmentId != null,
                "Destination compartment ID can't be empty for move request");
        Preconditions.checkState(isMove || dstCompartmentId == null,
                "Destination compartment ID can't be set for add request");

        QuotaEvaluationResult result = QuotaEvaluationResult.pass();

        long evaluationStartTime = System.nanoTime();

        try {
            result = timeLimiter.callWithTimeout(
                    () -> {
                        QuotaEvaluationMetrics.EVALUATION_REQUESTS_TOTAL.mark();
                        if (isMove) {
                            return evaluateMove(namespace, tenantId, srcCompartmentId, dstCompartmentId, size);
                        } else {
                            return evaluateAdd(namespace, tenantId, srcCompartmentId, size);
                        }
                    },
                    timeout.toMillis(), TimeUnit.MILLISECONDS);

        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompartmentQuotaException) {
                LOG.error("Quota evaluation failed for tenancy {}.", tenantId, e);
                QuotaEvaluationMetrics.EVALUATION_ERRORS_TOTAL.mark();
                QuotaEvaluationMetrics.QUOTA_ERRORS.mark();
            } else {
                LOG.error("Quota evaluation error for tenancy {}.", tenantId, e);
                QuotaEvaluationMetrics.EVALUATION_ERRORS_TOTAL.mark();
                QuotaEvaluationMetrics.UNKNOWN_ERRORS.mark();
            }
        } catch (RejectedExecutionException e) {
            LOG.error("Quota evaluation request rejected for tenancy {}.", tenantId, e);
            QuotaEvaluationMetrics.EVALUATION_ERRORS_TOTAL.mark();
            QuotaEvaluationMetrics.EVALUATION_REJECTED.mark();
        } catch (TimeoutException e) {
            LOG.error("Quota evaluation timed out for tenancy {}.", tenantId, e);
            QuotaEvaluationMetrics.EVALUATION_ERRORS_TOTAL.mark();
            QuotaEvaluationMetrics.EVALUATION_TIMEOUTS.mark();
        } catch (Exception e) {
            LOG.error("Quota evaluation error for tenancy {}.", tenantId, e);
            QuotaEvaluationMetrics.EVALUATION_ERRORS_TOTAL.mark();
            QuotaEvaluationMetrics.UNKNOWN_ERRORS.mark();
        }

        QuotaEvaluationMetrics.EVALUATION_LATENCY.update(System.nanoTime() - evaluationStartTime);

        if (!result.isSuccessful()) {
            if (result.getLimitViolations().size() > 0) {
                QuotaEvaluationMetrics.LIMIT_EVALUATION_REQUEST_FAILED.mark();
                throw new StorageLimitExceededException("Storage limit exceeded for tenancy during upload");
            }

            if (result.getQuotaViolations().size() > 0) {
                QuotaEvaluationMetrics.QUOTA_EVALUATION_REQUEST_FAILED.mark();
                throw new StorageQuotaExceededException(result.getFriendlyErrorMessage());
            }
        }

        QuotaEvaluationMetrics.EVALUATION_REQUEST_PASSED.mark();
    }

    private QuotaEvaluationResult getLimitViolationResult(long newObjectSizeInBytes,
                                                          long limitForTenant,
                                                          long usageForTenant) {
        Violation violation = new Violation(STORAGE_QUOTA_NAME, limitForTenant, newObjectSizeInBytes,
                usageForTenant, null);
        return new QuotaEvaluationResult(Arrays.asList(violation), new HashMap(), new ArrayList(), new HashMap(),
                "", "", false);
    }

    private QuotaEvaluationResult evaluateAdd(String namespace,
                                              String tenantId,
                                              String compartmentId,
                                              long size) throws CompartmentQuotaException {
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(tenantId);
        Preconditions.checkNotNull(compartmentId);

        final Map<String, Long> proposedIncreases = new HashMap<>();
        proposedIncreases.put(STORAGE_QUOTA_NAME, size);

        CreateResourcePayload requestPayload = CreateResourcePayload.builder()
                .tenantId(tenantId)
                .compartmentId(compartmentId)
                .resourceIncreases(proposedIncreases)
                .build();

        final QuotaEvaluationRequest quotaEvalRequest = evaluator.buildRequest(requestPayload);

        return evaluateRequest(namespace, size, quotaEvalRequest);
    }

    private QuotaEvaluationResult evaluateMove(String namespace,
                                               String tenantId,
                                               String sourceCompartmentId,
                                               String destCompartmentId,
                                               long size) throws CompartmentQuotaException {
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(tenantId);
        Preconditions.checkNotNull(sourceCompartmentId);
        Preconditions.checkNotNull(destCompartmentId);

        final Map<String, Long> proposedIncreases = new HashMap<>();
        proposedIncreases.put(STORAGE_QUOTA_NAME, size);

        MoveResourcePayload requestPayload = MoveResourcePayload.builder()
                .tenantId(tenantId)
                .oldCompartmentId(sourceCompartmentId)
                .newCompartmentId(destCompartmentId)
                .resourceIncreases(proposedIncreases)
                .build();

        final QuotaEvaluationRequest quotaEvalRequest = evaluator.buildRequest(requestPayload);

        return evaluateRequest(namespace, size, quotaEvalRequest);
    }

    private QuotaEvaluationResult evaluateRequest(String namespace,
                                                  long size,
                                                  QuotaEvaluationRequest quotaEvalRequest)
            throws CompartmentQuotaException {
        final String ns = namespaceCache.getIfPresent(quotaEvalRequest.getTenantId());
        if (ns == null) {
            // Sets the namespace for tenant ID to be used by ResourceControlledMetadataClient
            // while calling identity service
            namespaceCache.put(quotaEvalRequest.getTenantId(), namespace);
        }

        // prime() performs the network intensive work. It fetches the compartment tree from the identity
        // data-plane, determine the lineage of the target compartment, and then fetch the relevant quotas
        // from the limits DP.
        quotaEvalRequest.prime();

        long limitValue = getLimitValue(quotaEvalRequest);

        final List<CompartmentQuotas> compartmentQuotas = quotaEvalRequest.getQuotas().getCompartmentQuotas();

        if (quotaEvalRequest.isMove()) {
            if (compartmentQuotas.size() == 0) {
                // No quota value is set. Skip evaluation for move requests.
                QuotaEvaluationMetrics.EVALUATION_REQUEST_SKIPPED.mark();
                return QuotaEvaluationResult.pass();
            } else {
                // No limit evaluation is required for move operation
                final Map<String, Long> usageFromCache = usageCache.getUsageFromCache(namespace);
                return evaluateStorageQuota(quotaEvalRequest, usageFromCache);
            }
        } else {
            if (limitValue == Limits.DEFAULT_NO_STORAGE_LIMIT_BYTES && compartmentQuotas.size() == 0) {
                // No quota or limit values are set. Skip evaluation.
                QuotaEvaluationMetrics.EVALUATION_REQUEST_SKIPPED.mark();
                return QuotaEvaluationResult.pass();
            }

            if (limitValue == 0) {
                return getLimitViolationResult(size, limitValue, 0);
            }

            Map<String, Long> usageFromCache = usageCache.getUsageFromCache(namespace);

            // Evaluate storage limit first
            if (limitValue != Limits.DEFAULT_NO_STORAGE_LIMIT_BYTES) {
                final long usageForTenant = Math.max(0, usageFromCache.getOrDefault(namespace, 0L));
                QuotaEvaluationResult limitResult = evaluateStorageLimit(size, limitValue, usageForTenant);
                if (!limitResult.isSuccessful()) {
                    return limitResult;
                }
            }

            // Evaluate storage quota
            if (compartmentQuotas.size() != 0) {
                return evaluateStorageQuota(quotaEvalRequest, usageFromCache);
            }
        }

        return QuotaEvaluationResult.pass();
    }

    private long getLimitValue(QuotaEvaluationRequest quotaEvalRequest) {

        Preconditions.checkState(quotaEvalRequest.isPrimed());

        long limitValue = Limits.DEFAULT_NO_STORAGE_LIMIT_BYTES;

        final List<ServiceLimitEntry> serviceLimits = quotaEvalRequest.getQuotas().getServiceLimits();
        for (ServiceLimitEntry serviceLimitEntry : serviceLimits) {
            if (serviceLimitEntry.getName().equals(STORAGE_QUOTA_NAME.toLowerCase())) {
                // Get tenancy limit from quota request
                limitValue = serviceLimitEntry.getValue();
            }
        }

        return limitValue;
    }

    private QuotaEvaluationResult evaluateStorageLimit(long newObjectSizeInBytes,
                                                       long limitForTenant,
                                                       long usageForTenant) {

        Preconditions.checkState(limitForTenant >= 0);
        Preconditions.checkState(usageForTenant >= 0);

        // Make sure there is sufficient storage capacity remaining to add the new object. If capacity is full,
        // return false, even for 0 byte objects.
        // e.g. storage limit bytes is 10, usage is 5. We allow to put object with 5 bytes.
        // e.g. storage limit bytes is 10, usage is 10. We don't allow to put any object (including 0 byte object)
        // e.g. storage limit bytes is 0, usage is 0. We don't allow to put any object (including 0 byte object)

        final long remainingStorageCapacity = limitForTenant - usageForTenant;

        if (remainingStorageCapacity < newObjectSizeInBytes ||
                (remainingStorageCapacity == newObjectSizeInBytes && newObjectSizeInBytes == 0)) {

            return getLimitViolationResult(newObjectSizeInBytes, limitForTenant, usageForTenant);
        }

        return QuotaEvaluationResult.pass();
    }

    private QuotaEvaluationResult evaluateStorageQuota(QuotaEvaluationRequest quotaEvalRequest,
                                                       Map<String, Long> usageFromCache)
            throws CompartmentQuotaException {

        Preconditions.checkState(quotaEvalRequest.isPrimed());

        ResourceUsageRetriever usageRetriever = (compartmentOcid, quota) -> {
            Long usage = usageFromCache.get(compartmentOcid);
            return usage != null ? usage : 0;
        };

        return evaluator.evaluate(quotaEvalRequest, usageRetriever);
    }

}
