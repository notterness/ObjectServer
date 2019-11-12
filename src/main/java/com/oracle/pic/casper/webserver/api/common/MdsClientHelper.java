package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.exceptions.CasperException;
import com.oracle.pic.casper.common.metrics.MetricsBundle;
import com.oracle.pic.casper.webserver.api.backend.MdsMetrics;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.Callable;
import java.util.function.Function;

public final class MdsClientHelper {
    private MdsClientHelper() {
    }

    public static <T> T invokeMds(MetricsBundle aggregateBundle,
                                  MetricsBundle apiBundle,
                                  boolean retryable,
                                  Callable<T> callable,
                                  Function<StatusRuntimeException, RuntimeException> exceptionHandler) {
        try {
            return MdsMetrics.executeWithMetrics(aggregateBundle, apiBundle, retryable, callable);
        } catch (StatusRuntimeException ex) {
            throw exceptionHandler.apply(ex);
        } catch (Exception ex) {
            throw new CasperException(ex.getMessage(), ex.getCause());
        }
    }
}
