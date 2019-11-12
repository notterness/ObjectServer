package com.oracle.pic.casper.webserver.limit;

import com.oracle.pic.casper.common.exceptions.CasperException;

import java.util.concurrent.Callable;

public final class ResourceLimitHelper {

    private ResourceLimitHelper() {
    }

    /**
     * This function acquires a resource, call the callable while holding the resource
     * and release the resource after the callable completes. The function also logs the
     * function name that acquires the resource. The function name is retrieved from the
     * callstack on frame 2.
     *
     * stackTraceElements[0]: getStackTrace call itself.
     * stackTraceElements[1]: withResourceControl.
     * stackTraceElements[2]: The actual code that is trying to acquire the resource.
     */
    public static <T> T withResourceControl(ResourceLimiter resourceLimiter,
                                            ResourceType resourceType,
                                            String namespace,
                                            Callable<T> callable) {
        final StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        final String context = stackTraceElements[2].getClassName() + "::" + stackTraceElements[2].getMethodName();
        try (ResourceTicket resourceTicket =
                     resourceLimiter.acquireResourceTicket(namespace, resourceType, context)) {
            return callable.call();
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new CasperException(ex.getMessage(), ex);
        }
    }
}
