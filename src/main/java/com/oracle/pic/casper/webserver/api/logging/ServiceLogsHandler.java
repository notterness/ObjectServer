package com.oracle.pic.casper.webserver.api.logging;

import com.google.common.annotations.VisibleForTesting;
import com.oracle.pic.casper.common.config.v2.ServiceLogConfiguration;
import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.auditing.AuditLevel;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.WSExceptionTranslator;
import com.oracle.pic.casper.webserver.api.model.BucketLoggingStatus;
import com.oracle.pic.casper.webserver.api.model.logging.ServiceLogEntry;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.identity.authorization.permissions.ActionKind;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * This handler is responsible for writing the {@link ServiceLogEntry} to service.log
 *
 * These log files give insight to the customers about the object operations they perform
 * on the buckets in their tenancy.
 */
public class ServiceLogsHandler implements Handler<RoutingContext> {
    private static Logger log = LoggerFactory.getLogger(ServiceLogsHandler.class);

    private final ServiceLogWriter serviceLogWriter;
    private final ServiceLogConfiguration serviceLogConfiguration;
    private final WSExceptionTranslator wsExceptionTranslator;

    public ServiceLogsHandler(ServiceLogWriter serviceLogWriter,
                              ServiceLogConfiguration serviceLogConfiguration,
                              WSExceptionTranslator wsExceptionTranslator) {
        this.serviceLogWriter = serviceLogWriter;
        this.serviceLogConfiguration = serviceLogConfiguration;
        this.wsExceptionTranslator = wsExceptionTranslator;
    }

    /**
     * Conditions for logging to service logs:
     * 1) Service Logs should be enabled for the region
     * 2) The operation has to be a object level operation (AuditLevel.OBJECT_ACCESS)
     * 4) The serviceLogStatus on the bucket matches the casperOperation
     */
    @VisibleForTesting
    String getLogObjectId(Optional<CasperOperation> casperOperation,
                      Optional<BucketLoggingStatus> bucketLoggingStatus) {
        if (!serviceLogConfiguration.isEnabled() || !bucketLoggingStatus.isPresent()) {
            return null;
        }
        // If a failure happens through a non-real request.
        if (!casperOperation.isPresent()) {
            return null;
        }
        if (casperOperation.get().getAuditLevel() != AuditLevel.OBJECT_ACCESS) {
            return null;
        }
        if (casperOperation.get().getActionKind() == ActionKind.READ ||
                casperOperation.get().getActionKind() == ActionKind.LIST) {
            return bucketLoggingStatus.get().getReadLogId();
        } else {
            return bucketLoggingStatus.get().getWriteLogId();
        }
    }

    @Override
    public void handle(RoutingContext context) {
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.pushEndHandler((c) -> {
            String logId = getLogObjectId(wsRequestContext.getOperation(), wsRequestContext.getBucketLoggingStatus());
            if (StringUtils.isNotEmpty(logId)) {
                HttpServerResponse response = context.response();
                final int statusCode = response.getStatusCode();

                if (HttpResponseStatus.isError(statusCode)) {
                    final Throwable rootCause = wsExceptionTranslator.rewriteException(context, context.failure());
                    ErrorCode errorCode = wsExceptionTranslator.getErrorCode(rootCause);
                    ServiceLogsHelper.logServiceEntry(context, errorCode);
                }

                for (ServiceLogEntry serviceLogEntry : wsRequestContext.getServiceLogEntryList()) {

                    /**
                     * For bulk operation calls, all of the individual deletes get the same time
                     * Alternative - we can set the endTime in the bulk operation handlers (which is not the true endTime as well)
                     */
                    serviceLogEntry.updateEndTime(System.currentTimeMillis());

                    serviceLogEntry.updateLogObjectId(logId);

                    serviceLogWriter.log(serviceLogEntry, serviceLogConfiguration.isApolloEnabled());
                }
            }
        });
        context.next();
    }
}
