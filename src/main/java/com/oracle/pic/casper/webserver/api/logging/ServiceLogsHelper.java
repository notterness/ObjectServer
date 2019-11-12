package com.oracle.pic.casper.webserver.api.logging;

import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.webserver.api.auditing.VertxAuditHelper;
import com.oracle.pic.casper.webserver.api.model.logging.ServiceLogContent;
import com.oracle.pic.casper.webserver.api.model.logging.ServiceLogEntry;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.PrincipalType;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 *  This class is responsible for adding a serviceLogEntry to the web server context {@link WSRequestContext}.
 *
 *  The log method is invoked from various backend methods which perform the api operation.
 */
public final class ServiceLogsHelper {
    private static Logger log = LoggerFactory.getLogger(ServiceLogsHelper.class);

    private static final String UNKNOWN = "Unknown";
    private static final String PAR_PATH_MATCHER_REGEX = "/p/([^/]+)";
    private static final String PAR_REPLACEMENT = "/p/[SANITIZED]";
    private static final String PAR_UPLOAD_ROUTE = "/p/([^/]+)/n/([^/]+)/b/([^/]+)/u/";
    private static final String PAR_OBJECT_ROUTE = "/p/([^/]+)/n/([^/]+)/b/([^/]+)/o/(.+)";
    private static final Pattern PAR_ROUTE_PATTERN = Pattern.compile(PAR_OBJECT_ROUTE + "|" + PAR_UPLOAD_ROUTE);

    private ServiceLogsHelper() {
    }

    public static void logServiceEntry(RoutingContext context) {
        logServiceEntry(context, null);
    }


    public static void logServiceEntry(RoutingContext context, ErrorCode errorCode) {
        WSRequestContext wsRequestContext = WSRequestContext.get(context);
        Principal principal = wsRequestContext.getPrincipal().orElse(null);
        if (principal == null && StringUtils.isEmpty(wsRequestContext.getRequestorTenantOcid())) {
            log.info("TenantOcid not present in the context, service logServiceEntry not recorded");
            return;
        }
        String tenantOcid = wsRequestContext.getRequestorTenantOcid() == null ?
                principal.getTenantId() : wsRequestContext.getRequestorTenantOcid();
        ServiceLogEntry serviceLogEntry = generateServiceLogEntry(
                context, Optional.ofNullable(errorCode), tenantOcid, principal);
        wsRequestContext.addServiceLogEntry(serviceLogEntry);
    }

    private static ServiceLogEntry generateServiceLogEntry(RoutingContext context, Optional<ErrorCode> errorCode,
                                                    String tenantOcid, Principal principal) {
        WSRequestContext wsRequestContext = WSRequestContext.get(context);
        HttpMethod method = context.request().method();
        ConfigRegion region = ConfigRegion.tryFromSystemProperty().orElse(ConfigRegion.LOCAL);

        final HttpServerRequest request = context.request();
        // Sanitize PAR nonces (secrets) out of request paths.
        final String uri = request.uri().replaceAll(PAR_PATH_MATCHER_REGEX, PAR_REPLACEMENT);
        final boolean isPar =  PAR_ROUTE_PATTERN.matcher(request.uri()).matches();
        final String principalType = principal == null ? PrincipalType.USER.value() : principal.getType().value();
        final long timeSinceStartNS = System.nanoTime() - wsRequestContext.getStartTime();

        ServiceLogContent.ServiceLogContentBuilder serviceLogContentBuilder = ServiceLogContent.ServiceLogContentBuilder
                .aServiceLogContent()
                .objectName(wsRequestContext.getObjectName().orElse(null))
                .tenantId(tenantOcid)
                .tenantName(wsRequestContext.getTenantName())
                .compartmentId(wsRequestContext.getCompartmentID().orElse(UNKNOWN))
                .bucketName(wsRequestContext.getBucketName().orElse(UNKNOWN))
                .startTime(Instant.now().minus(timeSinceStartNS, ChronoUnit.NANOS).toEpochMilli())
                .requestResourcePath(uri)
                .requestAction(method)
                .namespaceName(wsRequestContext.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE))
                .clientIpAddress(wsRequestContext.getRealIP())
                .isPar(isPar)
                .principalId(principal == null ? UNKNOWN : principal.getSubjectId())
                .vcnId(wsRequestContext.getVcnID().orElse(null))
                .credentials(VertxAuditHelper.getCredentialId(request))
                .userAgent(request.getHeader(HttpHeaders.USER_AGENT))
                .opcRequestId(wsRequestContext.getCommonRequestContext().getOpcRequestId())
                .region(region.getFullName())
                .eTag(wsRequestContext.getEtag().orElse(null))
                .bucketCreator(wsRequestContext.getBucketCreator().orElse(UNKNOWN))
                .apiType(wsRequestContext.getCasperAPI().getValue())
                .authenticationType(principalType);

        wsRequestContext.getOperation().ifPresent(op -> serviceLogContentBuilder.operation(op.getSummary()));
        errorCode.ifPresent((ec) -> {
            serviceLogContentBuilder.errorCode(ec.getErrorName());
            serviceLogContentBuilder.statusCode(ec.getStatusCode());
        });
        wsRequestContext.getCompartment().ifPresent((compartment) ->
                serviceLogContentBuilder.compartmentName(compartment.getDisplayName()));

        if (context.response() != null) {
            serviceLogContentBuilder.statusCode(context.response().getStatusCode());
        } else {
            log.info("Status code not recorded in service log");
        }
        return new ServiceLogEntry(
                wsRequestContext.getCompartmentID().orElse(UNKNOWN),
                wsRequestContext.getBucketName().orElse(UNKNOWN), serviceLogContentBuilder.build());
    }
}
