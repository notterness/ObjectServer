package com.oracle.pic.casper.webserver.api.auditing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.config.ConfigAvailabilityDomain;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.v2.EventServiceConfiguration;
import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.util.IdUtil;
import com.oracle.pic.casper.webserver.auth.limits.Limits;
import com.oracle.pic.casper.webserver.auth.CasperResourceKind;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.commons.metadata.entities.Compartment;
import com.oracle.pic.commons.metadata.entities.User;
import com.oracle.pic.identity.authentication.ClaimType;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.PrincipalType;
import com.oracle.pic.sherlock.common.event.AuditEventV2;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class AuditFilterHandler implements Handler<RoutingContext> {

    private static final Logger LOG = LoggerFactory.getLogger(AuditFilterHandler.class);
    private static final String CLOUD_EVENTS_VERSION = "0.1";
    private static final String EVENT_TYPE_VERSION = "2.0";
    private static final String SOURCE = "ObjectStorage";
    private static final String UNKNOWN_PRINCIPAL = "UnknownPrincipal";
    private static final String UNKNOWN_COMPARTMENT = "UnknownCompartment";
    private static final String PAR_PATH_MATCHER_REGEX = "/p/([^/]+)";
    private static final String PAR_REPLACEMENT = "/p/[SANITIZED]";
    private static final AuditEventV2.Data.InternalDetails EVENTS_INTERNAL_DETAILS = AuditEventV2.Data
        .InternalDetails.builder().attributes(ImmutableMap.of("NO_ARCHIVE", true, "NO_INDEX", true)).build();

    //temporary flag, remove after we're fully off old events publishing pipeline
    private final boolean publishEvents;
    private final AuditLogger auditLogger;
    private final Limits limits;

    public AuditFilterHandler(EventServiceConfiguration config,
                              AuditLogger auditLogger,
                              Limits limits) {
        this.publishEvents = config.isEnabled() && !config.useOldPublisher();
        this.auditLogger = auditLogger;
        this.limits = limits;
    }

    /**
     * Construct an audit event in the Audit V2 schema for this request and write it to audit logs, if it should be
     * audited (see shouldLog() below).
     *
     * If it is not audited, log the events if events are enabled (see shouldLogEvent()).
     */
    @Override
    public void handle(RoutingContext context) {
        final WSRequestContext wsContext = WSRequestContext.get(context);
        wsContext.pushEndHandler(c -> {
            final boolean shouldLogAudit = shouldLog(context, wsContext);
            final boolean shouldLogEvent = shouldLogEvent(context, wsContext);
            if (shouldLogAudit) {
                logAudit(context, wsContext);
            }
            if (shouldLogEvent) {
                logEvent(shouldLogAudit, context, wsContext);
            }
        });
        context.next();
    }

    private void logAudit(RoutingContext context, WSRequestContext wsContext) {
        final AuditEventV2 entry;
        try {
            entry = generateAuditEntry(context, wsContext);
        } catch (Exception e) {
            LOG.error("Failed to generate audit entry", e);
            WebServerMetrics.AUDIT_ERROR.inc();
            return;
        }
        try {
            auditLogger.log(entry);
        } catch (Exception e) {
            LOG.error("Failed to log audit {}", entry, e);
            WebServerMetrics.AUDIT_ERROR.inc();
            return;
        }
        WebServerMetrics.AUDIT_SUCCESS.inc();
    }

    private void logEvent(boolean auditLogged, RoutingContext context, WSRequestContext wsContext) {
        wsContext.getObjectEvents().forEach(event -> {
            //skip if the event is already audited
            if (auditLogged &&
                event.getEventType().getName().equals(wsContext.getOperation().map(o -> o.getSummary()).orElse(null))) {
                return;
            }
            final AuditEventV2 entry;
            try {
                entry = generateEvent(context, wsContext, event);
            } catch (Exception e) {
                LOG.error("Failed to generate event entry", e);
                WebServerMetrics.OBJECT_EVENTS_ERROR.get(event.getEventType()).inc();
                return;
            }
            try {
                auditLogger.log(entry);
            } catch (Exception e) {
                LOG.error("Failed to log event {}", entry, e);
                WebServerMetrics.OBJECT_EVENTS_ERROR.get(event.getEventType()).inc();
                return;
            }
            WebServerMetrics.OBJECT_EVENTS_SUCCESS.get(event.getEventType()).inc();
        });
    }

    /**
     * Return whether this request should be logged or not.
     *
     * Each operation has an audit level - some operations we never log, some we always log, and some we log only for
     * certain customers such as Verizon because those operations (mainly object level) are high volume.
     *
     * For those requests, the level of logging on set on the bucket (audit mode), which can be disabled, write only,
     * or read and write.
     */
    @VisibleForTesting
    static boolean shouldLog(RoutingContext context, WSRequestContext wsContext) {
        //Do not audit requests that are not real request, such as http requests that hit our endpoint but do not map
        //to any API. e.g. https://objectstorage.us-phoenix-1.oraclecloud.com/this/is/not/a/real/request
        final Optional<CasperOperation> optionalOperation = wsContext.getOperation();
        if (!optionalOperation.isPresent()) {
            return false;
        }

        final CasperOperation operation = optionalOperation.get();
        final AuditLevel auditLevel = operation.getAuditLevel();
        if (auditLevel == AuditLevel.NEVER) {
            return false;
        }

        //do not audit object level requests that are disabled
        //do not audit object level read requests if the audit mode is write
        final ObjectLevelAuditMode auditMode = wsContext.getObjectLevelAuditMode();
        if (auditLevel == AuditLevel.OBJECT_ACCESS &&
            (auditMode == ObjectLevelAuditMode.Disabled ||
                (auditMode == ObjectLevelAuditMode.Write && isReadOperation(context)))) {
            return false;
        }

        return true;
    }

    private boolean shouldLogEvent(RoutingContext context, WSRequestContext wsContext) {
        if (!publishEvents) {
            return false;
        }
        if (limits.getEventsWhitelistedTenancies().contains(wsContext.getRequestorTenantOcid())) {
            return !context.failed();
        }
        return wsContext.isObjectEventsEnabled();
    }

    private static boolean isReadOperation(RoutingContext context) {
        final HttpMethod method = context.request().method();
        return method == HttpMethod.GET || method == HttpMethod.HEAD || method == HttpMethod.OPTIONS;
    }

    /**
     * Generate an AuditEventV2 object for this request
     */
    @VisibleForTesting
    static AuditEventV2 generateAuditEntry(RoutingContext context, WSRequestContext wsContext) {
        final CasperOperation operation = wsContext.getOperation().get();
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final Date now = Date.from(Instant.now());
        //make sure to not log the actual PAR
        final String uri = request.uri().replaceAll(PAR_PATH_MATCHER_REGEX, PAR_REPLACEMENT);
        final String requestId = wsContext.getCommonRequestContext().getOpcRequestId();
        //fill in the fields for audit
        final AuditEventV2.Data.Identity identity = AuditEventV2.Data.Identity.builder()
            .tenantId(wsContext.getRequestorTenantOcid())
            .credentials(VertxAuditHelper.getCredentialId(request))
            .principalName(getPrincipalDisplayName(wsContext.getPrincipal().orElse(null),
                    wsContext.getUser().orElse(null)))
            .principalId(wsContext.getPrincipal().map(p -> p.getSubjectId()).orElse(null))
            .ipAddress(wsContext.getRealIP())
            .userAgent(request.getHeader(HttpHeaders.USER_AGENT))
            // When the principal is of type USER then the authType will be equal to principal subtype,
            // or in all other cases set authType to null because principal.getSubType() returns PrincipalSubType.NONE
            .authType(wsContext.getPrincipal().filter(p -> p.getType() == PrincipalType.USER)
                    .map(p -> p.getSubType()).map(sT -> sT.value()).orElse(null))
            .build();
        final Map<String, String[]> requestParams = VertxAuditHelper.getRequestParameters(request);
        //for backwards compatibility, we need to add the kms key ids to request parameters
        addKmsKeyIdToParam(wsContext.getOperation().orElse(null), wsContext.getNewkmsKeyId(),
            wsContext.getOldKmsKeyId(), requestParams);
        final AuditEventV2.Data.Request auditRequest = AuditEventV2.Data.Request.builder()
            .id(requestId)
            //make sure to not log the actual PAR
            .path(request.path().replaceAll(PAR_PATH_MATCHER_REGEX, PAR_REPLACEMENT))
            .action(request.rawMethod())
            .parameters(requestParams)
            .headers(VertxAuditHelper.getRequestHeaders(request))
            .build();
        final AuditEventV2.Data.Response auditResponse = AuditEventV2.Data.Response.builder()
            .status(response.getStatusCode() + "")
            .responseTime(now)
            .headers(VertxAuditHelper.getResponseHeaders(response))
            .message(response.getStatusMessage())
            .payload(ImmutableMap.of("resourceName", uri, "id", uri))
            .build();
        //include any state changes that were stored in wsContext
        final AuditEventV2.Data.StateChange stateChange = wsContext.getOldState().isEmpty() &&
            wsContext.getNewState().isEmpty() ? null : AuditEventV2.Data.StateChange.builder()
                .previous(wsContext.getOldState())
                .current(wsContext.getNewState())
                .build();
        final Map<String, Object> additionalDetails = new HashMap<>();
        wsContext.getNamespaceName().ifPresent(namespace -> additionalDetails.put("namespace", namespace));
        wsContext.getBucketName().ifPresent(bucketName -> additionalDetails.put("bucketName", bucketName));
        wsContext.getBucketOcid().ifPresent(bucketId -> additionalDetails.put("bucketId", bucketId));
        wsContext.getEtag().ifPresent(etag -> additionalDetails.put("eTag", etag));
        wsContext.getBucketPublicAccessType().ifPresent(accessType -> additionalDetails.put("publicAccessType",
            accessType.getValue()));
        wsContext.getArchivalState().ifPresent(archivalState -> additionalDetails.put("archivalState",
            archivalState.getState()));

        //for bucket operations, set resourceId to the bucket ocid if possible, else use URI
        final String resourceId = operation.getResourceKind() == CasperResourceKind.BUCKETS ?
            wsContext.getBucketOcid().orElse(uri) : uri;

        final AuditEventV2.Data data = AuditEventV2.Data.builder()
            .eventGroupingId(requestId)
            .eventName(operation.getSummary())
            .compartmentId(wsContext.getCompartmentID().orElse(null))
            .compartmentName(getCompartmentDisplayName(wsContext.getCompartment().orElse(null)))
            .resourceName(wsContext.getResourceName().orElse(uri))
            .resourceId(resourceId)
            .availabilityDomain(availabilityDomainFormat(
                ConfigRegion.tryFromSystemProperty().orElse(ConfigRegion.LOCAL),
                ConfigAvailabilityDomain.tryFromSystemProperty().orElse(ConfigAvailabilityDomain.GLOBAL)))
            .tagSlug(wsContext.getTagSlug())
            .identity(identity)
            .request(auditRequest)
            .response(auditResponse)
            .stateChange(stateChange)
            .additionalDetails(additionalDetails)
            .build();
        return AuditEventV2.builder()
            .eventType(eventTypeFormat(operation))
            .cloudEventsVersion(CLOUD_EVENTS_VERSION)
            .eventTypeVersion(EVENT_TYPE_VERSION)
            .contentType(ContentType.APPLICATION_JSON)
            .source(SOURCE)
            .eventId(IdUtil.newUniqueId())
            .eventTime(now)
            .data(data)
            .build();
    }

    private AuditEventV2 generateEvent(RoutingContext context, WSRequestContext wsContext, ObjectEvent event) {
        final Date now = Date.from(Instant.now());
        //make sure to not log the actual PAR
        final String uri = context.request().uri().replaceAll(PAR_PATH_MATCHER_REGEX, PAR_REPLACEMENT);
        final String requestId = wsContext.getCommonRequestContext().getOpcRequestId();
        final Map<String, Object> additionalDetails = new HashMap<>();
        wsContext.getNamespaceName().ifPresent(namespace -> additionalDetails.put("namespace", namespace));
        wsContext.getBucketName().ifPresent(bucketName -> additionalDetails.put("bucketName", bucketName));
        wsContext.getBucketOcid().ifPresent(bucketId -> additionalDetails.put("bucketId", bucketId));
        additionalDetails.put("eTag", event.getETag());
        additionalDetails.put("archivalState", event.getArchivalState());

        final AuditEventV2.Data data = AuditEventV2.Data.builder()
            .eventGroupingId(requestId)
            .eventName(event.getEventType().getName())
            .compartmentId(wsContext.getCompartmentID().orElse(null))
            .compartmentName(getCompartmentDisplayName(wsContext.getCompartment().orElse(null)))
            .resourceName(event.getObjectName())
            .resourceId(uri)
            .availabilityDomain(availabilityDomainFormat(
                ConfigRegion.tryFromSystemProperty().orElse(ConfigRegion.LOCAL),
                ConfigAvailabilityDomain.tryFromSystemProperty().orElse(ConfigAvailabilityDomain.GLOBAL)))
            .tagSlug(wsContext.getTagSlug())
            .additionalDetails(additionalDetails)
            .internalDetails(EVENTS_INTERNAL_DETAILS)
            .build();
        return AuditEventV2.builder()
            .eventType(eventTypeFormat(event.getEventType()))
            .cloudEventsVersion(CLOUD_EVENTS_VERSION)
            .eventTypeVersion(EVENT_TYPE_VERSION)
            .contentType(ContentType.APPLICATION_JSON)
            .source(SOURCE)
            .eventId(IdUtil.newUniqueId())
            .eventTime(now)
            .data(data)
            .build();
    }

    /**
     * Get the compartment name from the compartment.
     */
    private static String getCompartmentDisplayName(Compartment compartment) {
        if (compartment == null) {
            return UNKNOWN_COMPARTMENT;
        }
        return compartment.getDisplayName();
    }

    /**
     * Use the provided subjectOcid to get the principal's display name.
     * The subjectId may not actually be a user -- in the case of service principal or even instance principal.
     * In that case just return the best appropriate name.
     * This method will never throw and returns UNKNOWN_PRINCIPAL in the case of a failure.
     */
    private static String getPrincipalDisplayName(Principal principal, User user) {
        if (principal == null || user == null) {
            return UNKNOWN_PRINCIPAL;
        }
        switch (principal.getType()) {
            case USER:
                return user.getDisplayName();
            case SERVICE:
                return principal.getClaimValue(ClaimType.SERVICE_NAME).orElse(UNKNOWN_PRINCIPAL);
            case RESOURCE:
                // Use the OCID of the resource
                return principal.getClaimValue(ClaimType.RESOURCE_ID).orElse(UNKNOWN_PRINCIPAL);
            case INSTANCE:
                //subjectId for instance principal is just the instance ocid
                return principal.getSubjectId();
            default:
                return UNKNOWN_PRINCIPAL;
        }
    }

    @VisibleForTesting
    public static final String eventTypeFormat(CasperOperation operation) {
        return String.format("com.oraclecloud.objectstorage.%s", operation.getSummary().toLowerCase());
    }

    private static String eventTypeFormat(ObjectEvent.EventType eventType) {
        return String.format("com.oraclecloud.objectstorage.%s", eventType.getName().toLowerCase());
    }

    @VisibleForTesting
    static String availabilityDomainFormat(ConfigRegion region, ConfigAvailabilityDomain ad) {
        //audit uses this specific format for ADs
        final String adName;
        switch (ad) {
            case AD1:
                adName = "AD-1";
                break;
            case AD2:
                adName = "AD-2";
                break;
            case AD3:
                adName = "AD-3";
                break;
            default:
                adName = "GLOBAL";
        }
        return String.format("%s-%s", region.getAirportCode(), adName);
    }

    private static void addKmsKeyIdToParam(CasperOperation operation,
                                    String newKmsKeyId,
                                    String oldKmsKeyId,
                                    Map<String, String[]> requestParameters) {
        if (operation == CasperOperation.CREATE_BUCKET
            || operation == CasperOperation.REENCRYPT_BUCKET
            || operation == CasperOperation.REENCRYPT_OBJECT) {
            if (!Strings.isNullOrEmpty(newKmsKeyId)) {
                requestParameters.put("kmsKeyId", new String[]{newKmsKeyId});
            }
        } else if (operation == CasperOperation.UPDATE_BUCKET) {
            if (!Objects.equals(newKmsKeyId, oldKmsKeyId)) {
                final String[] newKmsKeyIdArr = Strings.isNullOrEmpty(newKmsKeyId) ? null : new String[]{newKmsKeyId};
                final String[] oldKmsKeyIdArr = Strings.isNullOrEmpty(oldKmsKeyId) ? null : new String[]{oldKmsKeyId};
                requestParameters.put("newKmsKeyId", newKmsKeyIdArr);
                requestParameters.put("oldKmsKeyId", oldKmsKeyIdArr);
            }
        }
    }
}
