package com.oracle.pic.casper.webserver.util;

import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.webserver.api.auditing.ObjectEvent;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.CasperAPI;
import com.oracle.pic.casper.webserver.api.model.BucketLoggingStatus;
import com.oracle.pic.casper.webserver.api.model.PreAuthenticatedRequestMetadata;
import com.oracle.pic.casper.webserver.api.model.logging.ServiceLogEntry;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoContext;
import com.oracle.pic.commons.metadata.entities.Compartment;
import com.oracle.pic.commons.metadata.entities.User;
import com.oracle.pic.identity.authentication.Principal;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class WSRequestContext {

    public static final String REQUEST_CONTEXT_KEY = WSRequestContext.class.getCanonicalName();
    private static final Logger LOG = LoggerFactory.getLogger(WSRequestContext.class);

    private final CommonRequestContext commonRequestContext;
    private final long startTime;
    private final CasperAPI casperAPI;

    /**
     * MSS -- Maximum Segment Size --
     * Specifies the largest amount of data, specified in bytes,
     * that can be received in a single TCP segment.
     * It's a useful metric to know whether a certain route supports JumboFrames vs. 1500 MTU
     */
    private final int mss;

    /**
     * The embargo rules for the request, set by the EmbargoHandler and used in authorization.
     */
    @Nullable
    private volatile EmbargoContext visa;

    /**
     * The compartment ID used to authorize the request, set during authorization and used by the AuditFilterHandler.
     */
    @Nullable
    private volatile String compartmentID;

    @Nullable
    private volatile Compartment compartment;

    /**
     * The principal of the user making the request, set during authentication and used by the AuditFilterHandler.
     *  Various possible principals are covered:
     *  https://confluence.oci.oraclecorp.com/display/PLAT/How+to+use+Principal+object
     *  1. A user under the tenancy. The subject id is the user OCID
     *  2. An actual compute instance, either bare metal or VM. The subject id is the instance id
     *  3. A OCI service, running in or outside service enclave. The user id is service-name/certificate-fingerprint
     */
    @Nullable
    private volatile Principal principal;

    @Nullable
    private volatile User user;

    /*
     *The namespace name of the namespace accessed by the request (if any), set by request handlers
     */
    @Nullable
    private volatile String namespaceName;

    /**
     * The bucket name of the bucket accessed by the request (if any), set by request handlers and used by metering.
     */
    @Nullable
    private volatile String bucketName;

    /**
     * The object name of the object accessed by the request (if any), set by request handlers and used by metering.
     */
    @Nullable
    private volatile String objectName;

    /**
     * The internal ID of the VCN from which the request was sent (if any), set by CommonHandler, used in authorization.
     */
    @Nullable
    private final String vcnID;

    /**
     * The internal ID of the VCN from which the request was sent for white-listed tenants, set by CommonHandler, used
     * in authorization.
     */
    @Nullable
    private final String vcnDebugID;

    /**
     * The "real" IP of the request, set by CommonHandler, used in authorization.
     */
    private volatile String realIP;

    /**
     * An IP set by a test tenancy to exercise a CIDR based authorization policy.
     */
    private volatile String debugIP;

    /**
     * The host of the request, set by the CommonHandler, will be used for metering
     */
    private volatile String targetHost;

    /**
     * The PAR access type, set during authentication, used by authorization.
     */
    @Nullable
    private volatile PreAuthenticatedRequestMetadata.AccessType parAccessType;

    /**
     * The time of expiration for the PAR, set during authentication, used by authorization.
     */
    @Nullable
    private volatile Instant parExpireTime;


    /**
     * The operation of the current request, set during authorization and used by AuditFilter
     * Due to possible re-entry of Authorization multiple times [https://jira.oci.oraclecorp.com/browse/CASPER-2807]
     * The operation is mutable and will be set to the last operation provided to the authorizer
     */
    @Nullable
    private volatile CasperOperation operation;

    /**
     * The audit mode for objects, set in authorization, used by the AuditFilterHandler.
     */
    private volatile ObjectLevelAuditMode objAuditMode;

    /**
     * The new kmsKeyId for create bucket and update bucket
     */
    @Nullable
    private volatile String newkmsKeyId;

    /**
     * The old kmsKeyId for update bucket a bucket
     */
    @Nullable
    private volatile String oldKmsKeyId;


    @Nullable
    private volatile WebServerMonitoringMetric webServerMonitoringMetric;

    @Nullable
    private volatile String bucketOcid;

    /**
     * The requestor (caller) tenant ocid"
     */
    @Nullable
    private volatile String requestorTenantOcid;

    /**
     * The resource (destination) tenant ocid"
     */
    @Nullable
    private volatile String resourceTenantOcid;

    @Nullable
    private volatile byte[] tagSlug;

    @Nullable
    private volatile String tenantName;

    //used by Audit
    private volatile Map<String, Object> oldState;

    //used by Audit
    private volatile Map<String, Object> newState;

    //used by Audit
    private volatile String etag;

    //used by Audit
    private volatile BucketPublicAccessType bucketPublicAccessType;

    //used by Audit
    private volatile ArchivalState archivalState;

    //used by service logs
    @Nullable
    private volatile String bucketCreator;

    /**
     * A list of service logs that the backend adds to.
     * All the entries are then written to the physical file by
     * {@link com.oracle.pic.casper.webserver.api.logging.ServiceLogsHandler}
     */
    @Nullable
    private volatile List<ServiceLogEntry> serviceLogEntryList;

    private volatile List<ObjectEvent> objectEvents;

    /**
     * Whether the bucket has object events toggle enabled.
     */
    private volatile boolean objectEventsEnabled;

    /**
     * Information about the Public logging status of the bucket
     */
    @Nullable
    private volatile BucketLoggingStatus bucketLoggingStatus;

    /**
     * A stack of end handlers called at the end of processing each request once,
     * regardless of whether it succeeded or failed in any way.  The stack is implemented using Consumer.andThen().
     *
     * The boolean passed into the consumer method is whether the connection was abruptly closed or not.
     *
     * Our code should use this stack instead of calling HttpServerResponse.endHandler() or closeHandler() or
     * bodyEndHandler() etc.
     *
     * We call these handlers via HttpServerResponse.endHandler().
     */
    private Consumer<Boolean> endHandlers;

    /**
     * Whether the request has ended and its end handlers have run.
     */
    private boolean alreadyEnded;

    /**
     * Is this an Eagle request?
     */
    private final boolean isEagleRequest;

    public WSRequestContext(CommonRequestContext commonRequestContext, String realIP, CasperAPI casperAPI) {
        this(commonRequestContext, casperAPI, null, null, realIP, null, 0, null);
    }

    public WSRequestContext(CommonRequestContext commonContext,
                            CasperAPI casperAPI,
                            @Nullable String vcnID,
                            @Nullable String vcnDebugID,
                            String realIP,
                            @Nullable String debugIP,
                            int mss,
                            String targetHost) {
        this(commonContext, casperAPI, vcnID, vcnDebugID, realIP, debugIP, mss, targetHost, false);
    }

    public WSRequestContext(CommonRequestContext commonContext,
                            CasperAPI casperAPI,
                            @Nullable String vcnID,
                            @Nullable String vcnDebugID,
                            String realIP,
                            @Nullable String debugIP,
                            int mss,
                            String targetHost,
                            boolean isEagleRequest) {
        this.commonRequestContext = Preconditions.checkNotNull(commonContext);
        this.casperAPI = casperAPI;
        this.startTime = System.nanoTime();
        this.objAuditMode = ObjectLevelAuditMode.Disabled;
        this.vcnID = vcnID;
        this.vcnDebugID = vcnDebugID;
        this.realIP = Preconditions.checkNotNull(realIP);
        this.debugIP = debugIP;
        this.endHandlers = null;
        this.alreadyEnded = false;
        this.tagSlug = null;
        this.oldState = new HashMap<>();
        this.newState = new HashMap<>();
        this.objectEvents = new ArrayList<>();
        this.objectEventsEnabled = false;
        this.mss = mss;
        this.targetHost = targetHost;
        this.isEagleRequest = isEagleRequest;
    }

    public static MetricScope getMetricScope(RoutingContext context) {
        return getCommonRequestContext(context).getMetricScope();
    }

    /**
     * Return the {@link WSRequestContext} for the current routing context.
     *
     * It must be set by the {@link com.oracle.pic.casper.webserver.api.common.CommonHandler} or this
     * method will throw an exception
     *
     * @throws IllegalStateException if the WSReqeustContext was not set
     */
    public static WSRequestContext get(RoutingContext context) {
        WSRequestContext wsContext = safeGetWsRequestContext(context);
        if (wsContext == null) {
            throw new IllegalStateException("The '" + REQUEST_CONTEXT_KEY + "' is not set on the context object");
        }
        return wsContext;
    }

    /**
     * Safely access the WSRequestContext, returning null if none is found.
     *
     * Use this method if you can't be sure that {@link com.oracle.pic.casper.webserver.api.common.CommonHandler} has
     * handled the request.
     */
    @Nullable
    public static WSRequestContext safeGetWsRequestContext(RoutingContext context) {
        return context.get(REQUEST_CONTEXT_KEY);
    }

    public static CommonRequestContext getCommonRequestContext(RoutingContext context) {
        return get(context).getCommonRequestContext();
    }

    /**
     * Helper method to set a friendlier operation name for the trace other than the http method name
     */
    public static void setOperationName(String api, Class<? extends Handler<RoutingContext>> clazz,
                                        RoutingContext context, CasperOperation operation) {
        //turn a class like PutBucketHandler to PutBucket
        final String simpleClassName = clazz.getSimpleName().replace("Handler", "");
        getCommonRequestContext(context).getMetricScope()
                .getSpan().setOperationName(String.format("%s:%s", api, simpleClassName));
        WSRequestContext.get(context).setOperation(operation);
    }

    public CommonRequestContext getCommonRequestContext() {
        return commonRequestContext;
    }

    public long getStartTime() {
        return startTime;
    }

    public Optional<EmbargoContext> getVisa() {
        return Optional.ofNullable(visa);
    }

    public WSRequestContext setVisa(EmbargoContext visa) {
        this.visa = visa;
        return this;
    }

    /**
     * For the purposes of cross tenant requests, this will be the compartment ID that the resource being requested
     * lives in.  It will either be the compartment that the bucket lives in or it will be the tenant ID for account-
     * level requests.
     */
    public Optional<String> getCompartmentID() {
        return Optional.ofNullable(compartmentID);
    }

    /**
     * For the purposes of cross tenant requests, this must be the compartment ID that the resource being requested
     * lives in.  It must either be the compartment that the bucket lives in or the tenant ID for account-level
     * requests.
     */
    public WSRequestContext setCompartmentID(String compartmentID) {
        this.compartmentID = compartmentID;
        return this;
    }

    public Optional<Principal> getPrincipal() {
        return Optional.ofNullable(principal);
    }

    public WSRequestContext setPrincipal(Principal principal) {
        this.principal = principal;
        return this;
    }

    public Optional<User> getUser() {
        return Optional.ofNullable(user);
    }

    public WSRequestContext setUser(User user) {
        this.user = user;
        return this;
    }

    public Optional<String> getNamespaceName() {
        return Optional.ofNullable(namespaceName);
    }

    public WSRequestContext setNamespaceName(String namespaceName) {
        this.namespaceName = namespaceName;
        return this;
    }

    public Optional<String> getBucketName() {
        return Optional.ofNullable(bucketName);
    }

    public WSRequestContext setBucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    public Optional<String> getObjectName() {
        return Optional.ofNullable(objectName);
    }

    public boolean isEagleRequest() {
        return isEagleRequest;
    }

    public WSRequestContext setObjectName(String objectName) {
        this.objectName = objectName;
        return this;
    }

    public WSRequestContext setPARAccessType(PreAuthenticatedRequestMetadata.AccessType accessType) {
        this.parAccessType = accessType;
        return this;
    }

    public Optional<PreAuthenticatedRequestMetadata.AccessType> getPARAccessType() {
        return Optional.ofNullable(parAccessType);
    }

    public WSRequestContext setPARExpireTime(Instant expireTime) {
        this.parExpireTime = expireTime;
        return this;
    }

    public Optional<Instant> getPARExpireTime() {
        return Optional.ofNullable(parExpireTime);
    }

    public ObjectLevelAuditMode getObjectLevelAuditMode() {
        return objAuditMode;
    }

    public WSRequestContext setObjectLevelAuditMode(ObjectLevelAuditMode objAuditMode) {
        this.objAuditMode = objAuditMode;
        return this;
    }

    public Optional<String> getVcnID() {
        return Optional.ofNullable(vcnID);
    }

    public Optional<String> getVcnDebugID() {
        return Optional.ofNullable(vcnDebugID);
    }

    public String getRealIP() {
        return realIP;
    }

    public Optional<String> getDebugIP() {
        return Optional.ofNullable(debugIP);
    }

    public Optional<CasperOperation> getOperation() {
        return Optional.ofNullable(operation);
    }

    public WSRequestContext setOperation(@Nullable CasperOperation operation) {
        this.operation = operation;
        return this;
    }

    public int getMss() {
        return mss;
    }

    @Nullable
    public String getNewkmsKeyId() {
        return newkmsKeyId;
    }

    @Nullable
    public String getOldKmsKeyId() {
        return oldKmsKeyId;
    }

    public void setNewkmsKeyId(@Nullable String newkmsKeyId) {
        this.newkmsKeyId = newkmsKeyId;
    }

    public void setOldKmsKeyId(@Nullable String oldKmsKeyId) {
        this.oldKmsKeyId = oldKmsKeyId;
    }

    public Optional<WebServerMonitoringMetric> getWebServerMonitoringMetric() {
        return Optional.ofNullable(webServerMonitoringMetric);
    }

    public void setWebServerMonitoringMetric(@Nullable WebServerMonitoringMetric webServerMonitoringMetric) {
        this.webServerMonitoringMetric = webServerMonitoringMetric;
    }

    public Optional<String> getBucketOcid() {
        return Optional.ofNullable(bucketOcid);
    }

    public void setBucketOcid(@Nullable String bucketOcid) {
        this.bucketOcid = bucketOcid;
    }

    @Nullable
    public String getRequestorTenantOcid() {
        return requestorTenantOcid;
    }

    public void setRequestorTenantOcid(@Nullable String requestorTenantOcid) {
        this.requestorTenantOcid = requestorTenantOcid;
    }

    @Nullable
    public String getResourceTenantOcid() {
        return resourceTenantOcid;
    }

    public void setResourceTenantOcid(@Nullable String resourceTenantOcid) {
        this.resourceTenantOcid = resourceTenantOcid;
    }

    @Nullable
    public byte[] getTagSlug() {
        return tagSlug == null ? null : tagSlug.clone();
    }

    public void setTagSlug(@Nullable byte[] tagSlug) {
        this.tagSlug = tagSlug == null ? null : tagSlug.clone();
    }

    @Nullable
    public String getTenantName() {
        return tenantName;
    }

    public void setTenantName(@Nullable String tenantName) {
        this.tenantName = tenantName;
    }

    public Map<String, Object> getOldState() {
        return oldState;
    }

    public Map<String, Object> getNewState() {
        return newState;
    }

    public void addOldState(String key, Object value) {
        oldState.put(key, value);
    }

    public void addNewState(String key, Object value) {
        newState.put(key, value);
    }

    public Optional<String> getEtag() {
        return Optional.ofNullable(etag);
    }

    public void setEtag(String etag) {
        this.etag = etag;
    }

    public Optional<BucketPublicAccessType> getBucketPublicAccessType() {
        return Optional.ofNullable(bucketPublicAccessType);
    }

    public void setBucketPublicAccessType(BucketPublicAccessType bucketPublicAccessType) {
        this.bucketPublicAccessType = bucketPublicAccessType;
    }

    public Optional<ArchivalState> getArchivalState() {
        return Optional.ofNullable(archivalState);
    }

    public void setArchivalState(ArchivalState archivalState) {
        this.archivalState = archivalState;
    }

    public Optional<String> getResourceName() {
        return objectName != null ? Optional.of(objectName) : Optional.ofNullable(bucketName);
    }

    public List<ServiceLogEntry> getServiceLogEntryList() {
        return serviceLogEntryList == null ? Collections.EMPTY_LIST : serviceLogEntryList;
    }

    public void addServiceLogEntry(ServiceLogEntry serviceLogEntry) {
        if (serviceLogEntryList == null) {
            serviceLogEntryList = new ArrayList<>();
        }
        this.serviceLogEntryList.add(serviceLogEntry);
    }

    public List<ObjectEvent> getObjectEvents() {
        return objectEvents;
    }

    public void addObjectEvent(ObjectEvent event) {
        objectEvents.add(event);
    }

    public boolean isObjectEventsEnabled() {
        return objectEventsEnabled;
    }

    public void setObjectEventsEnabled(boolean objectEventsEnabled) {
        this.objectEventsEnabled = objectEventsEnabled;
    }

    @Nullable
    public Optional<String> getBucketCreator() {
        return Optional.ofNullable(bucketCreator);
    }

    public void setBucketCreator(@Nullable String bucketCreator) {
        this.bucketCreator = bucketCreator;
    }

    public CasperAPI getCasperAPI() {
        return casperAPI;
    }

    @Nullable
    public Optional<Compartment> getCompartment() {
        return Optional.ofNullable(compartment);
    }

    public void setCompartment(@Nullable Compartment compartment) {
        this.compartment = compartment;
    }

    /**
     * Push an end handler to the top of the stack, meaning at the end of the request, it will be called before any
     * handlers already in the stack.
     *
     * The boolean passed into the consumer method is whether the connection was closed, allowing the handler to act
     * accordingly.
     *
     * Exceptions thrown by the handler will not cascade!  This is so that the rest of the end handlers will continue
     * be called for proper clean up.
     */
    public synchronized void pushEndHandler(Consumer<Boolean> handler) {
        final Consumer<Boolean> exceptionCatchingHandler = connectionClosed -> {
            try {
                handler.accept(connectionClosed);
            } catch (Exception e) {
                LOG.error("End handler threw exception, connectionClosed={}", connectionClosed, e);
            }
        };
        endHandlers = endHandlers == null ? exceptionCatchingHandler : exceptionCatchingHandler.andThen(endHandlers);
    }

    /**
     * This is called in the CommonHandler, once at the end of the request.  Do not call this method from anywhere else.
     */
    public synchronized void runEndHandler(boolean connectionClosed) {
        if (alreadyEnded) {
            LOG.error("Request has already ended!");
        } else {
            if (endHandlers != null) {
                endHandlers.accept(connectionClosed);
            }
            alreadyEnded = true;
        }
    }

    public Optional<BucketLoggingStatus> getBucketLoggingStatus() {
        return Optional.ofNullable(bucketLoggingStatus);
    }

    public void setBucketLoggingStatus(BucketLoggingStatus bucketLoggingStatus) {
        this.bucketLoggingStatus = bucketLoggingStatus;
    }


    public String getTargetHost() {
        return targetHost;
    }
}
