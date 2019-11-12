package com.oracle.pic.casper.webserver.api.model.logging;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.http.HttpMethod;

import java.util.Date;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ServiceLogContent {

    private final String bucketName;

    private final String objectName;

    private final String requestResourcePath;

    private final String compartmentId;

    private final String compartmentName;

    private final String tenantId;

    private final String tenantName;

    private final String namespaceName;

    private final String errorCode;

    @JsonProperty("isPar")
    private final boolean isPar;

    private final HttpMethod requestAction;

    private final int statusCode;

    private final String clientIpAddress;

    private final String authenticationType;

    private final Date startTime;

    private Date endTime;

    private final String bucketCreator;

    private final String userAgent;

    private final String principalId;

    private final String vcnId;

    private final String region;

    private final String apiType;

    private final String credentials;

    private final String opcRequestId;

    private final String eTag;

    private final String operation;

    public String geteTag() {
        return eTag;
    }

    public String getOpcRequestId() {
        return opcRequestId;
    }

    public HttpMethod getRequestAction() {
        return requestAction;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getRequestResourcePath() {
        return requestResourcePath;
    }

    public String getCompartmentId() {
        return compartmentId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public String getErrorCode() {
        return errorCode;
    }

    @JsonProperty("isPar")
    public boolean isPar() {
        return isPar;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getClientIpAddress() {
        return clientIpAddress;
    }

    public String getAuthenticationType() {
        return authenticationType;
    }

    public Date getStartTime() {
        return startTime == null ? null : new Date(startTime.getTime());
    }

    public Date getEndTime() {
        return endTime == null ? null : new Date(endTime.getTime());
    }

    public void updateEndTime(long endTime) {
        this.endTime = new Date(endTime);
    }

    public String getCompartmentName() {
        return compartmentName;
    }

    public String getTenantName() {
        return tenantName;
    }

    public String getBucketCreator() {
        return bucketCreator;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public String getPrincipalId() {
        return principalId;
    }


    public String getVcnId() {
        return vcnId;
    }

    public String getRegion() {
        return region;
    }

    public String getApiType() {
        return apiType;
    }

    public String getCredentials() {
        return credentials;
    }

    public ServiceLogContent(String bucketName, String objectName, String requestResourcePath, String compartmentId,
                             String compartmentName, String tenantId, String tenantName, String namespaceName,
                             String errorCode, boolean isPar, HttpMethod requestAction, int statusCode,
                             String clientIpAddress, String authenticationType, long startTime, String bucketCreator,
                             String userAgent, String principalId, String vcnId, String region, String apiType,
                             String credentials, String opcRequestId, String eTag, String operation) {
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.requestResourcePath = requestResourcePath;
        this.compartmentId = compartmentId;
        this.compartmentName = compartmentName;
        this.tenantId = tenantId;
        this.tenantName = tenantName;
        this.namespaceName = namespaceName;
        this.errorCode = errorCode;
        this.isPar = isPar;
        this.requestAction = requestAction;
        this.statusCode = statusCode;
        this.clientIpAddress = clientIpAddress;
        this.authenticationType = authenticationType;
        this.startTime = new Date(startTime);
        this.bucketCreator = bucketCreator;
        this.userAgent = userAgent;
        this.principalId = principalId;
        this.vcnId = vcnId;
        this.region = region;
        this.apiType = apiType;
        this.credentials = credentials;
        this.opcRequestId = opcRequestId;
        this.eTag = eTag;
        this.operation = operation;
    }


    public static final class ServiceLogContentBuilder {
        private String bucketName;
        private String objectName;
        private String requestResourcePath;
        private String compartmentId;
        private String compartmentName;
        private String tenantId;
        private String tenantName;
        private String namespaceName;
        private String errorCode;
        private boolean isPar;
        private HttpMethod requestAction;
        private int statusCode;
        private String clientIpAddress;
        private String authenticationType;
        private long startTime;
        private String bucketCreator;
        private String userAgent;
        private String principalId;
        private String vcnId;
        private String region;
        private String apiType;
        private String credentials;
        private String opcRequestId;
        private String eTag;
        private String operation;

        private ServiceLogContentBuilder() {
        }

        public static ServiceLogContentBuilder aServiceLogContent() {
            return new ServiceLogContentBuilder();
        }

        public ServiceLogContentBuilder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public ServiceLogContentBuilder objectName(String objectName) {
            this.objectName = objectName;
            return this;
        }

        public ServiceLogContentBuilder requestResourcePath(String requestResourcePath) {
            this.requestResourcePath = requestResourcePath;
            return this;
        }

        public ServiceLogContentBuilder compartmentId(String compartmentId) {
            this.compartmentId = compartmentId;
            return this;
        }

        public ServiceLogContentBuilder compartmentName(String compartmentName) {
            this.compartmentName = compartmentName;
            return this;
        }

        public ServiceLogContentBuilder tenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public ServiceLogContentBuilder tenantName(String tenantName) {
            this.tenantName = tenantName;
            return this;
        }

        public ServiceLogContentBuilder namespaceName(String namespaceName) {
            this.namespaceName = namespaceName;
            return this;
        }

        public ServiceLogContentBuilder errorCode(String errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public ServiceLogContentBuilder isPar(boolean isPar) {
            this.isPar = isPar;
            return this;
        }

        public ServiceLogContentBuilder requestAction(HttpMethod requestAction) {
            this.requestAction = requestAction;
            return this;
        }

        public ServiceLogContentBuilder statusCode(int statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public ServiceLogContentBuilder clientIpAddress(String clientIpAddress) {
            this.clientIpAddress = clientIpAddress;
            return this;
        }

        public ServiceLogContentBuilder authenticationType(String authenticationType) {
            this.authenticationType = authenticationType;
            return this;
        }

        public ServiceLogContentBuilder startTime(long startTime) {
            this.startTime = startTime;
            return this;
        }


        public ServiceLogContentBuilder bucketCreator(String bucketCreator) {
            this.bucketCreator = bucketCreator;
            return this;
        }

        public ServiceLogContentBuilder userAgent(String userAgent) {
            this.userAgent = userAgent;
            return this;
        }

        public ServiceLogContentBuilder principalId(String principalId) {
            this.principalId = principalId;
            return this;
        }

        public ServiceLogContentBuilder vcnId(String vcnId) {
            this.vcnId = vcnId;
            return this;
        }

        public ServiceLogContentBuilder region(String region) {
            this.region = region;
            return this;
        }

        public ServiceLogContentBuilder apiType(String apiType) {
            this.apiType = apiType;
            return this;
        }

        public ServiceLogContentBuilder credentials(String credentials) {
            this.credentials = credentials;
            return this;
        }

        public ServiceLogContentBuilder opcRequestId(String opcRequestId) {
            this.opcRequestId = opcRequestId;
            return this;
        }

        public ServiceLogContentBuilder eTag(String eTag) {
            this.eTag = eTag;
            return this;
        }

        public ServiceLogContentBuilder operation(String operation) {
            this.operation = operation;
            return this;
        }

        public ServiceLogContent build() {
            ServiceLogContent serviceLogContent = new ServiceLogContent(bucketName, objectName, requestResourcePath,
                    compartmentId, compartmentName, tenantId, tenantName, namespaceName, errorCode, isPar,
                    requestAction, statusCode, clientIpAddress, authenticationType, startTime, bucketCreator,
                    userAgent, principalId, vcnId, region, apiType, credentials, opcRequestId, eTag, operation);
            return serviceLogContent;
        }
    }
}
