package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.CrossRegionUtils;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.WorkRequestBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.NamespaceCaseWhiteList;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.CopyRequestJson;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.casper.workrequest.copy.CopyRequestDetail;
import com.oracle.pic.commons.util.Region;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;
import java.util.Map;

/**
 * Handler to accept a cross region copy request, put it into the work request queue for processing and
 * return the work request id in header.
 */
public class CopyObjectHandler extends SyncBodyHandler {

    private final Authenticator authenticator;
    private final ObjectMapper mapper;
    private final WorkRequestBackend backend;
    private final EmbargoV3 embargoV3;
    private static final String WORK_REQUEST_ID_HEADER = "opc-work-request-id";

    public CopyObjectHandler(Authenticator authenticator,
                             ObjectMapper mapper,
                             WorkRequestBackend backend,
                             CountingHandler.MaximumContentLength maximumContentLength,
                             EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.mapper = mapper;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {

    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.CREATE_COPY_REQUEST);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_CREATE_COPY_BUNDLE);
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.COPYOBJECT_REQUEST_COUNT);

        final String sourceNamespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String sourceBucket = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final AuthenticationInfo authInfo = authenticator.authenticate(context,
                Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));
        final CopyRequestJson copyRequestJson = HttpContentHelpers.readCopyRequestJson(request, mapper, bytes);
        final String sourceObject = copyRequestJson.getSourceObjectName();
        final String destNamespace = NamespaceCaseWhiteList.lowercaseNamespace(Api.V2,
                copyRequestJson.getDestinationNamespace());

        EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.V2)
                .setOperation(CasperOperation.CREATE_COPY_REQUEST)
                .setNamespace(sourceNamespace)
                .setBucket(sourceBucket)
                .setObject(sourceObject)
                .build();
        embargoV3.enter(embargoV3Operation);

        // For supporting cross-region copy in R1 (from unstable to stable)
        // To identify if it is a intra-region (R1_UNSTABLE to R1_UNSTABLE) or inter-region (R1_UNSTABLE to R1_STABLE)
        final String requestDestRegion = copyRequestJson.getDestinationRegion();
        final ConfigRegion destConfigRegion = CrossRegionUtils.getConfigRegion(requestDestRegion);
        final Region destRegion;
        if (destConfigRegion != null) {
            destRegion = destConfigRegion.toRegion();
        } else {
            throw new HttpException(V2ErrorCode.INVALID_REGION, "Unknown region " +
                    requestDestRegion, request.path());
        }
        if (destRegion.getRealm() != ConfigRegion.fromSystemProperty().toRegion().getRealm()) {
            throw new HttpException(V2ErrorCode.INVALID_REGION, "Invalid region " +
                    copyRequestJson.getDestinationRegion() + ", copy between realms is not allowed", request.path());
        }
        final CopyRequestDetail copyRequestDetail = CopyRequestDetail.builder()
                .sourceNamespace(sourceNamespace)
                .sourceBucket(sourceBucket)
                .sourceObject(copyRequestJson.getSourceObjectName())
                .sourceIfMatch(copyRequestJson.getSourceObjectIfMatchETag())
                .destIfMatch(copyRequestJson.getDestinationObjectIfMatchETag())
                .destIfNoneMatch(copyRequestJson.getDestinationObjectIfNoneMatchETag())
                .destConfigRegion(destConfigRegion)
                .destNamespace(destNamespace)
                .destBucket(copyRequestJson.getDestinationBucket())
                .destObject(copyRequestJson.getDestinationObjectName())
                .build();
        //This strips the prefix out of user metadata in request body ("opc-meta-key" -> "key").
        //We use HttpContentHelpers.checkAndRemoveMetadataPrefix instead of HttpHeaderHelpers.getUserMetadata
        //because the header method grabs content metadata as well.
        final Map<String, String> metadata = HttpContentHelpers.checkAndRemoveMetadataPrefix(request,
                copyRequestJson.getDestinationObjectMetadata());
        final boolean isMissingMetadata = metadata != null && metadata.isEmpty();
        final String requestId;
        try {
            requestId = backend.createCopyRequest(
                    context, authInfo, copyRequestDetail, isMissingMetadata, metadata, sourceNamespace);
        } catch (Exception e) {
            throw HttpException.rewrite(request, e);
        }
        response.putHeader(WORK_REQUEST_ID_HEADER, requestId).setStatusCode(HttpResponseStatus.ACCEPTED).end();
    }
}
