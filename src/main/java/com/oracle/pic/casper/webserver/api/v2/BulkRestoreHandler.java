package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.glob.GlobPattern;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.WorkRequestBackend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.BulkRestoreRequestJson;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.casper.workrequest.bulkrestore.BulkRestoreRequestDetail;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handler to accept a bulk restore request, put it into the work request queue for processing and
 * return the work request id in header.
 */
public class BulkRestoreHandler extends SyncBodyHandler {

    private final Authenticator authenticator;
    private final ObjectMapper mapper;
    private final WorkRequestBackend backend;
    private final EmbargoV3 embargoV3;
    private static final String WORK_REQUEST_ID_HEADER = "opc-work-request-id";
    private static final Integer DEFAULT_RESTORE_DURATION_IN_HOURS = 24;
    private static final Integer MIN_RESTORE_DURATION_IN_HOURS = 1;
    private static final Integer MAX_RESTORE_DURATION_IN_HOURS = 240;
    private static final Integer MAX_GLOBS_LIST_SIZE = 20;


    public BulkRestoreHandler(Authenticator authenticator,
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
                V2ErrorCode.fromStatusCode(statusCode), "One or more restore requests failed",
                context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {

    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.CREATE_BULK_RESTORE_REQUEST);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_CREATE_BULK_RESTORE_BUNDLE);
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucket = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.CREATE_BULK_RESTORE_REQUEST)
            .setNamespace(namespace)
            .setBucket(bucket)
            .build();
        embargoV3.enter(embargoV3Operation);

        final AuthenticationInfo authInfo = authenticator.authenticate(context,
                Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));
        final BulkRestoreRequestJson bulkRestoreRequestJson
                = HttpContentHelpers.readBulkRestoreRequestJson(request, mapper, bytes);

        final String requestId;
        try {
            validateBulkRestoreRequestJson(bulkRestoreRequestJson, request);

            final BulkRestoreRequestDetail bulkRestoreRequestDetail = BulkRestoreRequestDetail.builder()
                    .namespace(namespace)
                    .bucket(bucket)
                    .inclusionPatterns(bulkRestoreRequestJson.getInclusionPatterns())
                    .exclusionPatterns(bulkRestoreRequestJson.getExclusionPatterns())
                    .restoreDurationInHours(bulkRestoreRequestJson.getHours() == null ?
                            DEFAULT_RESTORE_DURATION_IN_HOURS : bulkRestoreRequestJson.getHours())
                    .build();


            requestId = backend.createBulkRestoreRequest(context, authInfo, bulkRestoreRequestDetail);
        } catch (Exception e) {
            throw HttpException.rewrite(request, e);
        }
        response.putHeader(WORK_REQUEST_ID_HEADER, requestId).setStatusCode(HttpResponseStatus.ACCEPTED).end();
    }

    private void validateBulkRestoreRequestJson(BulkRestoreRequestJson bulkRestoreRequestJson,
                                                HttpServerRequest request) {
        Integer restoreDurationInHours = bulkRestoreRequestJson.getHours();
        List<String> inclusionPatterns = bulkRestoreRequestJson.getInclusionPatterns();
        List<String> exclusionPatterns = bulkRestoreRequestJson.getExclusionPatterns();

        if (restoreDurationInHours != null) {
            if (restoreDurationInHours < MIN_RESTORE_DURATION_IN_HOURS
                    || restoreDurationInHours > MAX_RESTORE_DURATION_IN_HOURS) {
                throw new HttpException(V2ErrorCode.INVALID_RESTORE_HOURS,
                        "Restore duration in hours must be between " + MIN_RESTORE_DURATION_IN_HOURS + " and "
                                + MAX_RESTORE_DURATION_IN_HOURS + ".", request.path());
            }
        }

        if (inclusionPatterns.size() > MAX_GLOBS_LIST_SIZE) {
            throw new HttpException(V2ErrorCode.INVALID_JSON,
                    "The number of inclusion patterns must be less than or equal to " + MAX_GLOBS_LIST_SIZE + ".",
                    request.path());
        }

        List<String> invalidInclusionPatterns = inclusionPatterns.stream()
                    .filter(p -> !GlobPattern.isValidGlob(p)).collect(Collectors.toList());

        if (invalidInclusionPatterns != null && !invalidInclusionPatterns.isEmpty()) {
            throw new HttpException(V2ErrorCode.INVALID_GLOB,
                    "The following inclusion patterns are invalid " + invalidInclusionPatterns.toString(),
                    request.path());
        }

        if (null != exclusionPatterns) {
            if (exclusionPatterns.size() > MAX_GLOBS_LIST_SIZE) {
                throw new HttpException(V2ErrorCode.INVALID_JSON,
                        "The number of exclusion patterns must be less than or equal to " + MAX_GLOBS_LIST_SIZE + ".",
                        request.path());
            }

            List<String> invalidExclusionPatterns = exclusionPatterns.stream()
                    .filter(p -> !GlobPattern.isValidGlob(p)).collect(Collectors.toList());

            if (!invalidExclusionPatterns.isEmpty()) {
                throw new HttpException(V2ErrorCode.INVALID_GLOB,
                        "The following exclusion patterns are invalid " + invalidExclusionPatterns.toString(),
                        request.path());
            }
        }
    }
}
