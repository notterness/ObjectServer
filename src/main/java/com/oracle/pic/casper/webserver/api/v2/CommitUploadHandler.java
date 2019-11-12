package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.oracle.pic.casper.common.config.MultipartLimit;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.model.FinishUploadRequest;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.UploadIdentifier;
import com.oracle.pic.casper.webserver.api.model.exceptions.UnauthorizedHeaderException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Base64;
import java.util.List;

/**
 * Vert.x HTTP handler to commit an upload.
 */
public class CommitUploadHandler extends SyncBodyHandler {

    /* Each part detail takes about 64 characters:
     * {"partNum": 1000, "etag": "...(32 chars)"},
     * plus the rest of the body {"partsToCommit": [...], "partsToExclude": [...]}.
     *
     * Bumped up to 70 * maximum number of parts to be safe.
     */
    private static final long MAX_COMMIT_UPLOAD_CONTENT_BYTES = 70L * MultipartLimit.MAX_UPLOAD_PART_NUM;
    private final Authenticator authenticator;
    private final Backend backend;
    private final ObjectMapper mapper;
    private final EmbargoV3 embargoV3;
    private List<String> servicePrincipals;

    public CommitUploadHandler(Authenticator authenticator,
                               Backend backend,
                               ObjectMapper mapper,
                               CountingHandler.MaximumContentLength maximumContentLength,
                               List<String> servicePrincipals,
                               EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.servicePrincipals = servicePrincipals;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new HttpException(
                V2ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {
        HttpMatchHelpers.validateConditionalHeaders(
                context.request(),
                HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.STAR_ONLY);

        HttpContentHelpers.negotiateApplicationJsonContent(context.request(), MAX_COMMIT_UPLOAD_CONTENT_BYTES);
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context,
            CasperOperation.COMMIT_MULTIPART_UPLOAD);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_COMMIT_UPLOAD_BUNDLE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        final String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.COMMIT_MULTIPART_UPLOAD)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoV3Operation);

        final String uploadId = HttpPathQueryHelpers.getUploadId(request);
        final UploadIdentifier uploadIdentifier = new UploadIdentifier(namespace, bucketName, objectName, uploadId);
        final String ifMatch = request.getHeader(HttpHeaders.IF_MATCH);
        final String ifNoneMatch = request.getHeader(HttpHeaders.IF_NONE_MATCH);
        final String md5Override = request.getHeader(CasperApiV2.MD5_OVERRIDE_HEADER);
        final String etagOverride = request.getHeader(CasperApiV2.ETAG_OVERRIDE_HEADER);
        final String partCountOverrideHeader = request.getHeader(CasperApiV2.PART_COUNT_OVERRIDE_HEADER);
        final Integer partCountOverride;
        try {
            partCountOverride = partCountOverrideHeader == null ? null : Integer.parseInt(partCountOverrideHeader);
        } catch (NumberFormatException e) {
            throw new BadRequestException("Cannot parse partCountOverrideHeader" + partCountOverrideHeader, e);
        }
        final String etagRound = request.getHeader(CasperApiV2.POLICY_ROUND_HEADER);
        // annotate secret header values for logging
        final MetricScope metricScope = WSRequestContext.getMetricScope(context);
        metricScope.annotate(CasperApiV2.MD5_OVERRIDE_HEADER, md5Override);
        metricScope.annotate(CasperApiV2.ETAG_OVERRIDE_HEADER, etagOverride);
        metricScope.annotate(CasperApiV2.POLICY_ROUND_HEADER, etagRound);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(
                    context, Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(bytes).asBytes()));
            // Only specific service principals can use secret headers
            if ((md5Override != null || etagOverride != null || partCountOverride != null || etagRound != null) &&
                    !(authInfo.getServicePrincipal().isPresent() &&
                            servicePrincipals.contains(authInfo.getServicePrincipal().get().getTenantId()))) {
                throw new UnauthorizedHeaderException();
            }
            final FinishUploadRequest commit = HttpContentHelpers.readCommitUploadContent(
                    request, uploadIdentifier, mapper, bytes, ifMatch, ifNoneMatch);
            final ObjectMetadata objMeta = backend.finishMultipartUpload(context, authInfo, commit, md5Override,
                    etagOverride, partCountOverride);
            response.putHeader(HttpHeaders.ETAG, objMeta.getETag())
                    .putHeader(HttpHeaders.LAST_MODIFIED, DateUtil.httpFormattedDate(objMeta.getModificationTime()));
            objMeta.getChecksum().addHeaderToResponseV2Put(response);
            response.end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
