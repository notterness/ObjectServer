package com.oracle.pic.casper.webserver.api.s3;

import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.NotFoundException;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.vertx.HttpServerUtil;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.ETagType;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.S3AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.NotImplementedException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static javax.measure.unit.NonSI.BYTE;

/**
 * Vert.x HTTP handler for S3 Api to create or overwrite an object.
 */
public class PutObjectHandler extends CompletableHandler {

    private final TrafficController controller;
    private final S3AsyncAuthenticator authenticator;
    private final PutObjectBackend backend;
    private final WebServerConfiguration webServerConf;
    private final EmbargoV3 embargoV3;

    public PutObjectHandler(TrafficController controller,
                            S3AsyncAuthenticator authenticator,
                            PutObjectBackend backend,
                            WebServerConfiguration webServerConf,
                            EmbargoV3 embargoV3) {
        this.controller = controller;
        this.authenticator = authenticator;
        this.backend = backend;
        this.webServerConf = webServerConf;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_PUT_OBJECT_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.PUT_OBJECT);
        final HttpServerRequest request = context.request();
        final String firstParam = HttpServerUtil.getRoutingParam(request);
        if (firstParam != null) {
            if (S3Api.UNSUPPORTED_CONFIGURATION_PARAMS.contains(firstParam)) {
                throw new NotImplementedException(String.format(
                        "S3 Set Object %s operation is not supported.", firstParam));
            }
            throw new NotFoundException("Not Found");
        }

        final HttpServerResponse response = context.response();

        // Vert.x reads the request in the background by default, but we want to stream it through a state machine, so
        // we tell Vert.x to pause the reading. The request is resumed in the PutStateMachine.
        request.pause();

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.PUTOBJECT_REQUEST_COUNT);
        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);
        final String objectName = S3HttpHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.PUT_OBJECT)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        if (request.getHeader(S3Api.COPY_SOURCE_HEADER) != null) {
            throw new NotImplementedException("S3 Copy Object operation is not supported.");
        }

        HttpContentHelpers.negotiateStorageObjectContent(request, 0, webServerConf.getMaxObjectSize().longValue(BYTE));

        final Map<String, String> metadata = HttpHeaderHelpers.getUserMetadataHeaders(
                request, HttpHeaderHelpers.UserMetadataPrefix.S3);
        final String expectedMD5 = ChecksumHelper.getContentMD5Header(request).orElse(null);
        final ObjectMetadata objMeta = HttpContentHelpers.readObjectMetadata(
                request, namespace, bucketName, objectName, metadata);

        // Acquire can throw an exception(with err code of 503 or 429) if the request couldnt be accepted
        controller.acquire(objMeta.getNamespace(), TrafficRecorder.RequestType.PutObject, objMeta.getSizeInBytes());

        return authenticator.authenticate(context, contentSha256)
            .thenCompose(authInfo -> {
                Digest expectedDigest = null;
                if (S3HttpHelpers.isSignedPayload(contentSha256)) {
                    try {
                        String base64Sha256 = ChecksumHelper.hexToBase64(contentSha256);
                        expectedDigest = Digest.fromBase64Encoded(DigestAlgorithm.SHA256, base64Sha256);
                    } catch (Exception e) {
                        throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT,
                                "x-amz-content-sha256 must be UNSIGNED-PAYLOAD or a valid sha256 value.",
                                request.path());
                    }
                }

                return backend.createOrOverwriteObject(
                        context,
                        authInfo,
                        objMeta,
                        curObjMeta -> HttpContentHelpers.write100Continue(request, response),
                        Api.V2,
                        WebServerMetrics.S3_PUT_OBJECT_BUNDLE,
                        expectedMD5,
                        expectedDigest,
                        null,
                        null,
                        ETagType.MD5);

            })
            .thenAccept(newObjMeta -> {
                newObjMeta.getChecksum().addHeaderToResponseS3(response);
                response.end();
            })
            .exceptionally(thrown -> {
                throw S3HttpException.rewrite(context, thrown);
            });
    }
}
