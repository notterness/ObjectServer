package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.mds.tenant.MdsNamespace;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.ETagType;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.Checksum;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.codec.DecoderException;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static javax.measure.unit.NonSI.BYTE;

/**
 * Writes an object.
 *
 * Swift implementation notes:
 *
 * Unlike GET and HEAD, PUT does not return any object-specific headers on 412 responses, even if the object does
 * already exist.
 * On 201 responses, Swift does return the object's ETag, Last-Modified, and Content-Type headers, but not the
 * X-Timestamp or custom user metadata headers.  We return them all because it's easier and safer to.
 * On error responses, Swift returns the Content-Type of the error message (text/plain or text/html, depending on the
 * error / error message), and we mostly follow suit.
 * Swift ignores the If-Match header; sending it does *not* result in 400.  We return 400.
 * Swift only allows wildcards for the If-None-Match header, returning 400 if you pass in anything else.  This matches
 * the casper v1 and v2 behavior, allowing clients to request object creation explicitly rather than update, so it's
 * easy for us to support and we do.
 * PUTing an object in Swift always creates a new metadata map for it (i.e. making a PUT request without any metadata
 * keys to an existing object with metadata will cause the object's content to be updated and its metadata to be
 * completely lost).  This is the simplest thing for us to do too, so we fully support Swift.
 * Swift ignores Content-MD5 headers passed to it; it does not validate them against the body.  We do validate for our
 * v1 and v2 APIs, but for Swift we follow suit and ignore the header.
 *
 * http://developer.openstack.org/api-ref-objectstorage-v1.html#createOrReplaceObject
 */
public class PutObjectHandler extends CompletableHandler {
    private final TrafficController controller;
    private final Authenticator authenticator;
    private final PutObjectBackend backend;
    private final TenantBackend tenantBackend;
    private final SwiftResponseWriter responseWriter;
    private final WebServerConfiguration webServerConfiguration;
    private final EmbargoV3 embargoV3;

    public PutObjectHandler(TrafficController controller,
                            Authenticator authenticator,
                            PutObjectBackend backend,
                            TenantBackend tenantBackend,
                            SwiftResponseWriter responseWriter,
                            WebServerConfiguration webServerConfiguration,
                            EmbargoV3 embargoV3) {
        this.controller = controller;
        this.authenticator = authenticator;
        this.backend = backend;
        this.tenantBackend = tenantBackend;
        this.responseWriter = responseWriter;
        this.webServerConfiguration = webServerConfiguration;
        this.embargoV3 = embargoV3;
    }

    /**
     * Curry up a validation callback for use by Backend#createOrOverwriteObject.
     *
     * This method and {@link #validate(Optional, HttpServerRequest, HttpServerResponse)} exist only to make the
     * CF chain below more readable.
     */
    private Consumer<Optional<ObjectMetadata>> curryValidationCallback(HttpServerRequest request,
                                                                       HttpServerResponse response) {
        return (optCurObjMeta) -> validate(optCurObjMeta, request, response);
    }

    /**
     * Validate a request after hitting the DB to see what was already there for that account, container, and object.
     *
     * This method and {@link #curryValidationCallback(HttpServerRequest, HttpServerResponse)} exist only to make the
     * CF chain below more readable.  Please excuse the Optional parameter.
     */
    private void validate(Optional<ObjectMetadata> optCurObjMeta, HttpServerRequest request,
                                    HttpServerResponse response) {
        HttpMatchHelpers.checkConditionalHeaders(request,
            optCurObjMeta.map(metadata -> metadata.getChecksum().getHex()).orElse(null));
        HttpContentHelpers.write100Continue(request, response);
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.SWIFT_PUT_OBJECT_BUNDLE);
        WSRequestContext.setOperationName("swift", getClass(), context, CasperOperation.PUT_OBJECT);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        final MetricScope scope = commonContext.getMetricScope();

        // Vert.x reads the request in the background by default, but we want to stream it through a state machine, so
        // we tell Vert.x to pause the reading. The request is resumed in the PutStateMachine.
        request.pause();

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.PUTOBJECT_REQUEST_COUNT);

        final String namespace = SwiftHttpPathHelpers.getNamespace(request, wsRequestContext);
        final String containerName = SwiftHttpPathHelpers.getContainerName(request, wsRequestContext);
        final String objectName = SwiftHttpPathHelpers.getObjectName(request, wsRequestContext);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.Swift)
            .setOperation(CasperOperation.PUT_OBJECT)
            .setNamespace(namespace)
            .setBucket(containerName)
            .setObject(objectName)
            .build();
        embargoV3.enter(embargoOperation);

        SwiftHttpContentHelpers.negotiateStorageObjectContent(
                request, webServerConfiguration.getMaxObjectSize().longValue(BYTE));
        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.NO,
                HttpMatchHelpers.IfNoneMatchAllowed.STAR_ONLY);

        final String expectedMD5 = SwiftHttpHeaderHelpers.getContentMD5(request);
        final ObjectMetadata objMeta = SwiftHttpContentHelpers.readObjectMetadata(
                request, namespace, containerName, objectName);

        final Consumer<Optional<ObjectMetadata>> validationCallback = curryValidationCallback(request, response);

        // Acquire can throw an exception(with err code of 503 or 429) if the request couldnt be accepted
        controller.acquire(objMeta.getNamespace(), TrafficRecorder.RequestType.PutObject, objMeta.getSizeInBytes());

        return VertxUtil.runAsync(() -> {
            final String tenantOcid = tenantBackend.getNamespace(context, scope, new NamespaceKey(Api.V2, namespace))
                    .map(MdsNamespace::getTenantOcid)
                    .orElse(null);
            return authenticator.authenticateSwiftOrCavageForPutObject(context, namespace, tenantOcid);
        }).thenCompose(authenticationInfo -> backend.createOrOverwriteObject(context,
                authenticationInfo,
                objMeta,
                validationCallback,
                Api.V2,
                WebServerMetrics.SWIFT_PUT_OBJECT_BUNDLE,
                expectedMD5,
                null,
                getIfMatchEtag(request),
                getIfNoneMatchEtag(request),
                ETagType.MD5))
            .handle((val, ex) -> SwiftHttpExceptionHelpers.handle(context, val, ex))
            .thenAccept(om -> SwiftHttpHeaderHelpers.writeObjectHeaders(response, om))
            .thenAccept(om -> responseWriter.writeCreatedResponse(context));
    }

    /**
     * Swift uses hex-encoded MD5s as object ETags, so here we need to convert from Swift's representation (hex)
     * to our storage representation (base64). However, we do allow matching '*', so if '*' is provided,
     * do not do the conversion.
     */
    private String getIfMatchEtag(HttpServerRequest request) {
        try {
            final String ifMatchHeader = request.getHeader(HttpHeaders.IF_MATCH);
            if (ifMatchHeader == null || ifMatchHeader.equals("*")) {
                return ifMatchHeader;
            } else {
                return Checksum.fromHex(ifMatchHeader).getBase64();
            }
        } catch (DecoderException e) {
            throw new HttpException(V2ErrorCode.IF_MATCH_FAILED, "If-Match failed.", request.path());
        }
    }

    /**
     * For ifNoneMatch, we only do the etag check when it is '*'.
     * If ifNoneMatch is null or not '*', we don't do any etag matching, so we don't need to do any conversion.
     */
    private String getIfNoneMatchEtag(HttpServerRequest request) {
        // For ifNoneMatch, we only do the etag check when it is '*'.
        // If ifNoneMatch is null or not '*', we don't do any etag matching, so we don't need to do any conversion.
        final String ifNoneMatchHeader = request.getHeader(HttpHeaders.IF_NONE_MATCH);
        if (ifNoneMatchHeader != null && ifNoneMatchHeader.equals("*")) {
            return ifNoneMatchHeader;
        } else {
            return null;
        }
    }
}
