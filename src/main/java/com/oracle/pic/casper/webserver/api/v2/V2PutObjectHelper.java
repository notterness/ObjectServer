package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.ETagType;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.exceptions.UnauthorizedHeaderException;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static javax.measure.unit.NonSI.BYTE;

public class V2PutObjectHelper {

    private final TrafficController controller;
    private final AsyncAuthenticator authenticator;
    private final PutObjectBackend backend;
    private final WebServerConfiguration webServerConfiguration;
    private final List<String> servicePrincipals;

    public V2PutObjectHelper(TrafficController controller,
                             AsyncAuthenticator authenticator,
                             PutObjectBackend backend,
                             WebServerConfiguration webServerConfiguration,
                             List<String> servicePrincipals) {
        this.controller = controller;
        this.authenticator = authenticator;
        this.backend = backend;
        this.webServerConfiguration = webServerConfiguration;
        this.servicePrincipals = servicePrincipals;
    }

    public CompletableFuture<Void> handleCompletably(RoutingContext context,
                                                     String namespace,
                                                     String bucketName,
                                                     String objectName) {
        Validator.validateV2Namespace(namespace);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_PUT_OBJECT_BUNDLE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        // Vert.x reads the request in the background by default, but we want to stream it through a state machine, so
        // we tell Vert.x to pause the reading. The request is resumed in the PutStateMachine.
        request.pause();

        HttpContentHelpers.negotiateStorageObjectContent(request, 0,
                webServerConfiguration.getMaxObjectSize().longValue(BYTE));
        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.STAR_ONLY);

        final Map<String, String> metadata = HttpHeaderHelpers.getUserMetadataHeaders(request);
        final String expectedMD5 = ChecksumHelper.getContentMD5Header(request).orElse(null);
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

        final String ifMatchEtag = request.getHeader(HttpHeaders.IF_MATCH);
        final String ifNoneMatchEtag = request.getHeader(HttpHeaders.IF_NONE_MATCH);

        final ObjectMetadata objMeta = HttpContentHelpers.readObjectMetadata(
                request, namespace, bucketName, objectName, metadata);

        // Acquire can throw an exception(with err code of 503 or 429) if the request couldnt be accepted
        controller.acquire(objMeta.getNamespace(), TrafficRecorder.RequestType.PutObject, objMeta.getSizeInBytes());

        return authenticator.authenticatePutObject(context)
                .thenCompose(authInfo -> {
                    if ((md5Override != null || etagOverride != null || partCountOverride != null ||
                        etagRound != null) &&
                            !(authInfo.getServicePrincipal().isPresent() &&
                                    servicePrincipals.contains(authInfo.getServicePrincipal().get().getTenantId()))) {
                        throw new UnauthorizedHeaderException();
                    }
                    return backend.createOrOverwriteObject(
                            context,
                            authInfo,
                            objMeta,
                            curObjMeta -> {
                                HttpMatchHelpers.checkConditionalHeaders(
                                        request, curObjMeta.map(ObjectMetadata::getETag).orElse(null));
                                HttpContentHelpers.write100Continue(request, response);
                            },
                            Api.V2,
                            WebServerMetrics.V2_PUT_OBJECT_BUNDLE,
                            expectedMD5,
                            null /* expectedSHA256 */,
                            ifMatchEtag,
                            ifNoneMatchEtag,
                            md5Override,
                            etagOverride,
                            partCountOverride,
                            ETagType.ETAG);
                })
                .handle((val, ex) -> HttpException.handle(request, val, ex))
                .thenAccept(newObjMeta -> {
                    response.putHeader(HttpHeaders.ETAG, newObjMeta.getETag());
                    response.putHeader(HttpHeaders.LAST_MODIFIED,
                            DateUtil.httpFormattedDate(newObjMeta.getModificationTime()));
                    newObjMeta.getChecksum().addHeaderToResponseV2Put(response);
                    response.end();
                });
    }
}
