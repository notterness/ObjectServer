package com.oracle.pic.casper.webserver.api.v1;

import com.oracle.pic.casper.common.config.v2.WebServerConfiguration;
import com.oracle.pic.casper.common.exceptions.TooBusyException;
import com.oracle.pic.casper.common.exceptions.TooManyRequestsException;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.ETagType;
import com.oracle.pic.casper.objectmeta.ObjectKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.PutObjectBackend;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.traffic.TenantThrottleException;
import com.oracle.pic.casper.webserver.traffic.TrafficControlException;
import com.oracle.pic.casper.webserver.traffic.TrafficController;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.Map;

import static javax.measure.unit.NonSI.BYTE;

/**
 * Adds a new object (or updates an existing one) to the object store.
 *
 * If the If-None-Match header is passed in, this method will throw an exception if an object already exists with
 * the same key. Similarly, if the If-Match header is passed in, then an object must already exist in the system
 * for that key and it's eTag must match the value provided in the header for the put to be accepted.
 */
public class PutObjectHandler extends AbstractRouteHandler {

    private final TrafficController controller;
    private final WebServerConfiguration webServerConfiguration;
    private final PutObjectBackend backend;
    private final EmbargoV3 embargoV3;

    public PutObjectHandler(TrafficController controller,
                            PutObjectBackend backend,
                            WebServerConfiguration webServerConfiguration,
                            EmbargoV3 embargoV3) {
        this.controller = controller;
        this.backend = backend;
        this.webServerConfiguration = webServerConfiguration;
        this.embargoV3 = embargoV3;
    }

    @Override
    protected void subclassHandle(RoutingContext context) {

        final HttpServerRequest request = context.request();
        final ObjectKey objectKey = RequestHelpers.computeObjectKey(request);
        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V1)
            .setNamespace(objectKey.getBucket().getNamespace())
            .setBucket(objectKey.getBucket().getName())
            .setObject(objectKey.getName())
            .setOperation(CasperOperation.PUT_OBJECT)
            .build();
        embargoV3.enter(embargoV3Operation);

        final HttpServerResponse response = context.response();

        MetricsHandler.addMetrics(context, WebServerMetrics.V1_PUT_OBJECT_BUNDLE);

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.setNamespaceName(RequestHelpers.computeNamespaceKey(request).getName());

        // First, pause the request stream so it's not read prematurely
        request.pause();

        // force content length
        HttpContentHelpers.negotiateStorageObjectContent(request, 0,
                webServerConfiguration.getMaxObjectSize().longValue(BYTE));
        final ObjectKey objKey = RequestHelpers.computeObjectKey(request);

        final Map<String, String> metadata =
                HttpHeaderHelpers.getUserMetadataHeaders(request, HttpHeaderHelpers.UserMetadataPrefix.V1);
        final String expectedMD5 = ChecksumHelper.getContentMD5Header(request).orElse(null);
        final ObjectMetadata objMeta = HttpContentHelpers.readObjectMetadata(
                request, objKey.getBucket().getNamespace(), objKey.getBucket().getName(), objKey.getName(), metadata);
        final String ifMatchEtag = request.getHeader(HttpHeaders.IF_MATCH);
        final String ifNoneMatchEtag = request.getHeader(HttpHeaders.IF_NONE_MATCH);

        try {
            controller.acquire(null, TrafficRecorder.RequestType.PutObject, objMeta.getSizeInBytes());
        } catch (TrafficControlException tce) {
            throw new TooBusyException("the service is currently unavailable");
        } catch (TenantThrottleException tte) {
            throw new TooManyRequestsException("too many requests!");
        }

        backend.createOrOverwriteObject(
                context,
                AuthenticationInfo.V1_USER,
                objMeta,
                om -> {
                    HttpMatchHelpers.checkConditionalHeaders(request, om.map(ObjectMetadata::getETag).orElse(null));
                    HttpContentHelpers.write100Continue(request, response);
                },
                Api.V1,
                WebServerMetrics.V1_PUT_OBJECT_BUNDLE,
                expectedMD5,
                null /* expectedSHA256 */,
                ifMatchEtag,
                ifNoneMatchEtag,
                ETagType.ETAG)
                .handle((val, ex) -> HttpException.handle(request, val, ex))
                .thenAccept(newObjMeta -> {
                    response.putHeader(HttpHeaders.ETAG, newObjMeta.getETag());
                    response.putHeader(HttpHeaders.LAST_MODIFIED,
                            DateUtil.httpFormattedDate(newObjMeta.getModificationTime()));
                    newObjMeta.getChecksum().addHeaderToResponseV1(response);
                    response.end();
                })
                .exceptionally(throwable -> PutObjectHandler.fail(context, throwable));
    }
}
