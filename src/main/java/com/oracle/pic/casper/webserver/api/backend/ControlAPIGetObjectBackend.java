package com.oracle.pic.casper.webserver.api.backend;

import com.oracle.pic.casper.common.config.v2.BoatConfiguration;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.AuthorizationException;
import com.oracle.pic.casper.common.exceptions.IfMatchException;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.vertx.VertxExecutor;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.common.vertx.stream.AbortableBlobReadStream;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.ChecksumType;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.AuthorizationResponse;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A control API's perspective on a {@link GetObjectBackend} that by necessity can perform operations beyond {@link
 * GetObjectBackend}'s public API.
 */
public class ControlAPIGetObjectBackend {

    /**
     * The underlying backend that implements this wrapper's enhanced capabilities.
     */
    private final GetObjectBackend backend;

    /**
     * BMC Operator Access Tenancy used to adjudicate access to control APIs.
     */
    private final BoatConfiguration boatConfiguration;

    /**
     * Construct a control perspective from a less privileged backend.
     *
     * @param backend The underlying backend.
     */
    public ControlAPIGetObjectBackend(GetObjectBackend backend, BoatConfiguration boatConfiguration) {
        this.backend = backend;
        this.boatConfiguration = boatConfiguration;
    }

    private void authorize(
            WSRequestContext wsRequestContext,
            AuthenticationInfo authInfo,
            String namespace,
            String bucketName
    ) {
        final Optional<AuthorizationResponse> authorizationResponse =
                backend.getAuthorizer().authorize(
                        wsRequestContext,
                        authInfo,
                        new NamespaceKey(Api.V2, namespace),
                        bucketName,
                        boatConfiguration.getCompartmentOcid(),
                        null,
                        CasperOperation.CHECK_OBJECT,
                        null,
                        true,
                        false,
                        CasperPermission.CHECK_ALL_OBJECTS
                );

        if (!authorizationResponse.isPresent()) {
            throw new AuthorizationException(
                    V2ErrorCode.UNAUTHORIZED.getStatusCode(), V2ErrorCode.UNAUTHORIZED.getErrorName(), null);
        }
    }

    /**
     * Check that an arbitrary object's digests matches those stored in its metadata.
     *
     * This currently only checks Content-MD5.
     *
     * @param context    A routing context for an active request.
     * @param namespace  The namespace in which the object's bucket resides.
     * @param bucketName The bucket in which the object resides.
     * @param objectName The object to check.
     * @return A {@link CompletableFuture<Void>} that completes exceptionally if the actual digest or content length
     * differs from the values stored in metadata.
     */
    public CompletableFuture<Void> checkObject(RoutingContext context,
                                               WSRequestContext wsRequestContext,
                                               AuthenticationInfo authInfo,
                                               String namespace,
                                               String bucketName,
                                               String objectName,
                                               Api api,
                                               @Nullable String ifMatch) {

        final MetricScope rootScope = WSRequestContext.getMetricScope(context);
        final MetricScope childScope = rootScope.child("controlAPI:checkObject");

        return VertxUtil.runAsync(childScope, () ->
            authorize(wsRequestContext, authInfo, namespace, bucketName)
        ).thenCompose(
                ignored -> backend.getStorageObject(
                        context,
                        GetObjectBackend.ReadOperation.GET,
                        bucket -> true,
                        namespace,
                        bucketName,
                        objectName,
                        api
                )
        ).thenComposeAsync(
                wsStorageObject -> {
                    if (ifMatch != null && !wsStorageObject.getETag().equals(ifMatch)) {
                        throw new IfMatchException("If-Match failed");
                    }
                    if (wsStorageObject.getChecksumType() != ChecksumType.MD5) {
                        throw new UncheckableObject(
                                String.format(
                                        "Can't check object with checksum %s", wsStorageObject.getChecksumType()));
                    }

                    if (wsStorageObject.getMd5() == null) {
                        throw new UncheckableObject("Can't check object with null MD5");
                    }

                    final CompletableFuture<Void> digestsChecked = new CompletableFuture<>();

                    final AbortableBlobReadStream objectStream = backend.getObjectStream(
                        WSRequestContext.get(context),
                        wsStorageObject,
                        null,
                        true,
                        WebServerMetrics.V2_CHECK_OBJECT_BUNDLE);

                    // We just want the MD5 digest...
                    objectStream.handler(buffer -> {
                    });

                    // ...if it passes the DefaultAbortableBlobReadStream's "local validation", complete the future
                    // without an exception...
                    objectStream.endHandler(ignored -> digestsChecked.complete(null));

                    // ...otherwise propagate the exception to the receiver.
                    objectStream.exceptionHandler(digestsChecked::completeExceptionally);
                    return digestsChecked;
                }, VertxExecutor.eventLoop());
    }
}
