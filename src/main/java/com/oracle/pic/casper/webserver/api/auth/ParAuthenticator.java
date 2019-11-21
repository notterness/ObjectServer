package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.webserver.api.model.GetPreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.PreAuthenticatedRequestMetadata;
import com.oracle.pic.casper.webserver.api.model.exceptions.ParNotFoundException;
import com.oracle.pic.casper.webserver.api.pars.BackendParId;
import com.oracle.pic.casper.webserver.api.pars.PreAuthenticatedRequestBackend;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Authenticator used by PAR runtime handlers, i.e. when using a PAR to access an object in Casper. When creating each
 * PAR, Casper obtains and stores the following attributes of it, among others:
 *
 * 1. parId = objectName + verifier ID. This is the unique identifier of PAR
 * 2. nonce - the super sensitive secret that is handed to the other user who uses the PAR at runtime
 * 3. originating identity principal
 *
 * The nonce is created at PAR provisioning time. Casper calculates the STS for the future runtime request and then
 * generates a nonce and parId from that. Brief steps are
 *
 * STS = fn1(request)
 * nonce = fn2(STS)
 * parId = fn3 (STS, nonce)
 *
 * When the PAR user shows up at runtime with a PAR (that has nonce in it), we need a special Authenticator that can
 * authenticate this PAR. This is done by locating the PAR in the backend store and pulling out the identity principal
 * associated with that PAR. Brief steps are
 *
 * STS = fn1(request)
 * parId = fn3(STS, nonce)
 *
 * We can then locate the PAR by parId and get the identity principal, which constitutes a successful authentication.
 *
 * Note that when we issue a bucket level PAR, there is no object name associated with that PAR, i.e no object name in
 * the STS. When the user shows up with the PAR at runtime, Casper does not know if it is object-level or bucket-level
 * because the http request looks essentially the same and the nonce is opaque to Casper.
 * It assumes that the incoming PAR is an object-level PAR first by including the object name in the STS, and tries to
 * retrieve it and authenticates the request upon successful retrieval. If this first attempt fails with a
 * {@link ParNotFoundException}, it tries again assuming the PAR is bucket-level, this time excluding object name from
 * the STS.
 */
public class ParAuthenticator implements AsyncAuthenticator {

    private static final Logger LOG = LoggerFactory.getLogger(ParAuthenticator.class);

    private final PreAuthenticatedRequestBackend parBackend;
    private final AuthenticationInfo defaultAuthInfo;

    /**
     * We need a fake defaultAuthInfo and a pass-through {@link AuthorizerPassThroughImpl} here because in this
     * scenario, Casper itself is using Casper's backend to perform a GET object operation. Hence Casper should let
     * itself through to authenticate the PAR runtime request. This also explains all the dependencies below, which are
     * needed by {@link PreAuthenticatedRequestBackend} to perform the GET.
     */
    public ParAuthenticator(PreAuthenticatedRequestBackend parBackend) {
        this.parBackend = parBackend;
        this.defaultAuthInfo = AuthenticationInfo.SUPER_USER;
    }

    @Override
    public CompletableFuture<AuthenticationInfo> authenticate(RoutingContext context) {
        return authenticateViaPars(context);
    }

    @Override
    public CompletableFuture<AuthenticationInfo> authenticate(RoutingContext context, String bodySha256) {
        return authenticateViaPars(context);
    }

    @Override
    public CompletableFuture<AuthenticationInfo> authenticatePutObject(RoutingContext context) {
        return authenticateViaPars(context);
    }

    // TODO :implement
    @Override
    public AuthenticationInfo authenticatePutObject(HttpHeaders headersOrig, String uriString, String namespace, String method, MetricScope metricScope) {
        return null;
    }


    public CompletableFuture<AuthenticationInfo> authenticateViaPars(RoutingContext context) {

        final CompletableFuture<AuthenticationInfo> result = new CompletableFuture<>();

        final HttpServerRequest request = context.request();
        // check that nonce exists
        String nonce = HttpPathQueryHelpers.getParPathNonce(request);

        // grab nonce and talk to identity sdk -> vends par id
        // try AuthN with objectName first
        Map<String, String> objectSTS = ParSigningHelper.getSTSForObjectLevelPars(request, context);
        String objectVerifierId = ParSigningHelper.getVerifierId(objectSTS, nonce);
        String objectName = HttpPathQueryHelpers.getObjectName(request, WSRequestContext.get(context));
        // customer-side parId does not have bucketId in it
        BackendParId objectParId = new BackendParId(objectName, objectVerifierId);
        LOG.debug("first try: parId {} from sts {} ", objectParId, objectSTS);

        GetPreAuthenticatedRequestRequest getRequest =
                new GetPreAuthenticatedRequestRequest(context, defaultAuthInfo, objectParId);
        parBackend.getPreAuthenticatedRequest(getRequest)
                .whenComplete((parMd, t) -> {
                    if (parMd != null) {
                        processResultFromPar(context, parMd, result);
                    } else if (unwrapException(t) instanceof ParNotFoundException) {
                        // retry without objectName
                        Map<String, String> bucketSTS = ParSigningHelper.getSTSForBucketLevelPars(request, context);
                        BackendParId bucketParId = new BackendParId(ParSigningHelper.getVerifierId(bucketSTS, nonce));
                        LOG.debug("second try: parId {} from sts {} ", bucketParId, bucketSTS);
                        GetPreAuthenticatedRequestRequest possibleGetRequest =
                                new GetPreAuthenticatedRequestRequest(context, defaultAuthInfo, bucketParId);
                        parBackend.getPreAuthenticatedRequest(possibleGetRequest)
                                .whenComplete((anotherParMd, anotherT) -> {
                                    if (anotherParMd != null) {
                                        processResultFromPar(context, anotherParMd, result);
                                    } else if (unwrapException(anotherT) instanceof ParNotFoundException) {
                                        result.completeExceptionally(
                                                new NotAuthenticatedException("PAR does not exist"));
                                    } else {
                                        result.completeExceptionally(unwrapException(anotherT));
                                    }
                                });
                    } else {
                        result.completeExceptionally(new NotAuthenticatedException("PAR does not exist"));
                    }
                });
        return result;
    }

    private void processResultFromPar(RoutingContext context,
                                      PreAuthenticatedRequestMetadata par,
                                      CompletableFuture<AuthenticationInfo> future) {
        final WSRequestContext wsContext = WSRequestContext.get(context);
        wsContext.setPARAccessType(par.getAccessType());
        wsContext.setPARExpireTime(par.getTimeExpires());
        future.complete(par.getAuthInfo());
    }

    private Exception unwrapException(Throwable t) {
        if (t == null) {
            return new RuntimeException("Trying to unwrap null Exception");
        }

        if (t instanceof CompletionException) {
            return (Exception) t.getCause();
        } else {
            return new RuntimeException(t);
        }
    }
}
