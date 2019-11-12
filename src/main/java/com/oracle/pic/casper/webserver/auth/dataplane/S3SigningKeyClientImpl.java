package com.oracle.pic.casper.webserver.auth.dataplane;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.oracle.pic.casper.common.config.failsafe.AuthDataPlaneFailSafeConfig;
import com.oracle.pic.casper.common.config.v2.AuthDataPlaneClientConfiguration;
import com.oracle.pic.casper.common.config.v2.S3SigningKeyClientConfiguration;
import com.oracle.pic.casper.common.exceptions.AuthDataPlaneConnectionException;
import com.oracle.pic.casper.common.exceptions.AuthDataPlaneServerException;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SigningKey;

import java.net.URI;

/**
 * Identity client for retrieving S3 signing keys and cache them for configured amount of time. Executes RPC calls
 * using auth data plane client in a failsafe manner and emits metrics.
 */
public class S3SigningKeyClientImpl extends CachingFailSafeClient<URI, SigningKey> implements S3SigningKeyClient {

    private final AuthDataPlaneClient authDataPlaneClient;

    public S3SigningKeyClientImpl(AuthDataPlaneClient authDataPlaneClient,
                                  AuthDataPlaneClientConfiguration dpConfig,
                                  S3SigningKeyClientConfiguration s3Config) {
        super(s3Config.getCacheConfiguration(),
                new AuthDataPlaneFailSafeConfig(dpConfig.getFailSafeConfiguration()),
                AuthMetrics.GET_SIGNING_KEY,
                AuthMetrics.S3_CACHE_SIZE);
        this.authDataPlaneClient = authDataPlaneClient;
    }

    @Override
    public SigningKey getSigningKey(String keyId, String region, String date, String service)
            throws AuthDataPlaneConnectionException, AuthDataPlaneServerException {
        final URI uri = Urls.derivedKey(keyId, region, date, service);
        try {
            return getValue(uri);
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof AuthDataPlaneConnectionException) {
                throw (AuthDataPlaneConnectionException) e.getCause();
            }
            if (e.getCause() instanceof AuthDataPlaneServerException) {
                throw (AuthDataPlaneServerException) e.getCause();
            }
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    protected SigningKey loadValue(URI uri) {
        return authDataPlaneClient.getSigningKey(uri);
    }

    @Override
    protected boolean isClientError(Exception e) {
        if (e instanceof AuthDataPlaneServerException) {
            AuthDataPlaneServerException authDataPlaneServerException = (AuthDataPlaneServerException) e;
            return HttpResponseStatus.isClientError(authDataPlaneServerException.getStatusCode());
        }
        return false;
    }
}
