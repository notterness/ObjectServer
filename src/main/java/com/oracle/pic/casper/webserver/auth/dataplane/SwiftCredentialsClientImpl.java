package com.oracle.pic.casper.webserver.auth.dataplane;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.oracle.pic.casper.common.config.failsafe.AuthDataPlaneFailSafeConfig;
import com.oracle.pic.casper.common.config.v2.AuthDataPlaneClientConfiguration;
import com.oracle.pic.casper.common.config.v2.SwiftCredentialsClientConfiguration;
import com.oracle.pic.casper.common.exceptions.AuthDataPlaneConnectionException;
import com.oracle.pic.casper.common.exceptions.AuthDataPlaneServerException;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.auth.dataplane.exceptions.NotAuthenticatedException;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SwiftCredentials;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SwiftUser;

public class SwiftCredentialsClientImpl extends CachingFailSafeClient<SwiftUser, SwiftCredentials>
        implements SwiftCredentialsClient {

    private final AuthDataPlaneClient authDataPlaneClient;

    public SwiftCredentialsClientImpl(AuthDataPlaneClient authDataPlaneClient,
                                      AuthDataPlaneClientConfiguration authConfig,
                                      SwiftCredentialsClientConfiguration swiftConfig) {
        super(swiftConfig.getCacheConfiguration(),
                new AuthDataPlaneFailSafeConfig(authConfig.getFailSafeConfiguration()),
                AuthMetrics.SWIFT_CREDENTIALS,
                AuthMetrics.SWIFT_CACHE_SIZE);
        this.authDataPlaneClient = authDataPlaneClient;
    }

    @Override
    public SwiftCredentials getSwiftCredentials(SwiftUser swiftUser) {
        try {
            return getValue(swiftUser);
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof AuthDataPlaneConnectionException) {
                throw (AuthDataPlaneConnectionException) e.getCause();
            }
            if (e.getCause() instanceof AuthDataPlaneServerException) {
                AuthDataPlaneServerException ex = (AuthDataPlaneServerException) e.getCause();
                if (ex.getStatusCode() == HttpResponseStatus.NOT_FOUND) {
                    throw new NotAuthenticatedException();
                }
                throw ex;
            }
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    protected SwiftCredentials loadValue(SwiftUser swiftUser) {
        return authDataPlaneClient.getSwiftCredentials(swiftUser);
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
