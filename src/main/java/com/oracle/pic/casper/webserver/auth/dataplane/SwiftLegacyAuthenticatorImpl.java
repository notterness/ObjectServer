package com.oracle.pic.casper.webserver.auth.dataplane;

import com.oracle.pic.casper.common.config.failsafe.AuthDataPlaneFailSafeConfig;
import com.oracle.pic.casper.common.config.failsafe.FailSafeConfig;
import com.oracle.pic.casper.common.config.v2.AuthDataPlaneClientConfiguration;
import com.oracle.pic.identity.authentication.Principal;
import net.jodah.failsafe.Failsafe;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Identity client for authenticating SWIFT users at identity data plane. Executes RPC calls
 * using auth data plane client in a failsafe manner and emits metrics.
 */
public class SwiftLegacyAuthenticatorImpl implements SwiftLegacyAuthenticator {

    private final AuthDataPlaneClient authDataPlaneClient;
    private final FailSafeConfig failSafeConfig;

    public SwiftLegacyAuthenticatorImpl(AuthDataPlaneClient authDataPlaneClient,
                                        AuthDataPlaneClientConfiguration authConfig) {
        this.failSafeConfig = new AuthDataPlaneFailSafeConfig(authConfig.getFailSafeConfiguration());
        this.authDataPlaneClient = authDataPlaneClient;
    }

    @Override
    public Principal authenticateSwift(@Nullable String tenantOcid, String tenantName,
                                       Map<String, List<String>> headers) {
        return Failsafe.with(failSafeConfig.getRetryPolicy())
                .with(failSafeConfig.getCircuitBreaker())
                .get(() -> authenticateSwiftWithMetrics(tenantOcid, tenantName, headers));
    }

    private Principal authenticateSwiftWithMetrics(@Nullable String tenantOcid, String tenantName,
                                                   Map<String, List<String>> headers) {
        boolean success = false;
        final long start = System.nanoTime();
        try {
            AuthMetrics.SWIFT_BASIC_AUTH.getRequests().inc();
            final Principal result = authDataPlaneClient.authenticateSwift(tenantOcid, tenantName, headers);
            success = true;
            return result;
        } finally {
            final long end = System.nanoTime();
            final long elapsed = end - start;
            AuthMetrics.SWIFT_BASIC_AUTH.getOverallLatency().update(elapsed);
            if (success) {
                AuthMetrics.SWIFT_BASIC_AUTH.getSuccesses().inc();
            } else {
                AuthMetrics.SWIFT_BASIC_AUTH.getServerErrors().inc();
            }
        }
    }
}
