package com.oracle.pic.casper.webserver.auth.dataplane;

import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.common.config.v2.SwiftAuthenticatorConfiguration;
import com.oracle.pic.casper.common.exceptions.AuthDataPlaneServerException;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.auth.dataplane.exceptions.NotAuthenticatedException;
import com.oracle.pic.casper.webserver.auth.dataplane.model.AuthUserAndPass;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SwiftCredentials;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SwiftHashPassword;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SwiftUser;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.SCryptPasswordAuthenticator;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Swift authentication can run in two modes:
 *
 * Legacy mode, which uses the SwiftLegacyAuthenticator
 * Cached mode, which uses the SwiftLegacyAuthenticator to get password hashes and
 * the SCryptPasswordAuthenticator to verify them.
 *
 * Which Mode the class runs in is controlled by the SwiftAuthenticatorConfiguration
 */
public class SwiftAuthenticatorImpl implements SwiftAuthenticator {

    private final SwiftCredentialsClient swiftCredentialsClient;
    private final SwiftLegacyAuthenticator swiftLegacyAuthenticator;
    private final SCryptPasswordAuthenticator sCryptPasswordAuthenticator;
    private final SwiftAuthenticatorConfiguration swiftAuthenticatorConfiguration;


    public SwiftAuthenticatorImpl(CasperConfig casperConfig,
                                  SwiftCredentialsClient swiftCredentialsClient,
                                  SwiftLegacyAuthenticator swiftLegacyAuthenticator) {
        this.swiftCredentialsClient = swiftCredentialsClient;
        this.swiftLegacyAuthenticator = swiftLegacyAuthenticator;
        this.sCryptPasswordAuthenticator = new SCryptPasswordAuthenticator();
        this.swiftAuthenticatorConfiguration = casperConfig.getSwiftAuthenticatorConfiguration();
    }

    @Override
    public Principal authenticate(@Nullable String tenantOcid,
                                  String tenantName,
                                  Map<String, List<String>> headers) {
        long start = System.nanoTime();
        try {
            AuthMetrics.WS_SWIFT_AUTHENTICATION.getRequests().inc();
            if (swiftAuthenticatorConfiguration.getAuthenticationMode() ==
                    SwiftAuthenticatorConfiguration.AuthenticationMode.CACHED) {
                // get the credentials for the swift user from identity
                // and validate the password against the hashed password.
                final AuthUserAndPass authUserAndPass = AuthUserAndPass.fromHeaders(headers);
                final SwiftUser swiftUser = new SwiftUser(tenantOcid, tenantName, authUserAndPass.getUsername());

                final long cacheGetStart = System.nanoTime();
                final SwiftCredentials swiftCredentials = swiftCredentialsClient.getSwiftCredentials(swiftUser);
                AuthMetrics.SWIFT_CACHE_GET_TIME.update(System.nanoTime() - cacheGetStart);

                for (SwiftHashPassword swiftHashPassword : swiftCredentials.getHashPasswords()) {
                    // validate the password against stored passwords.
                    final long scryptStart = System.nanoTime();
                    final boolean success = sCryptPasswordAuthenticator.validate(swiftHashPassword.getHashPassword(),
                            authUserAndPass.getPassword().getBytes(StandardCharsets.UTF_8));
                    AuthMetrics.SWIFT_SCRYPT_TIME.update(System.nanoTime() - scryptStart);
                    if (success) {
                        return swiftCredentials.getPrincipal();
                    }
                }
                throw new NotAuthenticatedException();
            } else {
                // if authentication mode set to 'identity' call Identity API to authenticate swift.
                return swiftLegacyAuthenticator.authenticateSwift(tenantOcid, tenantName, headers);
            }
        } catch (AuthDataPlaneServerException e) {
            if (HttpResponseStatus.isClientError(e.getStatusCode())) {
                AuthMetrics.WS_SWIFT_AUTHENTICATION.getClientErrors().inc();
            } else {
                AuthMetrics.WS_SWIFT_AUTHENTICATION.getServerErrors().inc();
            }
            throw e;
        } catch (NotAuthenticatedException e) {
            AuthMetrics.WS_SWIFT_AUTHENTICATION.getClientErrors().inc();
            throw e;
        } catch (RuntimeException e) {
            AuthMetrics.WS_SWIFT_AUTHENTICATION.getServerErrors().inc();
            throw e;
        } finally {
            AuthMetrics.WS_SWIFT_AUTHENTICATION.getOverallLatency().update(System.nanoTime() - start);
        }
    }
}
