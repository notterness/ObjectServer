package com.oracle.pic.casper.webserver.auth.dataplane;

import com.oracle.pic.identity.authentication.Principal;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Authenticator for swift requests. Authentication can be configured in one of the two modes: local or identity.
 *
 * When local mode is selected, swift credentials for a specified swift user are retrieved from identity data plane
 * using the API specified below.
 *
 * https://confluence.oci.oraclecorp.com/display/ID/Get+Credentials+For+Swift+Authentication+API+Design
 *
 * The credentials are cached for a configured amount of time in a local cache. Scrypt authenticator from Identity
 * SDK is used to validate the password against the list of credentials for the given tenant and user.
 *
 * When identity mode is selected, authentication is done at the identity data plane.
 *
 * @see com.oracle.pic.identity.authentication.SCryptPasswordAuthenticator
 * @see SwiftCredentialsClient
 * @see SwiftLegacyAuthenticator
 */
public interface SwiftAuthenticator {

    Principal authenticate(@Nullable String tenantOcid,
                           String tenantName,
                           Map<String, List<String>> headers);

}
