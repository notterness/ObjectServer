package com.oracle.pic.casper.webserver.auth.dataplane;

import com.oracle.pic.casper.webserver.auth.dataplane.model.SigningKey;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SwiftCredentials;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SwiftUser;
import com.oracle.pic.identity.authentication.Principal;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * AuthDataPlaneClient is a client for the identity data plane service.
 *
 * An overview of the identity services can be found here:
 *   https://confluence.oraclecorp.com/confluence/display/OCIID/Identity+Integration
 *
 * The Swagger specification for the data plane is linked from the wiki document above.
 *
 * This client does only RPC calls, failed calls are not retried and no metrics are generated.
 *
 */
public interface AuthDataPlaneClient extends Closeable {

    /**
     * Get the SIGv4 signing key for the given URI.
     *
     * The exceptions thrown by this method nearly always indicate that an internal error has occurred which is not the
     * fault of the client, and do not indicate that authentication has failed. However, when this method throws an
     * AuthDataPlaneServerException with a status code of 404, that indicates the signing key could not be found, which
     * is likely to mean that the key ID is invalid.
     */
    SigningKey getSigningKey(URI uri);

    /**
     * Authenticate a Swift request using basic authentication.
     *
     * All exceptions thrown by this method (except as noted below) indicate that an internal error has occurred (not
     * the fault of the client and not indicating that authentication has failed). The request has failed and must not
     * be retried (as noted above).
     *
     * @throws com.oracle.pic.casper.common.exceptions.AuthDataPlaneServerException if the user could
     *         not be authenticated by the data plane service. This must be returned to HTTP clients as a 401 error.
     *         The exception message is suitable for display to a client.
     */
    Principal authenticateSwift(
            @Nullable String tenantOcid, String tenantName, Map<String, List<String>> headers);


    /**
     * Get list of hash passwords for the given tenantOcid or tenantName and username.
     */
    SwiftCredentials getSwiftCredentials(SwiftUser swiftUser);
}
