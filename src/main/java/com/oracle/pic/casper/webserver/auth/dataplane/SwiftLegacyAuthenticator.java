package com.oracle.pic.casper.webserver.auth.dataplane;

import com.oracle.pic.casper.webserver.auth.dataplane.exceptions.NotAuthenticatedException;
import com.oracle.pic.identity.authentication.Principal;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public interface SwiftLegacyAuthenticator {

    /**
     * Authenticate a Swift request using basic authentication at identity data plane.
     *
     * All exceptions thrown by this method (except as noted below) indicate that an internal error has occurred (not
     * the fault of the client and not indicating that authentication has failed). The request has failed and must not
     * be retried (as noted above).
     *
     * @throws NotAuthenticatedException if the user could
     *         not be authenticated by the data plane service. This must be returned to HTTP clients as a 401 error.
     *         The exception message is suitable for display to a client.
     */
    Principal authenticateSwift(
            @Nullable String tenantOcid, String tenantName, Map<String, List<String>> headers);
}
