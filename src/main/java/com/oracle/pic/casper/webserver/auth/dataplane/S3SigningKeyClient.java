package com.oracle.pic.casper.webserver.auth.dataplane;

import com.oracle.pic.casper.webserver.auth.dataplane.model.SigningKey;

public interface S3SigningKeyClient {

    /**
     * Get the SIGv4 signing key for the given key ID, region, date and service.
     *
     * The exceptions thrown by this method nearly always indicate that an internal error has occurred which is not the
     * fault of the client, and do not indicate that authentication has failed. However, when this method throws an
     * AuthDataPlaneServerException with a status code of 404, that indicates the signing key could not be found, which
     * is likely to mean that the key ID is invalid.
     */
    SigningKey getSigningKey(String keyId, String region, String date, String service);
}
