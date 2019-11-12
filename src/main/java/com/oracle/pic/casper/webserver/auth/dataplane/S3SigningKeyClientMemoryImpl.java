package com.oracle.pic.casper.webserver.auth.dataplane;

import com.google.common.base.Preconditions;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SigningKey;
import com.oracle.pic.casper.webserver.auth.dataplane.sigv4.SIGV4SigningKey;
import com.oracle.pic.casper.webserver.auth.dataplane.sigv4.SIGV4SigningKeyCalculator;
import com.oracle.pic.identity.authentication.Principal;

import java.util.HashMap;
import java.util.Map;

public class S3SigningKeyClientMemoryImpl implements S3SigningKeyClient {
    private final Map<String, String> secrets;
    private final Map<String, Principal> principals;
    private final SIGV4SigningKeyCalculator calculator;

    public S3SigningKeyClientMemoryImpl() {
        this.secrets = new HashMap<>();
        this.principals = new HashMap<>();
        this.calculator = new SIGV4SigningKeyCalculator();
    }

    @Override
    public SigningKey getSigningKey(String keyId, String region, String date, String service) {
        String secretKey = secrets.get(keyId);
        if (secretKey == null) {
            throw new RuntimeException("secret not found");
        }
        SIGV4SigningKey signingKey = calculator.calculateSigningKey(secretKey, region, date, service);
        Principal principal = Preconditions.checkNotNull(principals.get(keyId));
        return new SigningKey(signingKey, principal);
    }

    public void addKey(String keyId, String secret, Principal principal) {
        secrets.put(keyId, secret);
        principals.put(keyId, principal);
    }

}
