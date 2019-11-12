package com.oracle.pic.casper.webserver.api.auth.sigv4;

import com.oracle.oci.casper.jopenssl.CryptoMessageDigest;
import com.oracle.pic.casper.common.encryption.CasperSecurity;
import com.oracle.pic.casper.webserver.auth.dataplane.sigv4.SIGV4SigningKey;
import com.oracle.pic.casper.webserver.auth.dataplane.sigv4.SIGV4SigningKeyCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;
import java.security.DigestException;
import java.security.InvalidKeyException;
import java.time.Instant;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Calculate SIGV4 signatures.
 */
public class SIGV4Signer {

    private static final Logger LOG = LoggerFactory.getLogger(SIGV4Signer.class);

    /**
     * Used to calculate the SHA-256 hash of the canonical request.
     */
    private final CryptoMessageDigest digest;

    /**
     * Used to calculate the HMAC SHA-256.
     */
    private final Mac mac;

    /**
     * Used to store the SHA-256 hash of the canonical request. We allocate
     * the buffer in the constructor so we don't have to keep reallocating it.
     */
    private final byte[] hashBytes;

    /**
     * Used to calculate the signature. We allocate the buffer in the
     * constructor so we don't have to keep reallocating it.
     */
    private final byte[] signatureBytes;

    public SIGV4Signer() {
        this.mac = CasperSecurity.getMac(SIGV4SigningKeyCalculator.HMAC_SHA256);
        this.digest = CasperSecurity.getMessageDigest("SHA-256");
        this.hashBytes = new byte[this.digest.getDigestLength()];
        this.signatureBytes = new byte[this.mac.getMacLength()];
    }

    /**
     * Given a canonical request and a signing key calculate the SIGV4 signature.
     */
    public synchronized String calculateAuthorizationSignature(String stringToSign, SIGV4SigningKey signingKey) {
        try {
            mac.init(new SecretKeySpec(signingKey.getKey(), SIGV4SigningKeyCalculator.HMAC_SHA256));
            mac.update(stringToSign.getBytes(UTF_8));
            mac.doFinal(this.signatureBytes, 0);
            return SIGV4Utils.hex(this.signatureBytes);
        } catch (InvalidKeyException | ShortBufferException e) {
            LOG.error("calculateAuthorizationSignature fails", e);
            throw new RuntimeException(e);
        }
    }

    public String stringToSign(Instant time, String region, String service, String canonicalRequest) {
        final String scope = scope(time, region, service);
        return String.format("%s%n%s%n%s%n%s", SIGV4SigningKeyCalculator.AWS4_HMAC_SHA256, SIGV4Utils.formatTime(time),
                scope, this.sha256(canonicalRequest));
    }

    /**
     * Calculate a SHA-256 hash of a string.
     */
    private synchronized String sha256(String data) {
        try {
            this.digest.update(data.getBytes(UTF_8));
            this.digest.digest(this.hashBytes, 0, this.hashBytes.length);
            return SIGV4Utils.hex(this.hashBytes);
        } catch (DigestException e) {
            LOG.error("Digest failure", e);
            throw new RuntimeException(e);
        }
    }

    private static String scope(Instant when, String region, String service) {
        return String.format("%s/%s/%s/%s", SIGV4Utils.formatDate(when), region, service, "aws4_request");
    }
}
