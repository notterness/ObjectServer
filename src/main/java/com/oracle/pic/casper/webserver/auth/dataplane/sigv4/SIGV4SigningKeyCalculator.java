package com.oracle.pic.casper.webserver.auth.dataplane.sigv4;

import com.google.inject.Singleton;
import com.oracle.pic.casper.common.encryption.CasperSecurity;

import javax.crypto.Mac;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Calculate signing keys that can be used to sign requests or validate a signed
 * request.
 */
@Singleton
public class SIGV4SigningKeyCalculator {

    public static final String AWS4_HMAC_SHA256 = "AWS4-HMAC-SHA256";
    public static final String HMAC_SHA256 = "HmacSHA256";
    /**
     * Prefixed to the secret key when generating a signing key. Stored as a
     * byte array so we don't have to convert it each time we want to generate
     * a signing key.
     */
    private static final byte[] AWS4 = "AWS4".getBytes(UTF_8);

    /**
     * The string "aws4_request" which is used as part of the signing key.
     * Stored as a byte array so we don't have to convert it each time we want
     * to generate a signing key.
     */
    private final byte[] aws4Request = "aws4_request".getBytes(UTF_8);

    /**
     * Used to calculate HMAC SHA-256.
     */
    private final Mac mac;

    /**
     * These are used as output buffers for our intermediate HMAC calculations.
     * We create them in the constructor so we don't keep allocating memory.
     */
    private final byte[] dateKey;
    private final byte[] dateRegionKey;
    private final byte[] dateRegionServiceKey;

    public SIGV4SigningKeyCalculator() {
        this.mac = CasperSecurity.getMac(HMAC_SHA256);
        this.dateKey = new byte[this.mac.getMacLength()];
        this.dateRegionKey = new byte[this.mac.getMacLength()];
        this.dateRegionServiceKey = new byte[this.mac.getMacLength()];
    }

    public synchronized SIGV4SigningKey calculateSigningKey(String secretKey, String region, String date,
                                                            String service) {
        final byte[] secretKeyBytes = secretKey.getBytes(UTF_8);
        final byte[] regionBytes = region.getBytes(UTF_8);
        final byte[] serviceBytes = service.getBytes(UTF_8);
        final byte[] secretBytes = Arrays.copyOf(AWS4, AWS4.length + secretKeyBytes.length);
        try {
            System.arraycopy(secretKeyBytes, 0, secretBytes, AWS4.length, secretKeyBytes.length);
            this.hmacSHA256(secretBytes, date.getBytes(UTF_8), this.dateKey);
            this.hmacSHA256(this.dateKey, regionBytes, this.dateRegionKey);
            this.hmacSHA256(this.dateRegionKey, serviceBytes, this.dateRegionServiceKey);
            return new SIGV4SigningKey(this.hmacSHA256(this.dateRegionServiceKey, this.aws4Request));
        } catch (ShortBufferException | InvalidKeyException e) {
            throw new RuntimeException(e);
        } finally {
            Arrays.fill(secretBytes, Byte.MAX_VALUE);
            Arrays.fill(this.dateKey, Byte.MAX_VALUE);
            Arrays.fill(this.dateRegionKey, Byte.MAX_VALUE);
            Arrays.fill(this.dateRegionServiceKey, Byte.MAX_VALUE);
        }
    }

    /**
     * Calculate the HMAC signature of some data. This puts the output in the
     * provided output buffer.
     */
    private void hmacSHA256(byte[] key, byte[] dataToSign, byte[] output)
            throws InvalidKeyException, ShortBufferException {
        this.mac.init(new SecretKeySpec(key, AWS4_HMAC_SHA256));
        this.mac.update(dataToSign);
        this.mac.doFinal(output, 0);
    }

    /**
     * Calculate the HMAC signature of some data. This returns a new buffer.
     */
    private byte[] hmacSHA256(byte[] key, byte[] dataToSign) throws InvalidKeyException {
        this.mac.init(new SecretKeySpec(key, AWS4_HMAC_SHA256));
        return this.mac.doFinal(dataToSign);
    }
}
