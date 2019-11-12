package com.oracle.pic.casper.webserver.vertx;

import com.codahale.metrics.Histogram;
import com.google.common.base.Preconditions;
import com.oracle.oci.casper.jopenssl.CryptoCipher;
import com.oracle.pic.casper.common.encryption.EncryptionKey;
import com.oracle.pic.casper.common.metrics.Metrics;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import javax.annotation.Nonnull;
import java.security.GeneralSecurityException;

/**
 * This class performs AEX CTR - Counter mode encryption.
 * CTR supports random seek which is desirable since we can support Range gets from a StorageServer.
 */
public class EncryptingReadStream extends CipherReadStream {

    private final CryptoCipher cipher;
    private final Histogram encryptionTime = Metrics.REGISTRY.histogram("webserver.encryption.time");
    private final boolean metersDebugEnabled;

    /**
     * Constructor that accepts the delegate that will deliver the buffer to encrypt and
     * the key with which to perform the encryption.
     * @param delegate The delegate in the chain
     * @param encryptionKey The encryption key used to encrypt stream
     * @param metersDebugEnabled
     */
    public EncryptingReadStream(@Nonnull ReadStream<Buffer> delegate, EncryptionKey encryptionKey,
                                boolean metersDebugEnabled)  {
        super(delegate);
        Preconditions.checkState(encryptionKey.getDataEncryptionKey().getAlgorithm().
                equalsIgnoreCase("AES"));
        try {
            cipher = encryptionKey.dataCipher(javax.crypto.Cipher.ENCRYPT_MODE, 0, false);
        } catch (GeneralSecurityException e) {
            throw new UnsupportedOperationException("Could not initialize the cipher", e);
        }
        this.metersDebugEnabled = metersDebugEnabled;
    }

    @Override
    protected Buffer before(Buffer buffer) {
        return doEncryption(buffer);
    }

    /**
     * So simple!
     */
    private Buffer doEncryption(Buffer buffer) {
        long start = 0;
        if (metersDebugEnabled) {
            start = System.nanoTime();
        }
        Buffer encrypt = updateCipher(cipher, buffer);
        if (metersDebugEnabled) {
            encryptionTime.update(System.nanoTime() - start);
        }
        return encrypt;
    }

}
