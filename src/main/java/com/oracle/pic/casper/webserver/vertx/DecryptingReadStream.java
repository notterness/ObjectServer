package com.oracle.pic.casper.webserver.vertx;

import com.google.common.base.Preconditions;
import com.oracle.oci.casper.jopenssl.CryptoCipher;
import com.oracle.pic.casper.common.encryption.EncryptionKey;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import java.security.GeneralSecurityException;

/**
 * This class decrypts ciphertexts that were encrypted using AES - CTR and is the sister stream
 * to {@link EncryptingReadStream}.
 */
public class DecryptingReadStream extends CipherReadStream {

    private final CryptoCipher cipher;

    /**
     * Constructs a decrypting read stream.
     * @param delegate The next readstream in the chain
     * @param encryptionKey The encryption key to decrypt with
     * @param offset which block to start decrypting at. Should likely be divisible by AES_BLOCK_SIZE
     */
    public DecryptingReadStream(@Nonnull ReadStream<Buffer> delegate, EncryptionKey encryptionKey, long offset) {
        super(delegate);
        Preconditions.checkState(encryptionKey.getDataEncryptionKey().getAlgorithm().
                equalsIgnoreCase("AES"));
        try {
            cipher = encryptionKey.dataCipher(Cipher.DECRYPT_MODE, offset, true);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Could not initialize the cipher ", e);
        }
    }

    @Override
    protected Buffer before(Buffer buffer) {
        return doDecryption(buffer);
    }

    private Buffer doDecryption(@Nonnull Buffer buffer) {
        return updateCipher(cipher, buffer);
    }
}
