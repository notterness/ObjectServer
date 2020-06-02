package com.webutils.webserver.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Md5Digest {

    private static final Logger LOG = LoggerFactory.getLogger(Md5Digest.class);

    private MessageDigest objectDigestCalculator;

    public Md5Digest() {
        /*
         ** Feed this ByteBuffer through the MD5 calculator
         */

        try {
            objectDigestCalculator = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            LOG.error("NoSuchAlgorithm MD5: " + ex.getMessage());
            objectDigestCalculator = null;
        }
    }


    public void digestByteBuffer(final ByteBuffer dataBuffer) {
        /*
         ** Need to handle the allocateDirect() ByteBuffer which do not allow access to the backing
         **   array. This might be a problem that will force the use of normal buffer allocation.
         */
        if (dataBuffer.isDirect()) {
            byte[] a = new byte[dataBuffer.limit()];
            dataBuffer.get(a, 0, dataBuffer.limit());
/*
            LOG.info("digestByteBuffer() size: " + a.length + " limit: " + dataBuffer.limit());

            String out = "";
            int count = 0;
            for (int i = 0; i < 64; i = i + 1) {
                out += a[i] + " ";

                count++;
                if (count == 16) {
                    LOG.info("  " + out);
                    out = "";
                    count = 0;
                }
            }
*/
            objectDigestCalculator.update(a, dataBuffer.position(), (dataBuffer.limit() - dataBuffer.position()));
        } else {
            objectDigestCalculator.update(dataBuffer.array(), dataBuffer.position(), (dataBuffer.limit() - dataBuffer.position()));
        }
    }

    public String getFinalDigest() {
        Digest objectDigest = new Digest(DigestAlgorithm.MD5, objectDigestCalculator.digest());
        return objectDigest.getBase64Encoded();
    }

}
