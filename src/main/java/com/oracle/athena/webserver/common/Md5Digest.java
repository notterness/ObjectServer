package com.oracle.athena.webserver.common;

import com.oracle.oci.casper.jopenssl.CryptoMessageDigest;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.encryption.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class Md5Digest {

    private static final Logger LOG = LoggerFactory.getLogger(Md5Digest.class);

    private Digest expectedDigest;

    private final CryptoMessageDigest objectDigestCalculator;

    private final String expectedMD5;

    public Md5Digest() {
        /*
         ** Feed this ByteBuffer through the MD5 calculator
         */
        expectedDigest = null;
        //final String expectedMD5 = ChecksumHelper.getContentMD5Header(request).orElse(null);
        expectedMD5 = null;

        objectDigestCalculator = DigestUtils.messageDigestFromAlgorithm(DigestAlgorithm.MD5);
        if (expectedMD5 != null) {
            expectedDigest = Digest.fromBase64Encoded(DigestAlgorithm.MD5, expectedMD5);
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
            objectDigestCalculator.update(a);
        } else {
            objectDigestCalculator.update(dataBuffer.array());
        }
    }

    public String getFinalDigest() {
        Digest objectDigest = new Digest(DigestAlgorithm.MD5, objectDigestCalculator.digest());
        return objectDigest.getBase64Encoded();
    }

}
