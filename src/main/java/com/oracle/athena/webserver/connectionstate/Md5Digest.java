package com.oracle.athena.webserver.connectionstate;

import com.oracle.oci.casper.jopenssl.CryptoMessageDigest;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.encryption.DigestUtils;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;

import java.nio.ByteBuffer;

public class Md5Digest {

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
