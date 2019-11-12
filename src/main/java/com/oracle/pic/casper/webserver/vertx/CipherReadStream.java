package com.oracle.pic.casper.webserver.vertx;

import com.oracle.oci.casper.jopenssl.CryptoCipher;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.vertx.stream.ComposingReadStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import javax.annotation.Nonnull;

/**
 * Base class for {@link io.vertx.core.streams.ReadStream} that can encrypt & decrypt.
 */
public abstract class CipherReadStream extends ComposingReadStream<Buffer> {

    /**
     * Number of bytes in a block, which is constant for AES.
     */
    public static final int AES_BLOCK_SIZE = 16;

    CipherReadStream(@Nonnull ReadStream<Buffer> delegate) {
        super(delegate);
    }

    /**
     * Helper method that given a start value will return the offset that you should apply (subtract) from the start
     * value to make sure the offset lands at an AES block boundary.
     */
    public static long calculateAesStartOffset(long start) {
        //Compute how much we need to reduce the start, to get to an AES boundary
        return start % AES_BLOCK_SIZE;
    }

    /**
     * Helper method to convert a {@link ByteRange} to one that falls exactly on an AES block boundary.
     * It will possibly subtract from the start and add to the end if necessary.
     */
    public static ByteRange convertToAesBoundaries(ByteRange byteRange, long totalPayloadLength) {
        final long start = byteRange.getStart();
        final long end = byteRange.getEnd();
        final long aesBlockStart = start - calculateAesStartOffset(start);
        //Compute how much we need to expand the end, to get to an AES boundary
        long endDelta = end % AES_BLOCK_SIZE;
        long aesBlockEnd = Math.min(totalPayloadLength - 1, end + (AES_BLOCK_SIZE - endDelta));
        return new ByteRange(aesBlockStart, aesBlockEnd);
    }

    protected static Buffer updateCipher(@Nonnull CryptoCipher cipher, @Nonnull Buffer buffer) {
        ByteBuf inputBuf = buffer.getByteBuf();
        byte[] outputBuf;
        if (inputBuf.hasArray()) {
            byte[] backingArray = inputBuf.array();
            // whenever, we need to access the backing array, we should always use arrayOffset() to make an adjustment
            // to the object
            int readerIndex = inputBuf.arrayOffset() + inputBuf.readerIndex();
            int bufferLength = inputBuf.readableBytes();
            assert buffer.length() == bufferLength;
            outputBuf = cipher.update(backingArray, readerIndex, bufferLength);
        } else {
            // this is the unlikely code path. All the buffer in vert.x is backed by array
            byte[] in = new byte[inputBuf.readableBytes()];
            inputBuf.getBytes(inputBuf.readerIndex(), in);
            outputBuf = cipher.update(in);
        }
        assert outputBuf.length == buffer.length();
        return Buffer.buffer(Unpooled.wrappedBuffer(outputBuf));
    }

}
