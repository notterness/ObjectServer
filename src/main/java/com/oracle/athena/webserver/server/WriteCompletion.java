package com.oracle.athena.webserver.server;

import java.nio.ByteBuffer;

public class WriteCompletion {

    public ByteBuffer buffer;
    private long transactionId;

    /*
     ** For writes that may take multiple steps to complete
     */
    public int totalBytesToWrite;
    public int bytesWritten;
    public int bufferStartingByte;
    public int currStartingByte;


    public WriteCompletion(ByteBuffer userData, final long writeConnId, final int bytesToWrite, final int startingByte) {
        buffer = userData;
        transactionId = writeConnId;

        bytesWritten = 0;
        totalBytesToWrite = bytesToWrite;
        bufferStartingByte = startingByte;
        currStartingByte = startingByte;
    }

    ByteBuffer getBuffer() {
        return buffer;
    }

    public int getBufferSize() {
        return buffer.remaining();
    }

    int getRemainingBytesToWrite() {
        return (totalBytesToWrite - bytesWritten);
    }

    public int getStartingByte() {
        return currStartingByte;
    }

    /*
     ** This is the generic write completion callback. The expectation is that the
     **   various components that use the WriteConnection infrastructure will
     **   extend this to have an operation specific callback.
     */
    public void writeCompleted(final int result, final long transaction) {
        if (result == 0) {
            // Write completed without issue
        } else {
            // Write failed and will never be completed. In addition, the
            // transaction has failed and it is closed.
        }
    }
}
