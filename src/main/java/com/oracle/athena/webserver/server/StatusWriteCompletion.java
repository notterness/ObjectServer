package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.ConnectionState;

import java.nio.ByteBuffer;


public class StatusWriteCompletion extends WriteCompletion {
    private ConnectionState connectionState;
    private WriteConnection serverWriteConn;

    public StatusWriteCompletion(ConnectionState work, WriteConnection conn, ByteBuffer userData, long transactionId, final int bytesToWrite, final int startingByte) {
        super(userData, transactionId, bytesToWrite, startingByte);

        connectionState = work;
        serverWriteConn = conn;
    }

    @Override
    public void writeCompleted(final int bytesXfr, final long transaction) {

        int result = bytesXfr;
        System.out.println("StatusWriteCompletion writeCompleted() " + bytesXfr + " transaction: " + transaction);

        // Convert the result into a more useful value to indicate completion
        if (bytesXfr > 0) {

            bytesWritten += bytesXfr;
            currStartingByte += bytesXfr;
            if (bytesWritten == totalBytesToWrite) {
                System.out.println("StatusWriteCompletion writeCompleted() all bytes written " + bytesXfr + " transaction: " + transaction);
            } else {
                // The start write location has been updated, so simply queue this back up
                serverWriteConn.writeData(this);
            }
            result = 0;
        }

        if (result == 0) {
            // Write completed without issue
            connectionState.statusWriteCompleted(0, buffer);
        } else {
            // Write failed and will never be completed. In addition, the
            // transaction has failed and it is closed. Need to tear down the
            // WriteConnection.
        }
    }

}
