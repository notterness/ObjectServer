package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.server.WriteCompletion;
import com.oracle.athena.webserver.server.WriteConnection;

import java.nio.ByteBuffer;


public class ClientWriteCompletion extends WriteCompletion {

    private ClientTest clientTest;
    private WriteConnection clientWriteConn;

    ClientWriteCompletion(ClientTest client, WriteConnection clientConn, ByteBuffer userData, long transactionId, final int bytesToWrite, final int startingByte) {
        super(userData, transactionId, bytesToWrite, startingByte);

        clientTest = client;
        clientWriteConn = clientConn;
    }

    @Override
    public void writeCompleted(final int bytesXfr, final long transaction) {

        int result = bytesXfr;

        System.out.println("ClientWriteCompletion writeCompleted() " + bytesXfr + " transaction: " + transaction);

        // Convert the result into a more useful value to indicate completion
        if (bytesXfr > 0) {

            bytesWritten += bytesXfr;
            currStartingByte += bytesXfr;
            if (bytesWritten == totalBytesToWrite) {

            } else {
                // The start write location has been updated, so simply queue this back up
                clientWriteConn.writeData(this);
            }
            result = 0;
        }

        if (result == 0) {
            // Write completed without issue
            clientTest.writeCompleted(0, buffer);
        } else {
            // Write failed and will never be completed. In addition, the
            // transaction has failed and it is closed. Need to tear down the
            // WriteConnection.
        }
    }
}
