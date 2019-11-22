package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.connectionstate.WebServerConnState;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StatusWriteCompletion extends WriteCompletion {

    private static final Logger LOG = LoggerFactory.getLogger(StatusWriteCompletion.class);

    private WebServerConnState connectionState;
    private WriteConnection serverWriteConn;

    public StatusWriteCompletion(WebServerConnState work, WriteConnection conn, ByteBuffer userData, long transactionId, final int bytesToWrite, final int startingByte) {
        super(userData, transactionId, bytesToWrite, startingByte);

        connectionState = work;
        serverWriteConn = conn;
    }

    @Override
    public void writeCompleted(final int bytesXfr, final long transaction) {

        int result = bytesXfr;
        LOG.info("StatusWriteCompletion writeCompleted() " + bytesXfr + " transaction: " + transaction);

        // Convert the result into a more useful value to indicate completion
        if (bytesXfr > 0) {

            bytesWritten += bytesXfr;
            currStartingByte += bytesXfr;
            if (bytesWritten == totalBytesToWrite) {
                LOG.info("StatusWriteCompletion writeCompleted() all bytes written " + bytesXfr + " transaction: " + transaction);
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
