package com.oracle.athena.webserver.connectionstate;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/*
 ** This is used to manage the individual states a ByteBuffer goes through
 */
public class BufferState {

    private ConnectionState connState;

    private ByteBuffer buffer;

    private BufferStateEnum bufferState;

    BufferState(final ConnectionState work) {
        bufferState = BufferStateEnum.INVALID_STATE;
        buffer = null;

        connState = work;
    }

    public void assignBuffer(final ByteBuffer userBuffer, final BufferStateEnum state) {
        buffer = userBuffer;
        bufferState = state;
    }

    public BufferStateEnum getBufferState() {
        return bufferState;
    }


    public ByteBuffer getCheckedBuffer(final BufferStateEnum expectedState) {
        if (bufferState == expectedState) {
            return buffer;
        }

        return null;
    }


    public ByteBuffer getBuffer() {
        return buffer;
    }

    public ConnectionState getConnState() {
        return connState;
    }

    /*
     ** This is called from the ServerWorkerThread chan.read() callback to update the state of the
     **   read buffer.
     **
     ** TODO: Need to handle error cases
     */
    public void setReadState(final BufferStateEnum newState) {

        System.out.println("setReadState(): current " + bufferState.toString() + " new " + newState.toString());

        bufferState = newState;

        if (newState == BufferStateEnum.READ_ERROR) {
            /*
             ** All the data has been read that will be read, but there is no point processing it as there
             **   was a channel error. Need to tell the ConnectionState to clean up and terminate this
             **   connection.
             */
            connState.readCompletedError();
        } else if (buffer.remaining() != 0) {
            // Read of all the data is completed
            connState.readCompleted(true);
        } else {
            connState.readCompleted(false);
        }
    }


    public String displayBuffer() {
        if (buffer.hasArray()) {
            return new String(buffer.array(),
                    buffer.arrayOffset() + buffer.position(),
                    buffer.remaining(), StandardCharsets.UTF_8);
        } else {
            final byte[] b = new byte[buffer.remaining()];
            buffer.duplicate().get(b);
            return new String(b);
        }
    }
}
