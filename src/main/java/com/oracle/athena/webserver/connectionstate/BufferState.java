package com.oracle.athena.webserver.connectionstate;

import java.nio.ByteBuffer;

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

        System.out.println("setReadState(): current " + bufferState.toString() + " new " + newState.toString() +
                " remaining: " + buffer.remaining());

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

    /*
     ** This is called after the buffer has been run through the HTTP parser. Buffers used
     **   for the header can be released.
     **
     ** TODO: Need to figure out how data buffers are handled
     */
    public void setHttpParseDone() {
        System.out.println("setHttpParseDone(): current " + bufferState.toString() +
                " remaining: " + buffer.remaining());

        bufferState = BufferStateEnum.PARSE_HTTP_DONE;
    }

}
