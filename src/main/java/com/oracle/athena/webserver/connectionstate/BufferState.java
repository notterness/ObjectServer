package com.oracle.athena.webserver.connectionstate;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 ** This is used to manage the individual states a ByteBuffer goes through
 */
public class BufferState {

    private static final Logger LOG = LoggerFactory.getLogger(BufferState.class);

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

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int getOwnerId() { return connState.getConnStateId(); }

    public ConnectionStateEnum getConnectionNextState() { return connState.getNextState(); }

    /*
    ** This is used to set the BufferState to a waiting for a read to complete state. This is called
    **   just prior to a read from channel operation starting in ConnectionState.
    **
    ** TODO: Need to make the update of the bufferState thread safe
     */
    public void setReadInProgress() {
         if (bufferState == BufferStateEnum.READ_HTTP_FROM_CHAN) {
            bufferState = BufferStateEnum.READ_WAIT_FOR_HTTP;
        } else if (bufferState == BufferStateEnum.READ_DATA_FROM_CHAN) {
            bufferState = BufferStateEnum.READ_WAIT_FOR_DATA;
        } else {
             // TODO: Add Assert((bufferState == READ_HTTP_FROM_CHAN) || (bufferState == READ_DATA_FROM_CHAN))

        }
    }

    /*
     ** This is called from the ServerWorkerThread chan.read() callback to update the state of the
     **   read buffer.
     **
     ** NOTE: This can only be called with the newState set to:
     **   BufferStateEnum.READ_DONE -> read completed without errors
     **   BufferStateEnum.READ_ERROR -> An error occurred while performing the read and the data in the buffer
     **     must be considered invalid.
     **
     ** TODO: Need to make the following thread safe when it modifies BufferState
     */
    public void setReadState(final BufferStateEnum newState) {

        LOG.info("[" + connState.getConnStateId() + "] setReadState() current: " + bufferState.toString() + " new: " + newState.toString() +
                " remaining: " + buffer.remaining());

        if (newState == BufferStateEnum.READ_ERROR) {
            /*
             ** All the data has been read that will be read, but there is no point processing it as there
             **   was a channel error. Need to tell the ConnectionState to clean up and terminate this
             **   connection.
             */
            bufferState = BufferStateEnum.READ_ERROR;
        } else {
            if (bufferState == BufferStateEnum.READ_WAIT_FOR_HTTP) {
                // Read of all the data is completed
                bufferState = BufferStateEnum.READ_HTTP_DONE;
            } else if (bufferState == BufferStateEnum.READ_WAIT_FOR_DATA) {
                // Read of all the data is completed
                bufferState = BufferStateEnum.READ_DATA_DONE;
            } else {
                LOG.info("ERROR: [" + connState.getConnStateId() + "] setReadState() invalid current state: " + bufferState.toString());
            }
        }
    }

    /*
    ** This will copy the contents of the newBuffer into the ByteBuffer associated with the BufferState.
     */
    public void copyByteBuffer(ByteBuffer srcBuffer) {
        LOG.info("copyByteBuffer() newBuffer remaining: " + srcBuffer.remaining() + " limit: " +
                srcBuffer.limit() + " position: " + srcBuffer.position());

        int nBytesToCopy = Math.min(buffer.remaining(), srcBuffer.remaining());
        if (!buffer.isDirect()) {
            buffer.put(srcBuffer.array(), srcBuffer.arrayOffset() + srcBuffer.position(), nBytesToCopy);
        } else {
            /*
            ** Need to perform manual copy (Need to handle the allocate direct issue better)
             */
            for (int i = 0; i < nBytesToCopy; i++) {
                buffer.put(srcBuffer.get(srcBuffer.position() + i));
            }

            buffer.limit(nBytesToCopy);
            buffer.position(0);
        }
    }

    /*
     ** This is called after the buffer has been run through the HTTP parser. Buffers used
     **   for the header can be released.
     **
     ** TODO: Need to figure out how data buffers are handled
     */
    public void setBufferHttpParseDone() {
        LOG.info("setBufferHttpParseDone(): current " + bufferState.toString() +
                " remaining: " + buffer.remaining());

        bufferState = BufferStateEnum.PARSE_HTTP_DONE;
    }

}
