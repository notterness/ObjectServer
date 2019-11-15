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

    public ByteBuffer getBuffer() {
        return buffer;
    }

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

        System.out.println("setReadState() current: " + bufferState.toString() + " new: " + newState.toString() +
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
                System.out.println("ERROR: setReadState() invalid current state: " + bufferState.toString());
            }
        }
    }

    /*
    ** This will replace the current ByteBuffer with the newBuffer in the BufferState. The previous
    **   ByteBuffer will be released back to the memory free pool.
     */
    public void swapByteBuffers(ByteBuffer newBuffer) {
        ByteBuffer oldBuffer;

        oldBuffer = buffer;
        buffer = newBuffer;

        /*
        ** TODO: The oldBuffer needs to be released back to the memory pool. But, there is currently
        **   an ownership problem with the memory that backs the ByteBuffers as they share the same
        **   backing memory and it cannot be given back to the pool until the new reference is done
        **   with it.
        **   One solution (since this should not be for a very big piece of memory) is to perform a
        **     new allocation and a copy in the StringChunk class.
        **   Also, need to have access to the MemoryAllocator that is in charge of this buffer.
         */
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
