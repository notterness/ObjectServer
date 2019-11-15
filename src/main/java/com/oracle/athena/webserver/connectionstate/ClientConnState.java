package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.server.ClientDataReadCallback;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Iterator;

public class ClientConnState extends ConnectionState {

    /*
     ** The following is used for client sockets reads to know what to callback when data is
     **   read from the socket. It is not used for server side connections.
     */
    private ClientDataReadCallback clientDataReadCallback;

    /*
     ** The following is used to release this ConnectionState back to the free pool.
     */
    ConnectionStatePool<ClientConnState> connectionStatePool;

    /*
    ** clientCallbackCompleted is set to true when all of the client callbacks have been
    **   made for the buffers
     */
    private boolean clientCallbackCompleted;


    public ClientConnState(final ConnectionStatePool<ClientConnState> pool, final int uniqueId) {

        super(uniqueId);

        connectionStatePool = pool;
        clientDataReadCallback = null;

        clientCallbackCompleted = false;
    }

    public void start() {
        super.start();
    }

    public void stateMachine() {

        /*
         ** First determine the state to execute
         */
        if (overallState != ConnectionStateEnum.INITIAL_SETUP) {
            overallState = determineNextState();
            System.out.println("ClientConnState[" + connStateId + "] state: " + overallState.toString());
        }

        switch (overallState) {
            case INITIAL_SETUP:
                setupInitial();
                System.out.println("ClientConnState[" + connStateId + "] INITIAL_SETUP client");

                requestedDataBuffers = 1;

                /*
                 ** Have to advance past INITIAL_SETUP to prevent the state machine from getting stuck
                 */
                overallState = ConnectionStateEnum.CHECK_SLOW_CHANNEL;
                addToWorkQueue(false);
                break;

            case CHECK_SLOW_CHANNEL:
                if (timeoutChecker.inactivityThresholdReached()) {
                    /*
                     ** TOTDO: Need to close out the channel and this connection
                     */
                } else {
                    overallState = determineNextState();

                    /*
                     ** Need to wait for something to kick the state machine to a new state
                     **
                     ** The ConnectionState will get put back on the execution queue when an external
                     **   operation completes.
                     */
                    if (overallState != ConnectionStateEnum.CHECK_SLOW_CHANNEL) {
                        addToWorkQueue(false);
                    } else {
                        addToWorkQueue(true);
                    }
                }
                break;

            case READ_DONE:
                // TODO: Assert() if this state is ever reached
                break;

            case READ_DONE_ERROR:
                // Release all the outstanding buffer
                releaseBufferState();
                addToWorkQueue(false);
                break;

            case ALLOC_CLIENT_DATA_BUFFER:
                if (allocClientReadBufferState() > 0) {
                    // advance the Connection state and fall through
                    setOverallState(ConnectionStateEnum.READ_CLIENT_DATA);
                } else {
                    addToWorkQueue(false);
                    break;
                }

            case READ_CLIENT_DATA:
                readIntoDataBuffers();
                break;

            case CLIENT_READ_CB:
                readClientBufferCallback();

                addToWorkQueue(false);
                break;

            case CONN_FINISHED:
                System.out.println("ClientConnState[" + connStateId + "] CONN_FINISHED");
                reset();

                // Now release this back to the free pool so it can be reused
                connectionStatePool.freeConnectionState(this);
                break;
        }
    }

    /*
     ** This function is used to determine the next state for the ConnectionState processing.
     **
     ** NOTE: This is only called from the start of the state machine.
     */
    private ConnectionStateEnum determineNextState() {

         /*
         ** Are there outstanding buffers to be allocated. If the code had attempted to allocate
         **   buffers and failed, check if there is other work to do. No point trying the buffer
         **   allocation right away.
         **
         */
        boolean outOfMemory = bufferAllocationFailed.get();
        if (!outOfMemory) {
            if (requestedDataBuffers > 0) {
                return ConnectionStateEnum.ALLOC_CLIENT_DATA_BUFFER;
            }
        }

        /*
         ** The NIO.2 AsynchronousChannelRead can only have a single outstanding read at at time.
         **
         ** TODO: Support the NIO.2 read that can be passed in an array of ByteBuffers
         */
        if ((allocatedDataBuffers > 0)  && (outstandingDataReadCount.get() == 0)){
            return ConnectionStateEnum.READ_CLIENT_DATA;
        }

        int dataReadComp = dataBufferReadsCompleted.get();
        if (dataReadComp > 0) {
            return ConnectionStateEnum.CLIENT_READ_CB;
        }

        /*
        ** The check for clientCallbackCompleted needs to be before the contentAllRead check
        **   otherwise the Connection will get stuck in the CLIENT_READ_CB state.
        **
        ** TODO: Is there value to moving the repsonse parsing into this state machine to properly handle
        **   the setting of the contentAllRead flag. Currently, it is never sent as the "Content-Length" is
        **   never parsed out of the HTTP response.
         */
        if (clientCallbackCompleted == true) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        /*
         ** Check if the content has all been read in and then proceed to finishing the processing
         **
         ** TODO: Start adding in the steps to process the content data instead of just sending status
         */
        boolean doneReadingContent = contentAllRead.get();
        if (doneReadingContent) {
            return ConnectionStateEnum.CLIENT_READ_CB;
        }

        /*
         ** Check if there was a channel error and cleanup if there are no outstanding
         **    reads.
         */
        if (channelError.get()) {
            int dataReadsPending = outstandingDataReadCount.get();

            if (dataReadsPending == 0) {
                return ConnectionStateEnum.CONN_FINISHED;
            }
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }

    /*
     ** This walks through the array of BufferState to find the next one that has completed
     **   the read of data into it and returns it.
     ** NOTE: The expectation is that when the Java NIO.2 read operation performs it's callback,
     **   there is no more data to be transferred into the buffer being read into.
     */
    private void readClientBufferCallback() {
        int readsCompleted = dataBufferReadsCompleted.get();

        System.out.println("ClientConnState[" + connStateId + "] readsCompleted: " + readsCompleted);

        if (readsCompleted > 0) {
            Iterator<BufferState> iter = dataReadDoneQueue.iterator();
            BufferState readDataBuffer;

            while (iter.hasNext()) {
                readDataBuffer = iter.next();
                iter.remove();

                // TODO: Assert ((state == BufferStateEnum.READ_DONE) || (state == BufferStateEnum.READ_ERROR))

                dataBufferReadsCompleted.decrementAndGet();

                /*
                 ** Callback the client
                 */
                if (readDataBuffer.getBufferState() == BufferStateEnum.READ_DATA_DONE) {
                    callClientReadCallback(0, readDataBuffer.getBuffer());
                } else {
                    callClientReadCallback(-1, null);
                }
            }
        }

        clientCallbackCompleted = true;
    }

    /*
     ** The following is used to handle the case when a read error occurred and the ConnectionState needs
     **   to cleanup and release resources.
     **
     ** The call path to get here is:
     **    ServerWorkerThread chan.read() failed() callback
     **      --> BufferState.setReadState()
     **
     */
    @Override
    void readCompletedError(final BufferState bufferState) {
        int dataReadCount = 0;

        channelError.set(true);
        dataReadCount = outstandingDataReadCount.decrementAndGet();

        /*
        ** Need to increment the number of data buffer reads completed so that the clients will
        **   receive their callback with an error for the buffer.
         */
        try {
            dataReadDoneQueue.put(bufferState);
            dataBufferReadsCompleted.incrementAndGet();
        } catch (InterruptedException int_ex) {
            System.out.println("ERROR ClientConnState[" + connStateId + "] readCompletedError() " + int_ex.getMessage());
        }

        System.out.println("ClientConnState[" + connStateId + "].readCompletedError() dataReadCount: " + dataReadCount +
                " overallState: " + overallState);

        /*
         ** If there are outstanding reads in progress, need to wait for those to
         **   complete before cleaning up the ConnectionState
         */
        if (dataReadCount == 0){
            addToWorkQueue(false);
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
    void setReadState(final BufferState bufferState, final BufferStateEnum newState) {

        BufferStateEnum currBufferState = bufferState.getBufferState();

        bufferState.setReadState(newState);

        if (newState == BufferStateEnum.READ_ERROR) {
            /*
             ** All the data has been read that will be read, but there is no point processing it as there
             **   was a channel error. Need to tell the ConnectionState to clean up and terminate this
             **   connection.
             */
            readCompletedError(bufferState);
        } else if (currBufferState == BufferStateEnum.READ_WAIT_FOR_DATA) {
                // Read of all the data is completed
                dataReadCompleted(bufferState);
        } else {
                System.out.println("ERROR: setReadState() invalid current state: " + bufferState.toString());
        }
    }


    /*
     ** This sets up the Connection for reading data for a client.
     */
    @Override
    public void setChannel(final AsynchronousSocketChannel chan) {
        if (chan != null) {
            try {
                SocketAddress addr = chan.getLocalAddress();

                System.out.println("ClientConnState[" + connStateId + "] setChannel() addr: " + addr);
            } catch (IOException io_ex) {
                System.out.println("socket closed " + io_ex.getMessage());
            }
        } else {
            System.out.println("ClientConnState[" + connStateId + "] setChannel(null)");

        }

        super.setAsyncChannel(chan);
        overallState = ConnectionStateEnum.INITIAL_SETUP;
    }

    public void setClientReadCallback(ClientDataReadCallback clientReadCb) {
        System.out.println("ClientConnState[" + connStateId + "] setClientReadCallback()");

        clientDataReadCallback = clientReadCb;
    }

    private void callClientReadCallback(final int status, final ByteBuffer readBuffer) {
        clientDataReadCallback.dataBufferRead(status, readBuffer);
    }

    void clearChannel() {
        clientDataReadCallback = null;

        clientCallbackCompleted = false;

        super.clearChannel();
    }

}
