package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.connectionstate.*;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Iterator;

public class ClientConnState extends ConnectionState {

    /*
    **
     */
    private ConnectionPipelineMgr pipelineManager;

    /*
     ** The following is used for client sockets reads to know what to callback when data is
     **   read from the socket. It is not used for server side connections.
     */
    private ClientDataReadCallback clientDataReadCallback;

    /*
     ** The following is used to release this ConnectionState back to the free pool.
     */
    private ConnectionStatePool<ClientConnState> connectionStatePool;

    /*
    ** clientCallbackCompleted is set to true when all of the client callbacks have been
    **   made for the buffers
     */
    private boolean clientCallbackCompleted;


    ClientConnState(final ConnectionStatePool<ClientConnState> pool, final int uniqueId) {

        super(uniqueId);

        connectionStatePool = pool;
        clientDataReadCallback = null;

        clientCallbackCompleted = false;
    }

    public void start() {

        super.start();

        pipelineManager = new ClientPutPipelineMgr(this);
    }

    public void stateMachine() {
        ConnectionStateEnum overallState;

        overallState = pipelineManager.nextPipelineStage();

        switch (overallState) {
            case INITIAL_SETUP:
                super.setupInitial();
                System.out.println("ClientConnState[" + getConnStateId() + "] INITIAL_SETUP client");

                addRequestedDataBuffer();

                addToWorkQueue(false);
                break;

            case CHECK_SLOW_CHANNEL:
                if (timeoutChecker.inactivityThresholdReached()) {
                    /*
                     ** TOTDO: Need to close out the channel and this connection
                     */
                } else {
                    overallState = pipelineManager.nextPipelineStage();

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
                if (allocClientReadBufferState() == 0) {
                    addToWorkQueue(false);
                    break;
                }

                /*
                ** If buffers were allocated, advance the Connection state and fall through to
                **   ConnectionStateEnum.READ_CLIENT_DATA
                */

            case READ_CLIENT_DATA:
                readIntoDataBuffers();
                break;

            case CLIENT_READ_CB:
                readClientBufferCallback();

                addToWorkQueue(false);
                break;

            case CONN_FINISHED:
                System.out.println("ClientConnState[" + getConnStateId() + "] CONN_FINISHED");
                reset();

                // Now release this back to the free pool so it can be reused
                connectionStatePool.freeConnectionState(this);
                break;
        }
    }


    /*
     ** This walks through the array of BufferState to find the next one that has completed
     **   the read of data into it and returns it.
     ** NOTE: The expectation is that when the Java NIO.2 read operation performs it's callback,
     **   there is no more data to be transferred into the buffer being read into.
     */
    private void readClientBufferCallback() {
        int readsCompleted = getDataBufferReadsCompleted();

        System.out.println("ClientConnState[" + getConnStateId() + "] readsCompleted: " + readsCompleted);

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
    ** Returns if the client callback associated with a data read has completed
     */
    boolean hasClientCallbackCompleted() {
        return clientCallbackCompleted;
    }

    void resetClientCallbackCompleted() {
        clientCallbackCompleted = false;
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
    public void readCompletedError(final BufferState bufferState) {
        channelError.set(true);
        int dataReadCount = outstandingDataReadCount.decrementAndGet();

        /*
        ** Need to increment the number of data buffer reads completed so that the clients will
        **   receive their callback with an error for the buffer.
         */
        try {
            dataReadDoneQueue.put(bufferState);
            dataBufferReadsCompleted.incrementAndGet();
        } catch (InterruptedException int_ex) {
            System.out.println("ERROR ClientConnState[" + getConnStateId() + "] readCompletedError() " + int_ex.getMessage());
        }

        System.out.println("ClientConnState[" + getConnStateId() + "].readCompletedError() dataReadCount: " + dataReadCount);

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
    public void setReadState(final BufferState bufferState, final BufferStateEnum newState) {

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

                System.out.println("ClientConnState[" + getConnStateId() + "] setChannel() addr: " + addr);
            } catch (IOException io_ex) {
                System.out.println("socket closed " + io_ex.getMessage());
            }
        } else {
            System.out.println("ClientConnState[" + getConnStateId() + "] setChannel(null)");

        }

        super.setAsyncChannel(chan);
    }

    void setClientReadCallback(ClientDataReadCallback clientReadCb) {
        System.out.println("ClientConnState[" + getConnStateId() + "] setClientReadCallback()");

        clientDataReadCallback = clientReadCb;
    }

    private void callClientReadCallback(final int status, final ByteBuffer readBuffer) {
        clientDataReadCallback.dataBufferRead(status, readBuffer);
    }

    public void clearChannel() {
        clientDataReadCallback = null;

        clientCallbackCompleted = false;

        super.clearChannel();
    }

}
