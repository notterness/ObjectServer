package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.connectionstate.*;
import com.oracle.athena.webserver.statemachine.StateQueueResult;
import org.eclipse.jetty.http.HttpStatus;

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
        StateQueueResult result;

        result = pipelineManager.executePipeline();
        switch (result) {
            case STATE_RESULT_COMPLETE:
                // set next pipeline.
                addToWorkQueue(false);
                break;
            case STATE_RESULT_WAIT:
                addToWorkQueue(true);
                break;

            case STATE_RESULT_REQUEUE:
                addToWorkQueue(false);
                break;
            case STATE_RESULT_FREE:
                break;
        }
    }

    /*
     **
     */
    @Override
    public void setupInitial() {
        super.setupInitial();

        System.out.println("WebServerConnState[" + getConnStateId() + "] INITIAL_SETUP server");
        addRequestedDataBuffer();
    }


    /*
     ** This walks through the array of BufferState to find the next one that has completed
     **   the read of data into it and returns it.
     ** NOTE: The expectation is that when the Java NIO.2 read operation performs it's callback,
     **   there is no more data to be transferred into the buffer being read into.
     */
    void readClientBufferCallback() {
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
     ** Accessor function related to the HTTP Parser and when an error occurs.
     **
     ** getHttpParserError() will return HttpStatus.OK_200 if there is no error, otherwise it will return
     **   the value set to indicate the parsing error.
     **
     ** TODO: Need to wire the response parsing error in
     */
    public int getHttpParseStatus() {
        int parsingStatus = HttpStatus.OK_200;
        if (httpParsingError.get()) {
            parsingStatus = HttpStatus.OK_200;
        }

        return parsingStatus;
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
