package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.http.BuildHttpResult;
import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.*;
import org.eclipse.jetty.http.HttpStatus;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/*
 ** This is used to keep track of the state of the connection for its lifetime.
 **
 ** chan is the socket that the connection is using to transfer data. It is assigned
 **   later and not at instantiation time to allow the ConnectionState objects to be
 **   managed via a pool.
 */
public class ConnectionState {

    /*
     ** maxOutstandingBuffers is how many operations can be done in parallel on this
     **   connection. This is to limit one connection from using all the buffers and
     **   all of the processing resources.
     */
    public static final int MAX_OUTSTANDING_BUFFERS = 10;

    /*
    ** This is how long the ConnectionState should wait until if goes back through the
    **   CHECK_SLOW_CHANNEL state if no other activity is taking place.
     */
    private static final long TIME_TILL_NEXT_TIMEOUT_CHECK = 500;

    /*
    ** connStateId is used as a way to track a ConnectionState in logging statements. Each
    **   ConnectionState has a unique ID to identify it for the life of the program.
     */
    private int connStateId;

    private AsynchronousSocketChannel connChan;

    /*
    ** This allows the ConnectionState to run as a target (Server) or a client. The flag changes what the
    **   ConnectionState does first (i.e. read in an HTTP header or just wait for data to be transferred).
     */
    private boolean serverConn;

    /*
     ** This is the WriteConnection used to write responses and return data on the server connection
     */
    private WriteConnection writeConn;

    /*
    ** overallState is used to drive the ConnectionState through the operational state machine.
     */
    private ConnectionStateEnum overallState;

    private long nextExecuteTime;

    /*
     ** The following is the complete information for the HTTP connection
     */
    private CasperHttpInfo casperHttpInfo;

    /*
    ** The following are used to track the number of outstanding reads in progress. There can
    **   be HTTP header reads or Data reads in progress.
    **
    ** NOTE: There should never be both HTTP header reads and Data reads in progress at the same
    **   time. The HTTP header reads should all be completed prior to starting Data reads.
     */
    private AtomicInteger outstandingHttpReadCount;
    private AtomicInteger outstandingDataReadCount;

    private AtomicBoolean httpHeaderParsed;

    /*
     ** The following is needed to allow the ConnectionState to allocate buffers to send responses.
     **
     ** dataResponseSent is set to true when the HTTP response header has been sent.
     */
    private BufferState responseBuffer;
    private AtomicBoolean dataResponseSent;

    /*
     ** There is an ByteBufferHttpParser per Connection since each parser keeps its own state.
     */
    private ByteBufferHttpParser httpParser;

    /*
     ** The following is used for client sockets reads to know what to callback when data is
     **   read from the socket. It is not used for server side connections.
     */
    private ClientDataReadCallback clientDataReadCallback;

    /*
    ** The following are used to insure that a ConnectionState is never on more than one queue and that
    **   if there is a choice between being on the timed wait queue (connOnDelayedQueue) or the normal
    **   execution queue (connOnExecutionQueue) is will always go on the execution queue.
     */
    private boolean connOnDelayedQueue;
    private boolean connOnExecutionQueue;

    private QueueMutex queueMutex;

    /*
     **
     */
    private int requestedHttpBuffers;
    private int allocatedHttpBufferCnt;
    private int requestedDataBuffers;
    private int allocatedDataBuffers;

    private AtomicBoolean bufferAllocationFailed;

    /*
     ** The next four items are associated with the thread that is running the ConnectionState
     **   state machine.
     */
    private ServerWorkerThread workerThread;
    private BufferStatePool bufferStatePool;
    private WriteConnThread writeThread;
    private BuildHttpResult resultBuilder;

    /*
    ** Using a different queue to manage each of the BufferState resources in the various states.
    **   The states for the buffer are currently:
    **     -> READ_FROM_CHAN - This means the buffer is ready to have data read into it
    **     -> READ_WAIT_FOR_DATA - Not currently used, but should be the state while waiting for data
    **     -> READ_DONE - The read into the buffer is complete. The buffer can either be placed on the
    **           waiting to be parsed (HTTP Parser) queue or the (what queue is next for content data)
    **     -> READ_ERROR - The read failed so the connection needs t be cleaned up.
     */

    /*
    ** The following queue is used to hold allocated BufferStates and it is only accessed from the main
    **   work thread so it does not need to be thread safe.
     */
    private LinkedList<BufferState> allocatedDataBufferQueue;

    private LinkedList<BufferState> allocatedHttpBufferQueue;

    private AtomicInteger httpBufferReadsCompleted;
    private boolean initialHttpBuffer;
    private BlockingQueue<BufferState> httpReadDoneQueue;

    private AtomicInteger dataBufferReadsCompleted;
    private BlockingQueue<BufferState> dataReadDoneQueue;

    /*
    ** The following is used to indicate that there was a channel error and the connection should be
    **   closed out.
     */
    private AtomicBoolean channelError;


    /*
    ** The following is used to check that progress is being made on a channel and the
    **   client is not slow sending or receiving data.
     */
    private TimeoutChecker timeoutChecker;

    /*
    ** The following is used to release this ConnectionState back to the free pool.
     */
    private ConnectionStatePool connectionStatePool;


    ConnectionState(final ConnectionStatePool pool, final int uniqueId) {
        overallState = ConnectionStateEnum.INVALID_STATE;

        outstandingHttpReadCount = new AtomicInteger(0);
        outstandingDataReadCount = new AtomicInteger(0);

        connectionStatePool = pool;

        connStateId = uniqueId;

        writeConn = null;
        bufferStatePool = null;
        resultBuilder = null;

        clientDataReadCallback = null;

        responseBuffer = null;
        dataResponseSent = new AtomicBoolean(false);

        httpHeaderParsed = new AtomicBoolean(false);

        channelError = new AtomicBoolean(false);

        serverConn = true;

        connOnDelayedQueue = false;
        connOnExecutionQueue = false;
        queueMutex = new QueueMutex();

        requestedDataBuffers = 0;
        allocatedDataBuffers = 0;
        allocatedDataBufferQueue = new LinkedList<>();

        requestedHttpBuffers = 0;
        allocatedHttpBufferCnt = 0;
        bufferAllocationFailed = new AtomicBoolean(false);
        allocatedHttpBufferQueue = new LinkedList<>();

        httpBufferReadsCompleted = new AtomicInteger(0);
        initialHttpBuffer = true;
        httpReadDoneQueue = new LinkedBlockingQueue<>(MAX_OUTSTANDING_BUFFERS * 2);

        dataBufferReadsCompleted = new AtomicInteger(0);
        dataReadDoneQueue = new LinkedBlockingQueue<>(MAX_OUTSTANDING_BUFFERS * 2);
    }

    /*
     **
     */
    void start() {

        /*
         ** The CasperHttpInfo keeps track of the details of a particular
         **   HTTP transfer and the parsed information.
         */
        casperHttpInfo = new CasperHttpInfo(this);

        httpParser = new ByteBufferHttpParser(casperHttpInfo);

        timeoutChecker = new TimeoutChecker();
    }

    public void stateMachine() {
        switch (overallState) {
            case INITIAL_SETUP:
                bufferStatePool = workerThread.getBufferStatePool();
                writeThread = workerThread.getWriteThread();
                resultBuilder = workerThread.getResultBuilder();

                if (serverConn) {
                    requestedHttpBuffers = 1;
                    overallState = ConnectionStateEnum.ALLOC_HTTP_BUFFER;
                } else {
                    System.out.println("ServerWorkerThread(" + connStateId + ") INITIAL_SETUP client");

                    requestedDataBuffers = 1;
                    overallState = ConnectionStateEnum.ALLOC_CLIENT_DATA_BUFFER;
                    addToWorkQueue(false);
                    break;
                }

                // Fall through
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

            case ALLOC_HTTP_BUFFER:
                /*
                 ** Do not continue if there are no buffers allocated. allocHttpBufferState() returns
                 **   the number of buffers that were allocated.
                 */
                if (allocHttpBufferState() == 0) {
                    /*
                    ** There may be other work that can be done while waiting to allocate buffers
                     */
                    overallState = determineNextState();
                    addToWorkQueue(false);
                    break;
                } else {
                    // Fall through
                    overallState = ConnectionStateEnum.READ_HTTP_BUFFER;
                }

            case READ_HTTP_BUFFER:
                readIntoMultipleBuffers();
                overallState = determineNextState();
                addToWorkQueue(false);
                break;

            case PARSE_HTTP_BUFFER:
                parseHttp();

                boolean headerReady = httpHeaderParsed.get();

                if (headerReady) {
                    setOverallState(ConnectionStateEnum.SEND_XFR_DATA_RESP);
                } else {
                    overallState = determineNextState();
                }
                addToWorkQueue(false);
                break;

            case SEND_XFR_DATA_RESP:
                // Send the response to the client to request they send data
                sendResponse();

                overallState = determineNextState();
                addToWorkQueue(false);
                break;

            case READ_FROM_CHAN:
                setOverallState(ConnectionStateEnum.CONN_FINISHED);
                addToWorkQueue(false);
                break;

            case READ_DONE:
                // TODO: Assert() if this state is ever reached
                break;

            case READ_DONE_ERROR:
                // Release all the outstanding buffer
                releaseBufferState();
                setOverallState(ConnectionStateEnum.CONN_FINISHED);
                addToWorkQueue(false);
                break;

            case ALLOC_CLIENT_DATA_BUFFER:
                if (allocClientReadBufferState() > 0) {
                    // advance the Connection state and fall through
                    setOverallState(ConnectionStateEnum.READ_CLIENT_DATA);
                } else {
                    overallState = determineNextState();
                    addToWorkQueue(false);
                    break;
                }

            case READ_CLIENT_DATA:
                readIntoDataBuffers();
                break;

            case CLIENT_READ_CB:
                readClientBufferCallback();

                setOverallState(ConnectionStateEnum.CONN_FINISHED);
                addToWorkQueue(false);
                break;

            case CONN_FINISHED:
                System.out.println("ConnectionState[" + connStateId + "] CONN_FINISHED");
                reset();

                // Now release this back to the free pool so it can be reused
                connectionStatePool.freeConnectionState(this);
                break;
        }
    }

    /*
    ** This function is used to determine the next state for the ConnectionState processing.
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
            if (requestedHttpBuffers > 0) {
                return ConnectionStateEnum.ALLOC_HTTP_BUFFER;
            }

            if (requestedDataBuffers > 0) {
                return ConnectionStateEnum.ALLOC_CLIENT_DATA_BUFFER;
            }
        }

        /*
        ** Are there buffers waiting to have a read performed?
         */
        if (allocatedHttpBufferCnt > 0) {
            return ConnectionStateEnum.READ_HTTP_BUFFER;
        }

        if (allocatedDataBuffers > 0) {
            return ConnectionStateEnum.READ_FROM_CHAN;
        }

        /*
        ** Are there completed reads, priority is processing the HTTP header
         */
        int httpReadComp = httpBufferReadsCompleted.get();
        if (httpReadComp > 0) {
            return ConnectionStateEnum.PARSE_HTTP_BUFFER;
        }

        int dataReadComp = dataBufferReadsCompleted.get();
        if (dataReadComp > 0) {
            return ConnectionStateEnum.CLIENT_READ_CB;
        }

        /*
        ** Check if there was a channel error and cleanup if there are no outstanding
        **    reads.
         */
        if (channelError.get()) {
            int httpReadsPending = outstandingHttpReadCount.get();
            int dataReadsPending = outstandingDataReadCount.get();

            if ((httpReadsPending == 0) && (dataReadsPending == 0)) {
                return ConnectionStateEnum.CONN_FINISHED;
            }
        }

        /*
        ** TODO: This is not really the exit point for the state machine, but until the
        **   steps for dealing with user data are added it is.
         */
        if (dataResponseSent.get()) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }

    /*
    ** This is used to add the ConnectionState to the worker thread's execute queue. The
    **   ConnectionState can be added to the immediate execution queue or the delayed
    **   execution queue.
     */
    public void addToWorkQueue(final boolean delayedExecution) {

        synchronized (queueMutex) {
            if (delayedExecution) {
                /*
                 ** If this ConnectionState is already on a queue, it cannot be added
                 **   to the delayed execution queue.
                 */
                if (!connOnExecutionQueue) {
                    connOnDelayedQueue = true;
                    nextExecuteTime = System.currentTimeMillis() + TIME_TILL_NEXT_TIMEOUT_CHECK;
                    workerThread.addToTimedQueue(this);
                }
            } else {
                if (connOnDelayedQueue) {
                    /*
                     ** Need to remove this from the delayed queue and then put it on the execution
                     **   queue
                     */
                    connOnDelayedQueue = false;
                    workerThread.removeFromTimedWaitQueue(this);
                }

                connOnExecutionQueue = true;
                workerThread.put(this);
            }
        }
    }

    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {

        synchronized (queueMutex) {
            if (delayedExecutionQueue) {
                connOnDelayedQueue = false;
                nextExecuteTime = 0;
            } else {
                connOnExecutionQueue = false;
            }
        }
    }

    public boolean hasWaitTimeElapsed() {
        long currTime = System.currentTimeMillis();

        if (currTime < nextExecuteTime) {
            return false;
        }

        return true;
    }


    /*
     ** Allocate a buffer to read HTTP header information into and associate it with this ConnectionState
     **
     ** The requestedHttpBuffers is not passed in since it is used to keep track of the number of buffers
     **   needed by this connection to perform another piece of work. The idea is that there may not be
     **   sufficient buffers available to allocate all that are requested, so there will be a wakeup call
     **   when buffers are available and then the connection will go back and try the allocation again.
     */
    private int allocHttpBufferState() {
        while (requestedHttpBuffers > 0) {
            BufferState bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_HTTP_FROM_CHAN, MemoryManager.SMALL_BUFFER_SIZE);
            if (bufferState != null) {
                allocatedHttpBufferQueue.add(bufferState);

                allocatedHttpBufferCnt++;
                requestedHttpBuffers--;
            } else {
                /*
                ** Unable to allocate memory, come back later
                 */
                bufferAllocationFailed.set(true);
                break;
            }
        }

        return allocatedHttpBufferCnt;
    }

    /*
    ** This is the function to add BufferState to the available queue. This means the BufferState are
    **   now ready to have data read into them.
     */
    private int allocClientReadBufferState() {
        System.out.println("ServerWorkerThread(" + connStateId + ") allocClientReadBufferState(1) " + Thread.currentThread().getName());

        while (requestedDataBuffers > 0) {
            /*
             ** Only have a limited number of buffer sitting in the allocated pool to prevent
             **   resource starvation for other connections.
             */
            if (allocatedDataBuffers < MAX_OUTSTANDING_BUFFERS) {
                BufferState bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_DATA_FROM_CHAN, MemoryManager.SMALL_BUFFER_SIZE);
                if (bufferState != null) {
                    System.out.println("ServerWorkerThread(" + connStateId + ") allocClientReadBufferState(2)");

                    allocatedDataBuffers++;
                    allocatedDataBufferQueue.add(bufferState);

                    requestedDataBuffers--;
                } else {
                    /*
                    ** The data buffer pool is exhausted, need to come back and try again later.
                     */
                    bufferAllocationFailed.set(true);
                    break;
                }
            } else {
                /*
                ** This ConnectionState is using it's share of data buffers, so it needs to wait to
                **   recycle some that are in use.
                 */
                break;
            }
        }

        return allocatedDataBuffers;
    }

    /*
    ** This is called following a channel read error to release all the BufferState objects
    **   back to the free pool.
     */
    private void releaseBufferState() {
        /*
        ** Walk through the allocated buffer queues and release the memory back to the
        **  free pools
         */
        BufferState bufferState;
        ListIterator<BufferState> iter = allocatedDataBufferQueue.listIterator(0);
        while (iter.hasNext()) {
            bufferState = iter.next();
            iter.remove();

            bufferStatePool.freeBufferState(bufferState);

            allocatedHttpBufferCnt--;
        }

        iter = allocatedHttpBufferQueue.listIterator(0);
        while (iter.hasNext()) {
            bufferState = iter.next();
            iter.remove();

            bufferStatePool.freeBufferState(bufferState);
            allocatedDataBuffers--;
        }

    }

    /*
    ** The following is the callback from the memory manager to indicate that buffers are available
    **   and an allocation call should be made.
    **
    ** TODO: Should the memory allocations take part in the callback to insure that there is not a
    **   galloping herd trying to allocate the freed up memory?
     */
    public void memoryBuffersAreAvailable() {
        bufferAllocationFailed.set(false);
    }

    /*
     ** This is used to start reads into one or more buffers. It looks for BufferState objects that have
     **   their state set to READ_FROM_CHAN. It then sends those buffers off to perform asynchronous reads.
     */
    private void readIntoMultipleBuffers() {
        BufferState bufferState;

        /*
         ** Only setup reads for allocated buffers
         */
        if (allocatedHttpBufferCnt > 0) {
            ListIterator<BufferState> iter = allocatedHttpBufferQueue.listIterator(0);
            while (iter.hasNext()) {
                bufferState = iter.next();
                iter.remove();

                allocatedHttpBufferCnt--;

                outstandingHttpReadCount.incrementAndGet();
                readFromChannel(bufferState);
            }
        }
    }

    /*
     ** This is used to start reads into one or more buffers. It looks for BufferState objects that have
     **   their state set to READ_FROM_CHAN. It then sends those buffers off to perform asynchronous reads.
     */
    private void readIntoDataBuffers() {
        BufferState bufferState;

        /*
         ** Only setup reads for allocated buffers
         */
        if (allocatedDataBuffers > 0) {
            ListIterator<BufferState> iter = allocatedDataBufferQueue.listIterator(0);
            while (iter.hasNext()) {
                bufferState = iter.next();
                iter.remove();

                allocatedDataBuffers--;

                outstandingDataReadCount.incrementAndGet();
                readFromChannel(bufferState);
            }
        }
    }

    /*
     ** This is used to setup the state machine and associated information to allow
     **   a write to take place on the passed in AsynchronousSocketChannel.
     */
    void setChannelAndWrite(final AsynchronousSocketChannel chan) {
        connChan = chan;
        overallState = ConnectionStateEnum.INITIAL_SETUP;

        /*
         ** Setup the WriteConnection at this point
         */
        writeConn = new WriteConnection(connStateId);

        writeConn.assignAsyncWriteChannel(chan);
    }

    /*
     ** This only sets up the Connection for reading data for a client.
     */
    void setChannel(final AsynchronousSocketChannel chan) {
        connChan = chan;
        overallState = ConnectionStateEnum.INITIAL_SETUP;
    }

    /*
     ** This is called when the WorkerThread that this ConnectionState will execute on is
     **   determined. It is done prior to the ConnectionState being queued to the
     **   WorkerThread the first time.
     **
     ** NOTE: The ConnectionState is reused and may execute on different threads for each
     **   lifetime of a connection.
     */
    public void assignWorkerThread(final ServerWorkerThread thread) {
        workerThread = thread;
    }

    /*
     ** This is called when the Server Connection is closed and this tracking object needs
     **   to be cleaned up.
     */
    void clearChannel() {
        connChan = null;

        /* Also clear out the WriteConnection reference */
        writeConn = null;

        bufferStatePool = null;

        clientDataReadCallback = null;

        httpHeaderParsed.set(false);
    }

    /*
     ** TODO: The overallState modifications and reads need to be made thread safe.
     */
    private void setOverallState(final ConnectionStateEnum state) {
        overallState = state;
    }

    public ConnectionStateEnum getState() {
        return overallState;
    }

    void setClientReadCallback(ClientDataReadCallback clientReadCb) {
        clientDataReadCallback = clientReadCb;

        serverConn = false;
    }

    private void callClientReadCallback(final int status, final ByteBuffer readBuffer) {
        clientDataReadCallback.dataBufferRead(status, readBuffer);
    }

    /*
    ** This is the common api to close the AsynchronousChannel. It is synchronized to make
    **   it threads safe in the event that different threads attempt to close the channel
    **   at the same time.
     */
    public synchronized  void closeChannel() {
        try {
            /*
            ** Handle case where the channel has already been closed.
             */
            if (connChan != null) {
                connChan.close();
            }
        } catch (IOException io_ex) {
            System.out.println("ConnectionState(" + connStateId + ") closeChannel() " + io_ex.getMessage());
        }

        connChan = null;
    }

    /*
    ** This is used to put the ConnectionState back into a pristine state so that it can be used
    **   to handle the next HTTP connection.
     */
    private void reset() {

        System.out.println("ConnectionState[" + connStateId + "] reset()");

        releaseBufferState();

        closeChannel();

        overallState = ConnectionStateEnum.INVALID_STATE;

        /*
         ** Clear the items associated with a particular worker thread that was used to execute this
         **   ConnectionState as it may change the next time this ConnectionState is used.
         */
        workerThread = null;
        writeThread = null;
        bufferStatePool = null;
        resultBuilder = null;

        /*
         ** Clear the write connection (it may already be null) since it will not be valid with the next
         **   use of this ConnectionState
         */
        writeConn = null;

        dataResponseSent.set(false);
        httpHeaderParsed.set(false);
        channelError.set(false);

        httpParser.resetHttpParser();
    }


    /*
     ** This walks through the array of BufferState to find the next one that has completed
     **   the read of data into it and returns it.
     ** NOTE: The expectation is that when the Java NIO.2 read operation performs it's callback,
     **   there is no more data to be transferred into the buffer being read into.
     */
    private void readClientBufferCallback() {
        int readsCompleted = dataBufferReadsCompleted.get();

        System.out.println("ConnectionState[" + connStateId + "] readsCompleted: " + readsCompleted);

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
     }


    /*
     ** This is called when a buffer read was completed. This means that the particular buffer
     **   can be processed by the HTTP Parser at this point.
     **
     ** The call path to get here is:
     **    ServerWorkerThread chan.read() completed() callback
     **      --> BufferState.setReadState()
     **
     ** NOTE: This is only called for the good path for reads. The error path is handled in the
     **   readCompletedError() function.
     */
    void httpReadCompleted(final BufferState bufferState, final boolean lastRead) {
        int readCompletedCount;

        int readCount = outstandingHttpReadCount.decrementAndGet();
        try {
            httpReadDoneQueue.put(bufferState);
            readCompletedCount = httpBufferReadsCompleted.incrementAndGet();
        } catch (InterruptedException int_ex) {
            System.out.println("httpReadCompleted(" + connStateId + ") " + int_ex.getMessage());
            readCompletedCount = httpBufferReadsCompleted.get();
        }

        /*
        ** Update the channel's health timeout
         */
        timeoutChecker.updateTime();

        System.out.println("ConnectionState[" + connStateId + "].httpReadCompleted() HTTP lastRead: " + lastRead +
                " readCount: " + readCount + " readCompletedCount: " + readCompletedCount +
                " overallState: " + overallState);

        if (!lastRead) {
            requestedHttpBuffers++;

            overallState = determineNextState();
            addToWorkQueue(false);
        } else if (readCount == 0) {
            /*
             ** If all of the outstanding reads have completed, then advance the state and
             ** queue this work item back up.
             */
            overallState = ConnectionStateEnum.PARSE_HTTP_BUFFER;
            addToWorkQueue(false);
        }

        System.out.println("ConnectionState[" + connStateId + "].httpReadCompleted() overallState: " + overallState.toString());
    }

    /*
     ** This is called when a buffer read was completed. This means that the particular buffer
     **   can be processed by the HTTP Parser at this point.
     **
     ** The call path to get here is:
     **    ServerWorkerThread chan.read() completed() callback
     **      --> BufferState.setReadState()
     **
     ** NOTE: This is only called for the good path for reads. The error path is handled in the
     **   readCompletedError() function.
     */
    void dataReadCompleted(final BufferState bufferState, final boolean lastRead) {
        int readCompletedCount;

        int readCount = outstandingDataReadCount.decrementAndGet();
        try {
            dataReadDoneQueue.put(bufferState);
            readCompletedCount = dataBufferReadsCompleted.incrementAndGet();
        } catch (InterruptedException int_ex) {
            System.out.println("dataReadCompleted(" + connStateId + ") " + int_ex.getMessage());
            readCompletedCount = dataBufferReadsCompleted.get();
        }

        /*
         ** Update the channel's health timeout
         */
        timeoutChecker.updateTime();

        System.out.println("ConnectionState[" + connStateId + "].dataReadCompleted() HTTP lastRead: " + lastRead +
                " readCount: " + readCount + " readCompletedCount: " + readCompletedCount +
                " overallState: " + overallState);

        if (!lastRead) {
            overallState = ConnectionStateEnum.READ_NEXT_BUFFER;
            addToWorkQueue(false);
        } else if (readCount == 0) {
            /*
             ** If all of the outstanding reads have completed, then advance the state and
             ** queue this work item back up.
             */
            overallState = ConnectionStateEnum.CLIENT_READ_CB;
            addToWorkQueue(false);
        }

        System.out.println("ConnectionState[" + connStateId + "].dataReadCompleted() overallState: " + overallState.toString());
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
    void readCompletedError(final BufferState bufferState) {
        int httpReadCount = 0;
        int dataReadCount = 0;

        channelError.set(true);

        if (bufferState.getBufferState() == BufferStateEnum.READ_WAIT_FOR_HTTP) {
            httpReadCount = outstandingHttpReadCount.decrementAndGet();
        } else {
            dataReadCount = outstandingDataReadCount.decrementAndGet();

            /*
            ** Need to increment the number of data buffer reads completed so that the clients will
            **   receive their callback with an error for the buffer.
             */
            try {
                dataReadDoneQueue.put(bufferState);
                dataBufferReadsCompleted.incrementAndGet();
            } catch (InterruptedException int_ex) {
                System.out.println("ERROR ConnectionState[" + connStateId + "] readCompletedError() " + int_ex.getMessage());
            }
        }

        System.out.println("ConnectionState[" + connStateId + "].readCompletedError() httpReadCount: " + httpReadCount +
                " dataReadCount: " + dataReadCount + " overallState: " + overallState);

        /*
         ** If there are outstanding reads in progress, need to wait for those to
         **   complete before cleaning up the ConnectionState
         */
        if ((httpReadCount == 0) && (dataReadCount == 0)){
            overallState = determineNextState();
            addToWorkQueue(false);
        }
    }

    /*
     ** This function walks through all the buffers that have reads completed and pushes
     **   then through the HTTP Parser.
     ** Once the header buffers have been parsed, they can be released. The goal is to not
     **   recycle the data buffers, so those may not need to be sent through the HTTP Parser'
     **   and instead should be handled directly.
     */
    private void parseHttp() {
        BufferState bufferState;
        ByteBuffer buffer;

        int bufferReadsDone = httpBufferReadsCompleted.get();
        if (bufferReadsDone > 0) {
            Iterator<BufferState> iter = httpReadDoneQueue.iterator();

            while (iter.hasNext()) {
                bufferState = iter.next();
                iter.remove();

                // TODO: Assert (bufferState.getState() == READ_HTTP_DONE)
                httpBufferReadsCompleted.decrementAndGet();

                buffer = bufferState.getBuffer();
                int bufferTextCapacity = buffer.position();
                buffer.rewind();
                buffer.limit(bufferTextCapacity);

                httpParser.parseHttpData(buffer, initialHttpBuffer);

                /*
                ** Set the BufferState to PARSE_DONE
                ** TODO: When can the buffers used for the header be released?
                */
                bufferState.setHttpParseDone();

                initialHttpBuffer = false;
            }

            /*
            ** Check if there needs to be another read to bring in more of the HTTP header
             */
            //boolean headerParsed = httpHeaderParsed.get();
            //if (!headerParsed) {
                /*
                ** Allocate another buffer and read in more data
                 */
            //    requestedHttpBuffers++;
            //}
        }
    }

    /*
     ** This is used to tell the connection management that the HTTP header has been parsed
     **   and the validation and authorization can take place.
     */
    void httpHeaderParseComplete() {
        System.out.println("httpHeaderParseComplete(" + connStateId + ") curr: " + overallState);

        httpHeaderParsed.set(true);
    }

    public int getConnStateId() {
        return connStateId;
    }

    /*
     ** Sets up the WriteConnection, but only does it once
     */
    private void setupWriteConnection() {
        if (writeConn == null) {
            writeConn = new WriteConnection(1);

            writeConn.assignAsyncWriteChannel(connChan);
        }
    }

    private WriteConnection getWriteConnection() {
        return writeConn;
    }

    private BufferState allocateResponseBuffer() {
        BufferState respBuffer = bufferStatePool.allocBufferState(this, BufferStateEnum.SEND_GET_DATA_RESPONSE, MemoryManager.MEDIUM_BUFFER_SIZE);

        responseBuffer = respBuffer;

        return respBuffer;
    }

    /*
    ** This is called when the status write completes back to the client.
     */
    public void statusWriteCompleted(final int bytesXfr, final ByteBuffer buffer) {

        BufferStateEnum currState;

        if (responseBuffer == null) {
            System.out.println("statusWriteCompleted: null responseBuffer" + connStateId);
        }

        currState = responseBuffer.getBufferState();
        System.out.println("statusWriteCompleted: " + connStateId + " " + currState.toString());

        switch (currState) {
            case SEND_GET_DATA_RESPONSE:
                dataResponseSent.set(true);
                overallState = determineNextState();
                break;

            case SEND_FINAL_RESPONSE:
                break;
        }

        bufferStatePool.freeBufferState(responseBuffer);
        responseBuffer = null;

        addToWorkQueue(false);
    }

    /*
     ** This is what performs the actual read from the channel.
     */
    private void readFromChannel(final BufferState readBufferState) {

        try {
            SocketAddress addr = connChan.getLocalAddress();

            System.out.println("readFromChannel[" + connStateId + "] socket: " + addr);
        } catch (IOException ex) {
            System.out.println("socket closed " + ex.getMessage());

            closeChannel();

            /*
             ** Mark the BufferState as READ_ERROR to indicate that the buffer is not valid and this
             **   will call into the ConnectionState to move the cleanup of the connection along if
             **   there are no more outstanding reads.
             */
            readBufferState.setReadState(BufferStateEnum.READ_ERROR);
        }

        /*
        ** Mark the BufferState that it has a read in progress. This is useful for tracking outstanding buffers
        **  that are not sitting on any particular queue.
         */
        readBufferState.setReadInProgress();

        // Read the data from the channel
        ByteBuffer readBuffer = readBufferState.getBuffer();
        connChan.read(readBuffer, readBufferState, new CompletionHandler<Integer, BufferState>() {

            @Override
            public void completed(final Integer bytesRead, final BufferState readBufferState) {
                System.out.println("readFromChannel[" + connStateId + "] bytesRead: " + bytesRead + " thread: " + Thread.currentThread().getName());

                if (bytesRead == -1) {
                    closeChannel();

                    readBufferState.setReadState(BufferStateEnum.READ_ERROR);
                } else if (bytesRead > 0) {
                    readBufferState.setReadState(BufferStateEnum.READ_DONE);
                } else if (bytesRead == 0) {
                    readBufferState.setReadState(BufferStateEnum.READ_ERROR);
                }
            }

            @Override
            public void failed(final Throwable exc, final BufferState readBufferState) {
                System.out.println("readFromChannel[" + connStateId + "] bytesRead: " + exc.getMessage() + " thread: " + Thread.currentThread().getName());

                closeChannel();

                /*
                 ** This buffer read failed, need to mark the buffer to indicate that it is not valid
                 */
                readBufferState.setReadState(BufferStateEnum.READ_ERROR);
            }
        });  // end of chan.read()
    }

    /*
     ** This will send out a particular response type on the server channel
     */
    private void sendResponse() {

        // Allocate the Completion object specific to this operation
        setupWriteConnection();

        BufferState buffState = allocateResponseBuffer();
        if (buffState != null) {
            ByteBuffer respBuffer = resultBuilder.buildResponse(buffState, HttpStatus.OK_200, true);
            //ByteBuffer respBuffer = resultBuilder.buildResponse(buffState, HttpStatus.CONTINUE_100, false);

            int bytesToWrite = respBuffer.position();
            respBuffer.flip();

            WriteConnection writeConn = getWriteConnection();
            StatusWriteCompletion statusComp = new StatusWriteCompletion(this, writeConn, respBuffer,
                    getConnStateId(), bytesToWrite, 0);
            writeThread.writeData(writeConn, statusComp);

            System.out.println("sendResponse(" + connStateId + ") 2");
        } else {
            System.out.println("sendResponse(" + connStateId + "): unable to allocate response buffer");
        }
    }

    /*
     ** DEBUG FUNCTIONS BELOW
     */

    /*
     ** This is a general debug function that dumps the buffer to the console.
     */
    private void displayBuffer(final BufferState bufferState) {
        ByteBuffer buffer = bufferState.getBuffer();

        System.out.println("buffer " + buffer.position() + " " + buffer.limit());
        String tmp = bb_to_str(buffer);
        System.out.println("ConnectionState buffer" + tmp);
    }

    private void str_to_bb(ByteBuffer out, String in) {
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            boolean endOfInput = true;

            encoder.encode(CharBuffer.wrap(in), out, endOfInput);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private String bb_to_str(ByteBuffer buffer) {
        buffer.flip();

        return StandardCharsets.UTF_8.decode(buffer).toString();
    }

    /*
    ** Used to protect the timed wait queues and the execution queues
     */
    static class QueueMutex {
        int count;
    }

}
