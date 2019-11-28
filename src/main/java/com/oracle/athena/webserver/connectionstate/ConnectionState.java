package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.http.BuildHttpResult;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.*;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 ** This is used to keep track of the state of the connection for its lifetime.
 **
 ** chan is the socket that the connection is using to transfer data. It is assigned
 **   later and not at instantiation time to allow the ConnectionState objects to be
 **   managed via a pool.
 */
abstract public class ConnectionState {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionState.class);

    /*
     ** MAX_OUTSTANDING_BUFFERS is how many operations can be done in parallel on this
     **   connection. This is to limit one connection from using all the buffers and
     **   all of the processing resources.
     */
    public static final int MAX_OUTSTANDING_BUFFERS = 10;

    /*
    ** OUTSTANDING_READ_ALLOC_POINT is used to determine when the next allocation of memory for reading in
    **    content needs to take place. The idea is to allocate a bunch of buffers and then let the reads take
    **    place into those buffers for a while before trying to allocate the next batch of read buffers.
    **
    ** Currently the maximum number of buffers that can be allocated to a ConnectionState is
    **   MAX_OUTSTANDING_BUFFERS and reads will take place on those buffers until there are
    **   OUTSTANDING_READ_ALLOC_POINT reads outstanding at which point a check is made to see if
    **   more buffers need to be allocated.
     */
    private static final int OUTSTANDING_READ_ALLOC_POINT = 2;


    /*
    ** This is how long the ConnectionState should wait until if goes back through the
    **   CHECK_SLOW_CHANNEL state if no other activity is taking place.
     */
    private static final long TIME_TILL_NEXT_TIMEOUT_CHECK = 500;

    public static final int NUM_SSL_APP_BUFFERS = 2;
    public static final int NUM_SSL_NET_BUFFERS = 2;

    /*
    ** connStateId is used as a way to track a ConnectionState in logging statements. Each
    **   ConnectionState has a unique ID to identify it for the life of the program.
     */
    int connStateId;

    /*
     ** The connChan comes from the either the accept() call in the ServerChannelLayer run() method
     **   or from (the test path):
     **     TestClient.registerClientReadCallback()
     **       InitiatorLoadBalancer.startNewClientReadConnection()
     **         ConnectionStatePool<T>.allocConnectionState()
     **           ClientConnState.setChannel()
     **  For the TestClient, the connChan is created when the test program opens a socket to
     **    perform writes on.
     */
    AsynchronousSocketChannel connChan;

    SSLContext sslContext;
    SSLEngine engine;

    private LinkedList<BufferState> allocatedSSLAppBufferQueue;
    private LinkedList<BufferState> allocatedSSLNetBufferQueue;
    private SSLEngineResult sslEngineResult;
    private boolean sslHandshakeRequired;
    private boolean sslHandshakeSuccess;
    private boolean sslBuffersNeeded;
    private SSLEngineResult.HandshakeStatus handshakeStatus;

    private final Object connChanMutex;


    private long nextExecuteTime;

    /*
    ** The following variables are used in the ContentReadPipeline and are used to keep track
    **   of the state of the various content read operations through the sending of the final
    **   status.
    **
    ** NOTE: There will never be both HTTP header reads and Data reads in progress at the same
    **   time. The HTTP header reads must all be completed prior to starting Data reads.
    **
    ** The variables are as follows:
    **    requestedDataBuffers - This is the number of content ByteBuffers that should be allocated. This may be less
    **      than the actual number of buffers that are required to read all of the content data in. This must be
    **      an AtomicInteger since it is decremented in the read callback (call from an NIO.2 thread) and looked
    **      at in the normal state machine processing.
    **
    **    allocatedDataBuffers - This is the number of buffers that have been allocated to read in content data and are
    **      waiting to have reads performed on the.
    **
    **    outstandingDataReadCount - This is the number of outstanding content reads are currently in progress.
    **    contentAllRead -
    **
     */
    protected AtomicInteger outstandingDataReadCount;
    private int requestedDataBuffers;
    private int allocatedDataBuffers;
    protected AtomicInteger dataBufferReadsCompleted;
    private AtomicBoolean contentAllRead;
    protected AtomicInteger dataBufferDigestCompleted;
    protected AtomicInteger dataBufferDigestSent;
    AtomicBoolean bufferAllocationFailed;
    AtomicBoolean digestComplete;


    /*
    ** The following are used to insure that a ConnectionState is never on more than one queue and that
    **   if there is a choice between being on the timed wait queue (connOnDelayedQueue) or the normal
    **   execution queue (connOnExecutionQueue) is will always go on the execution queue.
     */
    protected boolean connOnDelayedQueue;
    protected boolean connOnExecutionQueue;

    private final Object queueMutex;

    /*
    ** The following are used to keep track of the content being read in.
     */
    AtomicLong contentBytesToRead;
    private AtomicLong contentBytesAllocated;
    private AtomicLong contentBytesRead;

    private SSLContext context;

    /*
     ** The next four items are associated with the thread that is running the ConnectionState
     **   state machine.
     */
    protected ServerWorkerThread workerThread;
    protected BufferStatePool bufferStatePool;
    WriteConnThread writeThread;
    BuildHttpResult resultBuilder;

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

    /*
    ** The following queue is used to keep track of the BufferState that had a failed read. Items are placed
    **   on this queue from the NIO.2 callback thread, so it must be a thread safe queue.
     */
    protected BlockingQueue<BufferState> readErrorQueue;

    /*
     ** The following is used to keep track of BufferState that have completed their reads. Items are placed
     **   on this queue from the NIO.2 callback thread, so it must be thread safe.
     */
    protected BlockingQueue<BufferState> dataReadDoneQueue;

    /*
     ** The following is used to keep track of BufferState that have completed their reads. Items are placed
     **   on this queue from the NIO.2 callback thread, so it must be thread safe.
     */
    protected BlockingQueue<BufferState> dataMd5DoneQueue;

    /*
    ** The following is used to indicate that there was a channel error and the connection should be
    **   closed out.
    ** NOTE: This does not need to be an AtomicBoolean as the setter and getters all run within the
    **   same state machine on the same thread. If the code is changed to allow ConnectionState to
    **   run different channel read or write operations on different threads, then this will need to
    **   be made and AtomicBoolean.
     */
    protected boolean channelError;

    /*
    ** The following is used to indicate there has been a failure in the HTTP parsing.
     */
    protected AtomicBoolean httpParsingError;


    /*
    ** The following is used to check that progress is being made on a channel and the
    **   client is not slow sending or receiving data.
     */
    protected TimeoutChecker timeoutChecker;

    protected Md5Digest md5Digest;


    public ConnectionState(final int uniqueId) {
        outstandingDataReadCount = new AtomicInteger(0);

        connStateId = uniqueId;

        bufferStatePool = null;
        resultBuilder = null;

        /*
        ** When the connection begins, there cannot be a channelError.
         */
        channelError = false;

        connOnDelayedQueue = false;
        connOnExecutionQueue = false;
        queueMutex = new Object();

        requestedDataBuffers = 0;
        allocatedDataBuffers = 0;
        allocatedDataBufferQueue = new LinkedList<>();

        bufferAllocationFailed = new AtomicBoolean(false);

        dataBufferReadsCompleted = new AtomicInteger(0);
        dataReadDoneQueue = new LinkedBlockingQueue<>(MAX_OUTSTANDING_BUFFERS * 2);

        dataMd5DoneQueue = new LinkedBlockingQueue<>(MAX_OUTSTANDING_BUFFERS * 2);

        contentBytesToRead = new AtomicLong(0);
        contentBytesAllocated = new AtomicLong(0);
        contentBytesRead = new AtomicLong(0);
        contentAllRead = new AtomicBoolean(false);

        dataBufferDigestCompleted = new AtomicInteger(0);
        digestComplete  = new AtomicBoolean(false);
        dataBufferDigestSent = new AtomicInteger(0);

        /*
        ** This queue is used to keep track of all the buffers that have returned a read error. It is used
        **   to insure that updates to the error state are done on the worker thread and not on the
        **   callback thread from NIO.2
         */
        readErrorQueue = new LinkedBlockingQueue<>(MAX_OUTSTANDING_BUFFERS * 2);

        /*
        ** httpParsingError is kept in the base class so that is can also be used for the client child class as
        **   well. For the client, it is an error in the returned response data.
         */
        httpParsingError = new AtomicBoolean(false);

        /*
        ** connChan needs to be protected since the closeChannel() method can be accessed from
        **   various threads.
         */
        connChan = null;
        connChanMutex = new Object();
        md5Digest = null;

        sslContext = null;
        engine = null;
        allocatedSSLAppBufferQueue = new LinkedList<>();
        allocatedSSLNetBufferQueue = new LinkedList<>();
        sslEngineResult = null;
        sslHandshakeRequired = false;
        sslHandshakeSuccess = false;
        sslBuffersNeeded = false;
        handshakeStatus = SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
    }

    /*
     **
     */
    public void start() {
        timeoutChecker = new TimeoutChecker();
    }

    /*
    ** This is what actually performs the work
     */
    abstract public void stateMachine();

    abstract public ConnectionStateEnum getNextState();

    /*
     ** The following is used to handle the case when a read error occurred and the ConnectionState needs
     **   to cleanup and release resources.
     **
     ** The call path to get here is:
     **    ServerWorkerThread chan.read() failed() callback
     **      --> BufferState.setReadState()
     **
     */
    abstract public void readCompletedError(final BufferState bufferState);

    abstract public void setChannel(final AsynchronousSocketChannel chan);

    abstract public void setSslContext(final SSLContext sslContext);

    abstract public void setReadState(final BufferState bufferState, final BufferStateEnum newState);

    /*
    ** This is to perform some initial setup and could be removed if the ConnectionState is tied to the
    **   thread it is going to run under.
     */
    public void setupInitial() {
        if (workerThread == null){
            LOG.error("ConnectionState[" + connStateId + "] setupInitial() (workerThread == null)");
        }
        bufferStatePool = workerThread.getBufferStatePool();
        writeThread = workerThread.getWriteThread();
        resultBuilder = workerThread.getResultBuilder();
        md5Digest = new Md5Digest();

        timeoutChecker.updateTime();
    }

    /*
    ** Accessor functions related to the HTTP Parser and when an error occurs.
    **
    ** getHttpParserError() will return 0 if there is no error, otherwise it will return
    **   the value set to indicate the parsing error.
     */
    public void setHttpParsingError() {
        httpParsingError.set(true);
    }

    abstract public int getHttpParseStatus();

    /*
    ** This is used to add the ConnectionState to the worker thread's execute queue. The
    **   ConnectionState can be added to the immediate execution queue or the delayed
    **   execution queue.
    **
    ** The following methods are called by the ServerWorkerThread object under a queue mutex.
    **   markRemoveFromQueue - This method is used by the ServerWorkerThread to update the queue
    **     the connection is on when the connection is removed from the queue.
    **   markAddedToQueue - This method is used when a connection is added to a queue to mark
    **     which queue it is on.
    **   isOnWorkQueue - Accessor method
    **   isOnTimedWaitQueue - Accessor method
    **
    ** TODO: Might want to switch to using an enum instead of two different booleans to keep track
    **   of which queue the connection is on. It will probably clean up the code some.
     */
    public void addToWorkQueue(final boolean delayedExecution) {
        if (delayedExecution) {
            workerThread.addToDelayedQueue(this);
        } else {
            workerThread.addToWorkQueue(this);
        }
    }

    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        LOG.info("ConnectionState[" + connStateId + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (connOnDelayedQueue) {
            if (!delayedExecutionQueue) {
                LOG.warn("ConnectionState[" + connStateId + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on delayed queue");
            }

            connOnDelayedQueue = false;
            nextExecuteTime = 0;
        } else if (connOnExecutionQueue){
            if (delayedExecutionQueue) {
                LOG.warn("ConnectionState[" + connStateId + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on workQueue");
            }

            connOnExecutionQueue = false;
        } else {
            LOG.warn("ConnectionState[" + connStateId + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            nextExecuteTime = System.currentTimeMillis() + TIME_TILL_NEXT_TIMEOUT_CHECK;
            connOnDelayedQueue = true;
        } else {
            connOnExecutionQueue = true;
        }
    }

    public boolean isOnWorkQueue() {
        return connOnExecutionQueue;
    }

    public boolean isOnTimedWaitQueue() {
        return connOnDelayedQueue;
    }


    public boolean hasWaitTimeElapsed() {
        long currTime = System.currentTimeMillis();

        if (currTime < nextExecuteTime) {
            return false;
        }

        //System.out.println("ServerWorkerThread[" + connStateId + "] waitTimeElapsed " + currTime);
        return true;
    }

    public long getNextExecuteTime() {
        return nextExecuteTime;
    }

    /*
    ** Returns if there are outstanding requests for data buffers
     */
    public boolean needsMoreDataBuffers() {
        return (requestedDataBuffers > 0);
    }

    public void resetRequestedDataBuffers() {
        requestedDataBuffers = 0;
    }

    protected void addRequestedDataBuffer() {
        requestedDataBuffers++;
    }

    private BufferState allocateContentDataBuffers() {
        if (isSSL()) {
            int appBufferSize = engine.getSession().getApplicationBufferSize();
            int netBufferSize = engine.getSession().getPacketBufferSize();

            return bufferStatePool.allocBufferState(this, BufferStateEnum.READ_DATA_FROM_CHAN,
                                                     appBufferSize, netBufferSize);

        } else {
            return bufferStatePool.allocBufferState(this, BufferStateEnum.READ_DATA_FROM_CHAN, MemoryManager.MEDIUM_BUFFER_SIZE);
        }
    }

    /*
    ** This is the function to add BufferState to the available queue. This means the BufferState are
    **   now ready to have data read into them.
     */
    public int allocClientReadBufferState() {
        LOG.info("ServerWorkerThread[" + connStateId + "] allocClientReadBufferState(1) ");

        while (requestedDataBuffers > 0) {
            /*
             ** Only have a limited number of buffer sitting in the allocated pool to prevent
             **   resource starvation for other connections.
             */
            if (allocatedDataBuffers < MAX_OUTSTANDING_BUFFERS) {
                BufferState bufferState = allocateContentDataBuffers();
                if (bufferState != null) {
                    allocatedDataBuffers += bufferState.count();
                    allocatedDataBufferQueue.add(bufferState);

                    LOG.info("ServerWorkerThread[" + connStateId + "] allocClientReadBufferState(2) allocatedDataBuffers: " + allocatedDataBuffers);

                    /*
                    ** Update the Content information if this is a server connection. This is keeping track of how many
                    **   bytes worth of buffer have been allocated to read in the content information. This is used to
                    **   determine how many buffers should be allocated when reads complete.
                     */
                    int bufferSize = bufferState.getBuffer().limit();
                    contentBytesAllocated.addAndGet(bufferSize);

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
    ** Returns if there are data buffers allocated and waiting to have data read into them. In addition, it will
    **   only allow a single outstanding read to take place. This is due to the way sockets work in NIO.2.
     */
    public boolean dataBuffersWaitingForRead() {
        return ((allocatedDataBuffers > 0) && (outstandingDataReadCount.get() == 0));
    }

    public void resetBuffersWaiting() {
        allocatedDataBuffers = 0;
        outstandingDataReadCount.set(0);
    }

    /*
    ** This is called following a channel read error to release all the BufferState objects
    **   back to the free pool.
     */
    public void releaseBufferState() {
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
    ** This returns if this connection is allowed to obtain buffers
     */
    public boolean outOfMemory() {
        return (bufferAllocationFailed.get());
    }


    /*
     ** This is used to start reads into one or more buffers. It looks for BufferState objects that have
     **   their state set to READ_FROM_CHAN. It then sends those buffers off to perform asynchronous reads.
     */
    public void readIntoDataBuffers() {
        BufferState bufferState;

        /*
         ** Only setup reads for allocated buffers
         */
        if (allocatedDataBuffers > 0) {
            ListIterator<BufferState> iter = allocatedDataBufferQueue.listIterator(0);
            if (iter.hasNext()) {
                bufferState = iter.next();
                iter.remove();

                allocatedDataBuffers--;

                outstandingDataReadCount.incrementAndGet();
                readFromChannel(bufferState);
            }
        }
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
    public void clearChannel() {
        bufferStatePool = null;

        synchronized (connChanMutex) {
            connChan = null;
        }
    }

    /*
    ** This is the common api to close the AsynchronousChannel. It is synchronized to make
    **   it threads safe in the event that different threads attempt to close the channel
    **   at the same time.
     */
    protected void closeChannel() {
        synchronized (connChanMutex) {
            try {
                /*
                 ** Handle case where the channel has already been closed.
                 */
                if (connChan != null) {
                    connChan.close();
                }
            } catch (IOException io_ex) {
                LOG.info("ConnectionState[" + connStateId + "] closeChannel() " + io_ex.getMessage());
            }

            connChan = null;
        }
    }

    /*
    ** This is used to set the AsychronousSocketChannel within the super class so that it can be a
    **   synchronized operation if needed. Currently, there are no operations that can take place
    **   on the channel prior to it being set, so it is not synchronized.
     */
    protected void setAsyncChannel(AsynchronousSocketChannel chan) {
        connChan = chan;
    }

    protected void initSslEngine(SSLContext sslContext) {
        this.sslContext = sslContext;

        if (sslContext != null) {
            engine = sslContext.createSSLEngine();
            engine.setUseClientMode(false);
            engine.setNeedClientAuth(false);
        }
    }

    protected boolean isSSL() {
        return (sslContext != null);
    }

    /*
    ** This is used to put the ConnectionState back into a pristine state so that it can be used
    **   to handle the next HTTP connection.
     */
    public void reset() {

        LOG.info("ConnectionState[" + connStateId + "] reset()");

        workerThread.removeFromQueue(this);

        releaseBufferState();

        closeChannel();

        allocatedDataBuffers = 0;
        outstandingDataReadCount.set(0);

        /*
         ** Clear the items associated with a particular worker thread that was used to execute this
         **   ConnectionState as it may change the next time this ConnectionState is used.
         */
        workerThread = null;
        writeThread = null;
        bufferStatePool = null;
        resultBuilder = null;

        /*
        ** Clear out the channelError prior to this ConnectionState being reused.
         */
        channelError = false;

        /*
        ** Clear out the information about the content.
         */
        contentBytesToRead.set(0);
        contentBytesAllocated.set(0);
        contentBytesRead.set(0);
        contentAllRead.set(false);
        digestComplete.set(false);
        dataBufferDigestCompleted.set(0);
        dataBufferDigestSent.set(0);

        httpParsingError.set(false);
        md5Digest = null;
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
    protected void dataReadCompleted(final BufferState bufferState) {
        int readCount = outstandingDataReadCount.decrementAndGet();

        int bytesRead = bufferState.getChannelBuffer().position();

        if (isSSL()) {
            /* TODO: This will require its own state in the pipeline
                to handle underflow.  In that case need to wait for another
                read buffer, combine the buffers and resubmit for unwrap.  Properly
                unwrapped buffers then proceed to dataReadComplete.
             */
            sslUnwrapData(bufferState);
        }

        addDataBuffer(bufferState, bytesRead);

        /*
         ** Update the channel's health timeout
         */
        timeoutChecker.updateTime();

        LOG.info("ConnectionState[" + connStateId + "] dataReadCompleted() outstandingReadCount: " + readCount);

        addToWorkQueue(false);
    }

    /*
    ** This is used by the HttpParsePipelineMgr and the ContentReadPipelineMgr to determine if there have been any
    **   ByteBuffer channel read errors that need to be processed. Upon receiving the first read error, the
    **   channelError flag is set to indicate that the connection has failed and needs to be cleaned up.
     */
    public boolean readErrorQueueNotEmpty() {
        return (!readErrorQueue.isEmpty());
    }


    /*
    ** This adds the remainder from the buffer used to read in the header to the
    ** data buffer list and updates the details for how much to read.
     */
    void addDataBuffer(final BufferState bufferState, final int bytesRead) {
        int readCompletedCount;

        /*
         ** Update the number of bytes actually read for a server connection.
         */
        long totalBytesRead = contentBytesRead.addAndGet(bytesRead);

        try {
            dataReadDoneQueue.put(bufferState);
            //bufferState.setReadState(BufferStateEnum.READ_DONE);
            readCompletedCount = dataBufferReadsCompleted.incrementAndGet();
        } catch (InterruptedException int_ex) {
            /*
             ** TODO: This is an error case and the connection needs to be closed
             */
            LOG.info("dataReadCompleted(" + connStateId + ") " + int_ex.getMessage());
            readCompletedCount = dataBufferReadsCompleted.get();
        }

        LOG.info("ConnectionState[" + connStateId + "] addDataBuffer() readCompletedCount: " + readCompletedCount +
                " contentBytesRead: " + totalBytesRead);

        determineNextContentRead();
    }


    public void sslUnwrapData(BufferState bufferState) {
        ByteBuffer clientAppData = bufferState.getBuffer();
        ByteBuffer clientNetData = bufferState.getNetBuffer();

        int bytesRead = clientNetData.position();
        clientNetData.flip();
        while (clientNetData.hasRemaining()) {
            clientAppData.clear();
            SSLEngineResult result;
            try {
                result = engine.unwrap(clientNetData, clientAppData);
            } catch (SSLException e) {
                //TODO: Return error to client, log it
                System.out.println("Unable to unwrap data.");
                e.printStackTrace();
                return;
            }
            switch (result.getStatus()) {
                case OK:
                    //Successfully unwrapped received data
                    break;
                case BUFFER_OVERFLOW:
                    //TODO: destination buffer not large enough;
                    break;
                case BUFFER_UNDERFLOW:
                    //TODO: no data was read or the TLS packet was incomplete.
                    break;
                case CLOSED:
                    //TODO: Client wants to close connection
                    return;
                default:
                    //Log this unknown state, return error to client
                    LOG.error("ConnectionState[" + connStateId + "] sslUnwrapData() state: " + result.getStatus() +
                              " bytesRead: " + bytesRead);
                    break;
            }
        }
    }

    public void sslWrapData(BufferState bufferState) throws IOException{
        ByteBuffer clientAppData = bufferState.getBuffer();
        ByteBuffer clientNetData = bufferState.getNetBuffer();

        while (clientAppData.hasRemaining()) {
            clientNetData.clear();
            SSLEngineResult result  = engine.wrap(clientAppData, clientNetData);
            switch (result.getStatus()) {
                case OK:
                    System.out.println("Wrapped data.");
                    return;
                case BUFFER_OVERFLOW:
                    // FIXME: need to get a bigger buffer
                    break;
                case CLOSED:
                    //FIXME: ("Client wants to close connection...");
                    return;
                default:
                    //Log this state
                    System.out.println("Illegal state: " + result.getStatus().toString());
                    break;
            }
        }
    }


    /*
    ** This returns the number of data buffer reads that have completed (these are the reads to bring in the content)
     */
    public int getDataBufferReadsCompleted() {
        return dataBufferReadsCompleted.get();
    }

    public void resetDataBufferReadsCompleted() {
        dataBufferReadsCompleted.set(0);
    }

    public int getDataBufferDigestCompleted() {
        return dataBufferDigestCompleted.get();
    }

    public int getDataBufferDigestSent() {
        return dataBufferDigestSent.get();
    }
    /*
    ** This method determines what to do next with content buffers. It determines how much of the
    **   data being sent has aleady been read in and determines how many buffers (if any) are needed
    **   to read in the remaining data.
    ** It will limit the number of buffers allocated to perform the reads to insure that one
    **   connection cannot deplete the available buffer pool and starve out other connections.
    **
    ** TODO: This should be moved to a state machine step that also processes the read completed
    **   buffers to remove the need for the Atomic variables.
     */
    void determineNextContentRead() {
        /*
        ** First determine how many buffers would be needed to read up the remaining data
         */
        long bytesToRead = contentBytesToRead.get();
        long bytesAllocated = contentBytesAllocated.get();
        int buffersNeeded = (int) ((bytesToRead - bytesAllocated) / MemoryManager.MEDIUM_BUFFER_SIZE);
        int maxBuffersToAllocate = MAX_OUTSTANDING_BUFFERS - (allocatedDataBuffers + requestedDataBuffers);
        if (buffersNeeded > maxBuffersToAllocate) {
            buffersNeeded = maxBuffersToAllocate;
        } else if (buffersNeeded < 0) {
            buffersNeeded = 0;
        }

        LOG.info("ConnectionState[" + connStateId + "] determineNextContentRead() buffersNeeded: " + buffersNeeded +
                " allocatedDataBuffers: " + allocatedDataBuffers + " requestedDataBuffers: " + requestedDataBuffers);

        if (buffersNeeded > 0) {
            /*
            ** How many buffers are outstanding waiting for reads to complete?
             */
            int outstandingReads = outstandingDataReadCount.get();

            if (outstandingReads < OUTSTANDING_READ_ALLOC_POINT) {
                requestedDataBuffers += buffersNeeded;
            }
        } else if (allocatedDataBuffers == 0) {
            /*
            ** First check if there are still outstanding reads to be completed. If there are no outstanding reads
            **   and there are no allocated data buffers waiting to start reads, then we can determine how
            **   much data is actually remaining to be read.
             */
            int outstandingReads = outstandingDataReadCount.get();
            if (outstandingReads == 0) {
                long actualBytesRead = contentBytesRead.get();
                if (actualBytesRead > bytesToRead) {
                    /*
                    ** TODO: May want to go back and set the limit on the last buffer
                     */
                    actualBytesRead = bytesToRead;
                }

                long remainingToRead = bytesToRead - actualBytesRead;
                if (remainingToRead == 0) {
                    /*
                    ** No more content data to read, so move onto the next processing step
                     */
                    LOG.info("ConnectionState[" + connStateId + "] determineNextContentRead() contentAllRead true");

                    contentAllRead.set(true);
                } else {
                    /*
                     ** This is the case where the buffer reads are partial reads (i.e. the read request 1k, but only
                     **   500 bytes were read in). So, this means that more buffers are required.
                     */
                    buffersNeeded = (int) (remainingToRead / MemoryManager.MEDIUM_BUFFER_SIZE);
                    if (buffersNeeded > MAX_OUTSTANDING_BUFFERS) {
                        buffersNeeded = MAX_OUTSTANDING_BUFFERS;
                    }

                    requestedDataBuffers += buffersNeeded;
                }
            }
        }
    }

    /*
    ** Returns if the content (data that follows the headers in a PUT operation) has all been
    **   read in.
     */
    public boolean hasAllContentBeenRead() {
        return contentAllRead.get();
    }

    public void resetContentAllRead() {
        contentAllRead.set(false);
    }

    public int dataReadsPending() {
        return outstandingDataReadCount.get();
    }

    /*
     ** Returns if there has been an error on this AsynchronousConnectionChannel. This is set following a
     **   channel read error.
     */
    public boolean hasChannelFailed() {
        return channelError;
    }


    /*
    ** connStateId is used to uniquely identify this ConnectionState. It is used for tracing operations that
    **   occur with this connection.
     */
    public int getConnStateId() {
        return connStateId;
    }

    /*
     ** This is what performs the actual read from the channel.
     */
    void readFromChannel(final BufferState readBufferState) {

        synchronized (connChanMutex) {
            /*
             ** Make sure the connChan has not be closed out before trying to read or perform
             **   any operations on it.
             */
            if (connChan == null) {
                LOG.info("ConnectionState[" + connStateId + "] socket closed: connChan null");

                /*
                 ** Mark the BufferState as READ_ERROR to indicate that the buffer is not valid and this
                 **   will call into the ConnectionState to move the cleanup of the connection along if
                 **   there are no more outstanding reads.
                 */
                setReadState(readBufferState, BufferStateEnum.READ_ERROR);
                return;
            }

            try {
                SocketAddress addr = connChan.getLocalAddress();
                LOG.info("readFromChannel[" + connStateId + "] socket: " + addr);
            } catch (IOException ex) {
                LOG.info("socket closed " + ex.getMessage());

                closeChannel();

                /*
                 ** Mark the BufferState as READ_ERROR to indicate that the buffer is not valid and this
                 **   will call into the ConnectionState to move the cleanup of the connection along if
                 **   there are no more outstanding reads.
                 */
                setReadState(readBufferState, BufferStateEnum.READ_ERROR);
                return;
            }
        }

        /*
        ** Mark the BufferState that it has a read in progress. This is useful for tracking outstanding buffers
        **  that are not sitting on any particular queue.
         */
        readBufferState.setReadInProgress();

        // Read the data from the channel
        ByteBuffer readBuffer = readBufferState.getChannelBuffer();
        connChan.read(readBuffer, readBufferState, new CompletionHandler<Integer, BufferState>() {

            @Override
            public void completed(final Integer bytesRead, final BufferState readBufferState) {
                LOG.info("readFromChannel[" + connStateId + "] bytesRead: " + bytesRead);

                if (bytesRead == -1) {
                    closeChannel();

                    setReadState(readBufferState, BufferStateEnum.READ_ERROR);
                } else if (bytesRead > 0) {
                    setReadState(readBufferState, BufferStateEnum.READ_DONE);
                } else if (bytesRead == 0) {
                    setReadState(readBufferState, BufferStateEnum.READ_ERROR);
                }
            }

            @Override
            public void failed(final Throwable exc, final BufferState readBufferState) {
                LOG.info("readFromChannel[" + connStateId + "] failed bytesRead: " + exc.getMessage());

                closeChannel();

                /*
                 ** This buffer read failed, need to mark the buffer to indicate that it is not valid
                 */
                setReadState(readBufferState, BufferStateEnum.READ_ERROR);
            }
        });  // end of chan.read()
    }


    /*
     ** If buffer is clear, read from channel and return future.  Otherwise
	 ** return completed future.
     */
    public Future<Integer> readFromChannelFuture(final ByteBuffer buffer) {
        AsynchronousSocketChannel readChan;

        synchronized (connChanMutex) {
            readChan = connChan;
        }

        if (buffer.position() != 0) {
            return (Future<Integer>) CompletableFuture.completedFuture(buffer.position());
        } else {
            return readChan.read(buffer);
        }
    }

    public Future<Integer> writeToChannelFuture(final ByteBuffer buffer) {
        AsynchronousSocketChannel writeChan;

        synchronized (connChanMutex) {
            writeChan = connChan;
        }

        // Read the data from the channel
        return writeChan.write(buffer);
    }

    public void updateDigest(ByteBuffer byteBuffer) {
        md5Digest.digestByteBuffer(byteBuffer);
        dataBufferDigestCompleted.incrementAndGet();
    }

    public void sendBuffersToMd5Worker() {
        BufferState md5ReadyBuffers[] = new BufferState[0];
        md5ReadyBuffers = dataReadDoneQueue.toArray(md5ReadyBuffers);

        for (BufferState bufferState : md5ReadyBuffers) {
            dataReadDoneQueue.remove(bufferState);
            dataBufferReadsCompleted.decrementAndGet();
            workerThread.addServerDigestWork(bufferState);
            bufferState.setBufferState(BufferStateEnum.DIGEST_WAIT);
            dataBufferDigestSent.incrementAndGet();
        }
    }

    public void md5CalculateComplete() {
        String dataDigestString = md5Digest.getFinalDigest();
        LOG.info("md5Digest " + dataDigestString );
        digestComplete.set(true);
    }

    public void md5WorkerCallback(BufferState bufferState) {
        dataMd5DoneQueue.add(bufferState);
    }

    void md5BufferWorkComplete() {
        BufferState[] md5DoneArray = new BufferState[0];
        md5DoneArray = dataMd5DoneQueue.toArray(md5DoneArray);

        for (BufferState bufferState : md5DoneArray) {
            if (bufferState.getBufferState() == BufferStateEnum.DIGEST_WAIT) {
                dataBufferDigestCompleted.incrementAndGet();
                dataBufferDigestSent.decrementAndGet();
                bufferState.setBufferState(BufferStateEnum.DIGEST_DONE);
                LOG.info("bufferToMd5 " + bufferState + " this :" + this);
            }
        }
    }

    boolean getDigestComplete() {
        return digestComplete.get();
    }

    boolean hasMd5CompleteBuffers() {
        return dataMd5DoneQueue.size() > dataBufferDigestCompleted.get();
    }

    /*
     ** DEBUG FUNCTIONS BELOW
     */

    /*
     ** This is a general debug function that dumps the buffer to the console.
     */
    void displayBuffer(final BufferState bufferState) {
        ByteBuffer buffer = bufferState.getBuffer();
        int position = buffer.position();
        int limit = buffer.limit();

        String tmp = bb_to_str(buffer);
        LOG.info("ConnectionState buffer: " + tmp);

        /*
        ** Need to reset the values otherwise anything that tries to operate on this
        **   this buffer later on will have the position and limit set to 0
         */
        buffer.position(position);
        buffer.limit(limit);
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
        return StandardCharsets.UTF_8.decode(buffer).toString();
    }

    public boolean isSSLHandshakeRequired() {
        return sslHandshakeRequired;
    }

    public void setSSLHandshakeRequired(boolean sslHandshakeRequired) {
        this.sslHandshakeRequired = sslHandshakeRequired;
    }

    public boolean isSSLHandshakeSuccess() {
        return sslHandshakeSuccess;
    }

    public void setSSLHandshakeSuccess(boolean sslHandshakeSuccess) {
        this.sslHandshakeSuccess = sslHandshakeSuccess;
    }

    public boolean isSSLBuffersNeeded() {
        return sslBuffersNeeded;
    }

    public void setSSLBuffersNeeded( boolean sslBuffersNeeded ){
        this.sslBuffersNeeded = sslBuffersNeeded;
    }

    /*
     ** Allocate a buffers for SSL handshaking.
     ** Server and client buffers are supposed to be large enough to hold all message data the server
     ** will send and expects to receive from the client. Since the messages to be exchanged will usually be less
     ** than 16KB long the capacity of these fields should also be smaller.  Expected buffer sizes are retrieved
     ** from the session object.
     */
    public int allocSSLHandshakeBuffers() {
        int appBufferSize = engine.getSession().getApplicationBufferSize();
        int netBufferSize = engine.getSession().getPacketBufferSize();

        while (allocatedSSLAppBufferQueue.size() < ConnectionState.NUM_SSL_APP_BUFFERS) {
            BufferState bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.SSL_HANDSHAKE_APP_BUFFER, appBufferSize);
            if (bufferState != null) {
                allocatedSSLAppBufferQueue.add(bufferState);

            } else {
                /*
                 ** Unable to allocate memory, come back later
                 */
                bufferAllocationFailed.set(true);
                return allocatedSSLAppBufferQueue.size();
            }
        }

        while (allocatedSSLNetBufferQueue.size() < ConnectionState.NUM_SSL_NET_BUFFERS) {
            BufferState bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.SSL_HANDSHAKE_NET_BUFFER, netBufferSize);
            if (bufferState != null) {
                allocatedSSLNetBufferQueue.add(bufferState);

            } else {
                /*
                 ** Unable to allocate memory, come back later
                 */
                bufferAllocationFailed.set(true);
                return allocatedSSLAppBufferQueue.size() + allocatedSSLAppBufferQueue.size();
            }
        }

        setSSLBuffersNeeded(false);
        return allocatedSSLAppBufferQueue.size() + allocatedSSLAppBufferQueue.size();
    }

    public void freeSSLHandshakeBuffers() {
        ListIterator<BufferState> iterApp = allocatedSSLAppBufferQueue.listIterator(0);
        ListIterator<BufferState> iterNet = allocatedSSLNetBufferQueue.listIterator(0);

        while (iterApp.hasNext()) {
            BufferState bufferState = iterApp.next();
            iterApp.remove();

            bufferStatePool.freeBufferState(bufferState);
        }

        while (iterNet.hasNext()) {
            BufferState bufferState = iterNet.next();
            iterNet.remove();

            bufferStatePool.freeBufferState(bufferState);
        }

    }

    public void beginHandshake() throws SSLException{
        engine.beginHandshake();
        setSSLHandshakeRequired(true);
        setSSLHandshakeSuccess(false);
        handshakeStatus = engine.getHandshakeStatus();
    }

    /*
     * Implements the handshake protocol between two peers, required for the establishment of the SSL/TLS connection.
     * During the handshake, encryption configuration information - such as the list of available cipher suites - will be exchanged
     * and if the handshake is successful will lead to an established SSL/TLS session.
     *
     * A typical handshake will usually contain the following steps:
     *
     *   1. wrap:     ClientHello
     *   2. unwrap:   ServerHello/Cert/ServerHelloDone
     *   3. wrap:     ClientKeyExchange
     *   4. wrap:     ChangeCipherSpec
     *   5. wrap:     Finished
     *   6. unwrap:   ChangeCipherSpec
     *   7. unwrap:   Finished
     *
     * Handshake is also used during the end of the session, in order to properly close the connection between the two peers.
     * A proper connection close will typically include the one peer sending a CLOSE message to another, and then wait for
     * the other's CLOSE message to close the transport link. The other peer from his perspective would read a CLOSE message
     * from his peer and then enter the handshake procedure to send his own CLOSE message as well.
     *
     */
    public boolean doSSLHandshake() {

        if ((allocatedSSLAppBufferQueue.size() < ConnectionState.NUM_SSL_APP_BUFFERS) ||
                (allocatedSSLNetBufferQueue.size() < ConnectionState.NUM_SSL_NET_BUFFERS)) {
            setSSLBuffersNeeded(true);
            setSSLHandshakeRequired(false);
            return false;
        }

        ListIterator<BufferState> iterApp = allocatedSSLAppBufferQueue.listIterator(0);
        ListIterator<BufferState> iterNet = allocatedSSLNetBufferQueue.listIterator(0);
        ByteBuffer clientAppData = iterApp.next().getBuffer();
        ByteBuffer clientNetData = iterNet.next().getBuffer();
        ByteBuffer serverAppData = iterApp.next().getBuffer();
        ByteBuffer serverNetData = iterNet.next().getBuffer();

        switch (handshakeStatus) {
            case NEED_UNWRAP:
                Future<Integer> rd = readFromChannelFuture(clientNetData);
                Integer res = -1;
                try {
                    res = rd.get();
                } catch (InterruptedException e) {
                    // TODO: logging
                } catch (ExecutionException e) {
                    // TODO: logging
                }
                if (res < 0) {
                    if (engine.isInboundDone() && engine.isOutboundDone()) {
                        return false;
                    }
                    try {
                        engine.closeInbound();
                    } catch (SSLException e) {
                    }
                    engine.closeOutbound();
                    // After closeOutbound the engine will be set to WRAP state, in order to try to send a close message to the client.
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                }
                clientNetData.flip();
                try {
                    sslEngineResult = engine.unwrap(clientNetData, clientAppData);
                    clientNetData.compact();
                    handshakeStatus = sslEngineResult.getHandshakeStatus();
                } catch (SSLException sslException) {
                    engine.closeOutbound();
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                }
                switch (sslEngineResult.getStatus()) {
                    case OK:
                        break;
                    case BUFFER_OVERFLOW:
                        // Will occur when clientAppData's capacity is smaller than the data derived from clientNetData's unwrap.
                        //FIXME: handle this condition
                        break;
                    case BUFFER_UNDERFLOW:
                        // Will occur either when no data was read from the client or when the clientNetData buffer was too small to hold all client's data.
                        //FIXME: handle this condition
                        break;
                    case CLOSED:
                        if (engine.isOutboundDone()) {
                            return false;
                        } else {
                            engine.closeOutbound();
                            handshakeStatus = engine.getHandshakeStatus();
                            break;
                        }
                    default:
                        throw new IllegalStateException("Invalid SSL status: " + sslEngineResult.getStatus());
                }
                break;
            case NEED_WRAP:
                serverNetData.clear();
                try {
                    sslEngineResult = engine.wrap(serverAppData, serverNetData);
                    handshakeStatus = sslEngineResult.getHandshakeStatus();
                } catch (SSLException sslException) {
                    engine.closeOutbound();
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                }
                switch (sslEngineResult.getStatus()) {
                    case OK :
                        serverNetData.flip();
                        while (serverNetData.hasRemaining()) {
                            Future <Integer> wr = writeToChannelFuture(serverNetData);
                            try {
                                wr.get();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (ExecutionException e) {
                                e.printStackTrace();
                            }
                        }
                        break;
                    case BUFFER_OVERFLOW:
                        // Will occur if there is not enough space in serverNetData buffer to write all the data that would be generated by the method wrap.
                        // Since serverNetData is set to session's packet size we should not get to this point because SSLEngine is supposed
                        // to produce messages smaller or equal to that, but a general handling would be the following:
                        //FIXME: handle this condition
                        break;
                    case BUFFER_UNDERFLOW:
                        //Buffer underflow occurred after a wrap. I don't think we should ever get here
                        //FIXME: handle this condition
                    case CLOSED:
                        try {
                            serverNetData.flip();
                            while (serverNetData.hasRemaining()) {
                                Future <Integer> wr = writeToChannelFuture(serverNetData);
                                try {
                                    wr.get();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                    //FIXME: handle this condition
                                } catch (ExecutionException e) {
                                    e.printStackTrace();
                                    //FIXME: handle this condition
                                }
                            }
                            // At this point the handshake status will probably be NEED_UNWRAP so we make sure that peerNetData is clear to read.
                            clientNetData.clear();
                        } catch (Exception e) {
                            handshakeStatus = engine.getHandshakeStatus();
                        }
                        break;
                    default:
                        throw new IllegalStateException("Invalid SSL status: " + sslEngineResult.getStatus());
                }
                break;
            case NEED_TASK:
                Runnable task;
                while ((task = engine.getDelegatedTask()) != null) {
                    System.out.println("run task " + task);
                    task.run();
                }
                handshakeStatus = engine.getHandshakeStatus();
                break;
            case FINISHED:
                setSSLHandshakeRequired(false);
                setSSLHandshakeSuccess(true);
                return false;
            case NOT_HANDSHAKING:
                // FIXME: handle this condition
                setSSLHandshakeRequired(false);
                return false;
            default:
                throw new IllegalStateException("Invalid SSL status: " + handshakeStatus);
        }

        return true;
    }
}
