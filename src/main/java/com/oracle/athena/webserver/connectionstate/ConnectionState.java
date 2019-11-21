package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.http.BuildHttpResult;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.*;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/*
 ** This is used to keep track of the state of the connection for its lifetime.
 **
 ** chan is the socket that the connection is using to transfer data. It is assigned
 **   later and not at instantiation time to allow the ConnectionState objects to be
 **   managed via a pool.
 */
abstract public class ConnectionState {

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
    **      than the actual number of buffers that are required to read all of the content data in.
    **    allocatedDataBuffers - This is the number of buffers that have been allocated to read in content data and are
    **      waiting to have reads performed on the.
    **    outstandingDataReadCount - This is the number of outstanding content reads are currently in progress.
    **    contentAllRead -
    **
    ** TODO: outstandingDataReadCount and dataBufferReadsCompleted are marked public so the subclass
    **   ClientConnState can access them
     */
    protected AtomicInteger outstandingDataReadCount;
    private int requestedDataBuffers;
    private int allocatedDataBuffers;
    protected AtomicInteger dataBufferReadsCompleted;
    private AtomicBoolean contentAllRead;

    AtomicBoolean bufferAllocationFailed;


    /*
    ** The following are used to insure that a ConnectionState is never on more than one queue and that
    **   if there is a choice between being on the timed wait queue (connOnDelayedQueue) or the normal
    **   execution queue (connOnExecutionQueue) is will always go on the execution queue.
     */
    private boolean connOnDelayedQueue;
    private boolean connOnExecutionQueue;

    private final Object queueMutex;

    /*
    ** The following are used to keep track of the content being read in.
     */
    AtomicLong contentBytesToRead;
    private AtomicLong contentBytesAllocated;
    private AtomicLong contentBytesRead;


    /*
     ** The next four items are associated with the thread that is running the ConnectionState
     **   state machine.
     */
    private ServerWorkerThread workerThread;
    BufferStatePool bufferStatePool;
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
     **
     */
    protected BlockingQueue<BufferState> dataReadDoneQueue;

    /*
    ** The following is used to indicate that there was a channel error and the connection should be
    **   closed out.
     */
    protected AtomicBoolean channelError;

    /*
    ** The following is used to indicate there has been a failure in the HTTP parsing.
     */
    protected AtomicBoolean httpParsingError;


    /*
    ** The following is used to check that progress is being made on a channel and the
    **   client is not slow sending or receiving data.
     */
    protected TimeoutChecker timeoutChecker;

    public ConnectionState(final int uniqueId) {
        outstandingDataReadCount = new AtomicInteger(0);

        connStateId = uniqueId;

        bufferStatePool = null;
        resultBuilder = null;

        channelError = new AtomicBoolean(false);

        connOnDelayedQueue = false;
        connOnExecutionQueue = false;
        queueMutex = new Object();

        requestedDataBuffers = 0;
        allocatedDataBuffers = 0;
        allocatedDataBufferQueue = new LinkedList<>();

        bufferAllocationFailed = new AtomicBoolean(false);

        dataBufferReadsCompleted = new AtomicInteger(0);
        dataReadDoneQueue = new LinkedBlockingQueue<>(MAX_OUTSTANDING_BUFFERS * 2);

        contentBytesToRead = new AtomicLong(0);
        contentBytesAllocated = new AtomicLong(0);
        contentBytesRead = new AtomicLong(0);
        contentAllRead = new AtomicBoolean(false);

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

    abstract public void setReadState(final BufferState bufferState, final BufferStateEnum newState);

    /*
    ** This is to perform some initial setup and could be removed if the ConnectionState is tied to the
    **   thread it is going to run under.
     */
    protected void setupInitial() {
        bufferStatePool = workerThread.getBufferStatePool();
        writeThread = workerThread.getWriteThread();
        resultBuilder = workerThread.getResultBuilder();

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
     */
    public void addToWorkQueue(final boolean delayedExecution) {
        synchronized (queueMutex) {
            workerThread.put(this, delayedExecution);
        }
    }

    public void markAddedToDelayedQueue() {
        connOnDelayedQueue = true;
        nextExecuteTime = System.currentTimeMillis() + ConnectionState.TIME_TILL_NEXT_TIMEOUT_CHECK;
    }

    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {

        if (delayedExecutionQueue) {
            connOnDelayedQueue = false;
            nextExecuteTime = 0;
        } else {
            connOnExecutionQueue = false;
        }

        System.out.println("ConnectionState[" + connStateId + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
    }


    public boolean hasWaitTimeElapsed() {
        long currTime = System.currentTimeMillis();

        if (currTime < nextExecuteTime) {
            return false;
        }

        System.out.println("ServerWorkerThread[" + connStateId + "] waitTimeElapsed " + currTime);
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

    /*
    ** This is the function to add BufferState to the available queue. This means the BufferState are
    **   now ready to have data read into them.
     */
    protected int allocClientReadBufferState() {
        System.out.println("ServerWorkerThread[" + connStateId + "] allocClientReadBufferState(1) " + Thread.currentThread().getName());

        while (requestedDataBuffers > 0) {
            /*
             ** Only have a limited number of buffer sitting in the allocated pool to prevent
             **   resource starvation for other connections.
             */
            if (allocatedDataBuffers < MAX_OUTSTANDING_BUFFERS) {
                BufferState bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_DATA_FROM_CHAN, MemoryManager.MEDIUM_BUFFER_SIZE);
                if (bufferState != null) {
                    System.out.println("ServerWorkerThread[" + connStateId + "] allocClientReadBufferState(2)");

                    allocatedDataBuffers++;
                    allocatedDataBufferQueue.add(bufferState);

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
    protected void readIntoDataBuffers() {
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
    private void closeChannel() {
        synchronized (connChanMutex) {
            try {
                /*
                 ** Handle case where the channel has already been closed.
                 */
                if (connChan != null) {
                    connChan.close();
                }
            } catch (IOException io_ex) {
                System.out.println("ConnectionState[" + connStateId + "] closeChannel() " + io_ex.getMessage());
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

    /*
    ** This is used to put the ConnectionState back into a pristine state so that it can be used
    **   to handle the next HTTP connection.
     */
    public void reset() {

        System.out.println("ConnectionState[" + connStateId + "] reset()");

        releaseBufferState();

        closeChannel();

        /*
         ** Clear the items associated with a particular worker thread that was used to execute this
         **   ConnectionState as it may change the next time this ConnectionState is used.
         */
        workerThread = null;
        writeThread = null;
        bufferStatePool = null;
        resultBuilder = null;

        channelError.set(false);

        /*
        ** Clear out the information about the content.
         */
        contentBytesToRead.set(0);
        contentBytesAllocated.set(0);
        contentBytesRead.set(0);
        contentAllRead.set(false);

        httpParsingError.set(false);
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

        int bytesRead = bufferState.getBuffer().position();
        addDataBuffer(bufferState, bytesRead);

        /*
         ** Update the channel's health timeout
         */
        timeoutChecker.updateTime();

        System.out.println("ConnectionState[" + connStateId + "].dataReadCompleted() outstandingReadCount: " + readCount);

        addToWorkQueue(false);
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
        contentBytesRead.addAndGet(bytesRead);

        try {
            dataReadDoneQueue.put(bufferState);
            readCompletedCount = dataBufferReadsCompleted.incrementAndGet();
        } catch (InterruptedException int_ex) {
            /*
             ** TODO: This is an error case and the connection needs to be closed
             */
            System.out.println("dataReadCompleted(" + connStateId + ") " + int_ex.getMessage());
            readCompletedCount = dataBufferReadsCompleted.get();
        }

        System.out.println("ConnectionState[" + connStateId + "].addDataBuffer() readCompletedCount: " + readCompletedCount);

        determineNextContentRead();
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

    /*
    ** This function determines what to do next with content buffers.
    **
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

        System.out.println("ConnectionState[" + connStateId + "] determineNextContentRead() buffersNeeded: " + buffersNeeded +
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
                    System.out.println("ConnectionState[" + connStateId + "] determineNextContentRead() contentAllRead true");

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
     ** Returns if there has been an error on this AsynchronousConnectionChannel
     */
    public boolean hasChannelFailed() {
        return channelError.get();
    }


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
                System.out.println("ConnectionState[" + connStateId + "] socket closed: connChan null");

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
                System.out.println("readFromChannel[" + connStateId + "] socket: " + addr);
            } catch (IOException ex) {
                System.out.println("socket closed " + ex.getMessage());

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
        ByteBuffer readBuffer = readBufferState.getBuffer();
        connChan.read(readBuffer, readBufferState, new CompletionHandler<Integer, BufferState>() {

            @Override
            public void completed(final Integer bytesRead, final BufferState readBufferState) {
                System.out.println("readFromChannel[" + connStateId + "] bytesRead: " + bytesRead + " thread: " + Thread.currentThread().getName());

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
                System.out.println("readFromChannel[" + connStateId + "] bytesRead: " + exc.getMessage() + " thread: " + Thread.currentThread().getName());

                closeChannel();

                /*
                 ** This buffer read failed, need to mark the buffer to indicate that it is not valid
                 */
                setReadState(readBufferState, BufferStateEnum.READ_ERROR);
            }
        });  // end of chan.read()
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
        System.out.println("ConnectionState buffer: " + tmp);

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

}
