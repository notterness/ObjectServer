package com.webutils.webserver.operations;

import com.webutils.storageserver.operations.ReadFromFile;
import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.PutObjectParams;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ReadObjectFromFile implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(ReadFromFile.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.READ_OBJECT_FROM_FILE;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** The readBufferMgr provides the buffers that are used for the following:
     **   - Buffers to hold the requested files data
     **   - The filled in buffers that are written out the TCP port to the Object Server
     */
    private final BufferManager readBufferMgr;

    private final Operation bufferMetering;
    private final BufferManagerPointer meteringPointer;

    private final PutObjectParams putParams;

    /*
     ** The following operations complete() method will be called when this operation and it's
     **   dependent operation are finished. This allows the upper layer to clean up and
     **   release any resources.
     */
    private final Operation completeCallback;

    /*
     ** The fileReadPtr is used to track ByteBuffer(s) that are filled with client object data and are
     **   ready to be written out the TCP connection to the Object Server.
     */
    private BufferManagerPointer fileReadPtr;

    /*
     ** The following are used to keep track of how much has been written to this Storage Server and
     **   how much is supposed to be written.
     */
    private long bytesToReadFromFile;
    private long fileBytesRead;

    private int savedSrcPosition;

    /*
     ** The following are used for the file management
     */
    private FileChannel readFileChannel;

    /*
     */
    public ReadObjectFromFile(final RequestContext requestContext, final Operation metering,
                              final BufferManagerPointer meteringPtr, final PutObjectParams putParams,
                              final Operation completeCb) {

        this.requestContext = requestContext;

        this.bufferMetering = metering;
        this.meteringPointer = meteringPtr;

        this.putParams = putParams;

        /*
         ** Since this is data leaving the client system and going to the Object Server, it uses the write buffer manager
         */
        this.readBufferMgr = requestContext.getClientWriteBufferManager();
        this.completeCallback = completeCb;

        /*
         ** This starts out not being on any queue
         */
        this.onExecutionQueue = false;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

    /*
     ** This returns the fileReadPtr. The fileReadPtr is the buffer that file data is read into from the
     **   client's file.
     ** The operation that writes the data out the TCP port to the Object Server will depend upon
     **   this pointer to know when data is available to write out the port.
     ** The operation that computes the Md5 checksum for the file also uses this pointer.
     */
    public BufferManagerPointer initialize() {
        /*
         ** This keeps track of the number of bytes that have been read to determine how many still need to be read
         */
        fileBytesRead = 0;

        /*
         ** Register this with the Buffer Manager to allow it to be event(ed) when
         **   buffers are added by the read producer.
         **
         ** This operation (ReadFromFile) is a producer of ByteBuffer(s) that are then written out the TCP port to the
         **   Object Server
         */
        fileReadPtr = readBufferMgr.register(this, meteringPointer);

        /*
         ** savedSrcPosition is used to handle the case where the data may come back from the file in small pieces
         **   (meaning less than a buffers worth of data)
         */
        savedSrcPosition = 0;

        /*
         ** Open up the File for reading
         */
        bytesToReadFromFile = 0;
        File inFile = new File(putParams.getFilePathName());
        try {
            readFileChannel = new FileInputStream(inFile).getChannel();

            try {
                bytesToReadFromFile = readFileChannel.size();

                /*
                ** Update the PutObjectParams
                 */
                if (!putParams.setObjectSizeInBytes(bytesToReadFromFile)) {
                    try {
                        readFileChannel.close();
                    } catch (IOException ex) {
                        LOG.error("Unable to close file - " + putParams.getFilePathName() + " ex:" + ex.getMessage());
                    }

                    readFileChannel = null;
                }
            } catch (IOException io_ex) {
                LOG.error("Unable to obtain file length - " + putParams.getFilePathName() + " ex:" + io_ex.getMessage());
                try {
                    readFileChannel.close();
                } catch (IOException ex) {
                    LOG.error("Unable to close file - " + putParams.getFilePathName() + " ex:" + ex.getMessage());
                }

                readFileChannel = null;
            }
        } catch (FileNotFoundException ex) {
            LOG.info("ReadObjectFromFile file not found: " + ex.getMessage());
            readFileChannel = null;
        }

        if (readFileChannel != null) {
            LOG.info("ReadObjectFromFile initialize done - bytesToReadFromFile: " + bytesToReadFromFile);
        } else {
            /*
            ** Log an error as there is no point in continuing
             */
            requestContext.getHttpInfo().setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500,
                    "\"ReadObjectFromFile failure\"");
        }

        return fileReadPtr;
    }

    /*
     ** The WriteToBuffer operation will have its "event()" method invoked whenever there is data read into
     **   the ClientReadBufferManager.
     */
    public void event() {
        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
     ** This method will go through all of the available buffers in the ClientReadBufferManager that have data
     **   in them and encrypt that data and put it into the StorageServerWriteBufferManager.
     ** There are two cases to consider for the loop:
     **   -> Are the ByteBuffer(s) in the ClientReadBufferManager that have data within them and are ready to be
     **        encrypted.]
     **   -> Are there ByteBuffer(s) in the StorageServerWriteBufferManager that are available. There is always the
     **        case where all the buffers in the StorageServerWriteBufferManager are waiting to be written to one or
     **        more Storage Servers and everything is going to back up until the writes start completing and making
     **        buffers available.
     */
    public void execute() {
        ByteBuffer readBuffer;

        while ((readBuffer = readBufferMgr.peek(fileReadPtr)) != null) {
            /*
            ** Create a temporary ByteBuffer to hold the readBuffer so that it is not
            **  affecting the position() and limit() indexes
            **
            ** NOTE: savedSrcPosition is modified within the readFileChannel.read() handling as the read may
            **   only consume a portion of the buffer and it will take multiple passes through using the same
            **   buffer to actually read all the data from the file.
             */
            ByteBuffer srcBuffer = readBuffer.duplicate();
            srcBuffer.position(savedSrcPosition);

            if (readFileChannel != null) {
                try {
                    int bytesRead = readFileChannel.read(srcBuffer);

                    fileBytesRead += bytesRead;
                    if (srcBuffer.remaining() != 0) {
                        /*
                        ** Save this for the next time around since a temporary buffer is being used.
                         */
                        savedSrcPosition = srcBuffer.position();

                        /*
                        ** Queue this up to try again later and force the exit from the while loop
                         */
                        event();
                    } else {
                        /*
                        ** Done with this buffer, see if there is more data to read from the file
                         */
                        readBufferMgr.updateProducerWritePointer(fileReadPtr);
                        savedSrcPosition = 0;

                        LOG.info("ReadObjectFromFile read buffer full bytesRead: " + fileBytesRead +
                                " bytesToReadFromFile: " + bytesToReadFromFile);

                        /*
                        ** Check if all the bytes have been read from the file. If not, dole out another ByteBuffer
                        **   to the NIO read operation.
                         */
                        if (fileBytesRead < bytesToReadFromFile) {
                            bufferMetering.event();
                        } else if (fileBytesRead == bytesToReadFromFile) {
                            /*
                            ** Done with this operation, so set the flag within the RequestContext that produced this that all
                            **   of the data to be written to the Object Server has been filled into the buffers.
                             */
                            requestContext.setAllClientBuffersFilled();
                            completeCallback.event();
                        }
                    }
                } catch (IOException io_ex) {
                    /*
                    ** Not going to be able to read anything else, so call complete() and terminate this operation.
                     */
                    LOG.info("ReadObjectFromFile read exception: " + io_ex.getMessage());
                    completeCallback.event();
                }
            } else {
                LOG.error("readFileChannel is null");
            }

        } // while()
    }

    /*
     ** This will never be called for the CloseOutRequest. When the execute() method completes, the
     **   RequestContext is no longer "running".
     */
    public void complete() {
        LOG.info("ReadObjectFromFile[" + requestContext.getRequestId() + "] complete");

        try {
            readFileChannel.close();
        } catch (IOException ex) {
            LOG.info("ReadObjectFromFile[" + requestContext.getRequestId() + "] close exception: " + ex.getMessage());
        }
        readFileChannel = null;

        readBufferMgr.unregister(fileReadPtr);
        fileReadPtr = null;
    }

    /*
     ** The following are used to add the Operation to the event thread's event queue. The
     **   Operation can be added to the immediate execution queue or the delayed
     **   execution queue.
     **
     ** The following methods are called by the event thread under a queue mutex.
     **   markRemoveFromQueue - This method is used by the event thread to update the queue
     **     the Operation is on when the operation is removed from the queue.
     **   markAddedToQueue - This method is used when an operation is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **   hasWaitTimeElapsed - Is this Operation ready to run again to check some timeout condition
     **
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("ReadObjectFromFile[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ReadObjectFromFile[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ReadObjectFromFile[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ReadObjectFromFile[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
        } else {
            onExecutionQueue = true;
        }
    }

    public boolean isOnWorkQueue() {
        return onExecutionQueue;
    }

    public boolean isOnTimedWaitQueue() {
        return false;
    }

    public boolean hasWaitTimeElapsed() {
        LOG.warn("ReadObjectFromFile[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        fileReadPtr.dumpPointerInfo();
        LOG.info("");
    }

}
