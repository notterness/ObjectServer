package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.requestcontext.ClientRequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class WriteObjectToFile implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(WriteObjectToFile.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.WRITE_OBJECT_TO_FILE;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final ClientRequestContext requestContext;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     */
    private final BufferManager clientReadBufferMgr;

    /*
     ** The clientFullBufferPtr is used to track ByteBuffer(s) that are filled with client object data and are
     **   ready to be written out to the file on disk for later comparison.
     */
    private final BufferManagerPointer readBufferPointer;

    private final Operation readBufferMetering;

    /*
    ** The name of the file that Object will be saved in
     */
    private final String fileName;

    /*
     ** The following operations complete() method will be called when this operation and it's
     **   dependent operation are finished. This allows the upper layer to clean up and
     **   release any resources.
     */
    private final Operation completeCallback;

    private BufferManagerPointer clientFileWritePtr;

    /*
     ** The following are used to keep track of how much has been written to this Storage Server and
     **   how much is supposed to be written.
     */
    private final int bytesToWriteToFile;
    private int fileBytesWritten;

    private int savedSrcPosition;

    /*
     ** The following are used for the file management
     */
    private FileChannel writeFileChannel;

    /*
     */
    public WriteObjectToFile(final ClientRequestContext requestContext, final BufferManagerPointer readBufferPtr,
                             final Operation metering, final String fileName, final int objectSize,
                             final Operation completeCb) {

        this.requestContext = requestContext;
        this.clientReadBufferMgr = requestContext.getClientReadBufferManager();
        this.fileName = fileName;
        this.completeCallback = completeCb;

        this.readBufferPointer = readBufferPtr;

        this.readBufferMetering = metering;
        this.bytesToWriteToFile = objectSize;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

    /*
     ** This returns the BufferManagerPointer obtained by this operation, if there is one. If this operation
     **   does not use a BufferManagerPointer, it will return null.
     */
    public BufferManagerPointer initialize() {
        /*
         ** This keeps track of the number of bytes that have been written and how many
         **   need to be written.
         */
        fileBytesWritten = 0;

        /*
         ** Register this with the Buffer Manager to allow it to be event(ed) when
         **   buffers are added by the read producer.
         **
         ** This operation (WriteObectToFile) is a consumer of ByteBuffer(s) produced by the ReadBuffer operation.
         */
        clientFileWritePtr = clientReadBufferMgr.register(this, readBufferPointer);

        /*
         ** savedSrcPosition is used to handle the case where there are multiple readers from the readBufferPointer and
         **   there has already been data read from the buffer. In that case, the position() will not be zero, but there
         **   is a race condition as to how the cursors within the "base" buffer are adjusted. The best solution is to
         **   use a "copy" of the buffer and to set its cursors appropriately.
         */
        ByteBuffer readBuffer;
        if ((readBuffer = clientReadBufferMgr.peek(clientFileWritePtr)) != null) {
            savedSrcPosition = readBuffer.position();

            /*
             ** Add this to the execute queue since there is already data in a buffer to decrypt
             */
            event();
        } else {
            savedSrcPosition = 0;

            /*
            ** Need to provide a buffer to the READ_BUFFER operation to allow the NioSocket to read in the next data
            **   bytes.
             */
            readBufferMetering.event();
        }

        /*
         ** Open up the File for writing
         */
        File outFile = new File("./logs/" + fileName);
        try {
            writeFileChannel = new FileOutputStream(outFile, false).getChannel();
        } catch (FileNotFoundException ex) {
            LOG.info("WriteObjectToFile file not found fileName: " + fileName + " exception: " + ex.getMessage());
            writeFileChannel = null;
        }

        LOG.info("WriteObjectToFile initialize done savedSrcPosition: " + savedSrcPosition);

        return clientFileWritePtr;
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
        boolean outOfBuffers = false;

        while (!outOfBuffers) {
            if ((readBuffer = clientReadBufferMgr.peek(clientFileWritePtr)) != null) {
                /*
                 ** Create a temporary ByteBuffer to hold the readBuffer so that it is not
                 **  affecting the position() and limit() indexes
                 **
                 ** NOTE: savedSrcPosition is modified within the writeFileChannel.write() handling as the write may
                 **   only consume a portion of the buffer and it will take multiple passes through using the same
                 **   buffer to actually write all the data to the file.
                 */
                ByteBuffer srcBuffer = readBuffer.duplicate();
                srcBuffer.position(savedSrcPosition);

                if (writeFileChannel != null) {
                    try {
                        int bytesWritten = writeFileChannel.write(srcBuffer);

                        fileBytesWritten += bytesWritten;
                        if (srcBuffer.remaining() != 0) {
                            /*
                             ** Save this for the next time around since a temporary buffer is being used.
                             */
                            savedSrcPosition = srcBuffer.position();

                            /*
                             ** Queue this up to try again later and force the exit from the while loop
                             */
                            event();
                            outOfBuffers = true;
                        } else {
                            /*
                             ** Done with this buffer, see if there are more to write to the file
                             */
                            clientReadBufferMgr.updateConsumerReadPointer(clientFileWritePtr);
                            savedSrcPosition = 0;
                        }
                    } catch (IOException io_ex) {
                        /*
                         ** Not going to be able to write anything else, so call complete() and
                         **   terminate this operation.
                         */
                        requestContext.getHttpInfo().setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500,
                                "\"WriteObjectToFile failure\"");
                        LOG.info("WriteObjectToFile write exception: " + io_ex.getMessage());

                        closeOutFile(true);
                        completeCallback.event();
                    }
                }

            } else {
                LOG.info("WriteObjectToFile out of read buffers bytesWritten: " +
                        fileBytesWritten + " bytesToWriteToFile: " + bytesToWriteToFile);

                /*
                 ** Check if all the bytes (meaning the amount passed in the content-length in the HTTP header)
                 **   have been written to the file. If not, dole out another ByteBuffer to the NIO read
                 **   operation.
                 */
                if (fileBytesWritten < bytesToWriteToFile) {
                    readBufferMetering.event();
                } else if (fileBytesWritten == bytesToWriteToFile) {
                    /*
                     ** Done with this operation, so set the flag within the RequestContext. The SetupStorageServerPut
                     **   operation is dependent upon this write of the data to disk completing as well as the Md5 digest
                     **   being completed before it can send status back to the Object Server.
                     ** Tell the SetupStorageServerPut operation that produced this that it is done.
                     */
                    requestContext.setAllObjectDataWritten();

                    closeOutFile(false);
                    completeCallback.event();
                }

                outOfBuffers = true;
            }
        }
    }

    /*
    ** This is used to close out the file. It has a flag to delete the file in the case of an error
     */
    private void closeOutFile(final boolean deleteFile) {
        try {
            writeFileChannel.close();
        } catch (IOException ex) {
            LOG.info("WriteObjectToFile close exception: " + ex.getMessage());
        }
        writeFileChannel = null;

        System.out.println("WriteToFile closeOutFile() filename: " + fileName);
        if (deleteFile) {
            LOG.warn("Deleting file: " + fileName + " due to error: " + requestContext.getHttpParseStatus());

            Path filePath = Paths.get(fileName);
            try {
                Files.deleteIfExists(filePath);
            } catch (IOException ex) {
                LOG.warn("Failure deleting file: " + fileName + " exception: " + ex.getMessage());
            }
        }
    }

    /*
     */
    public void complete() {
        LOG.info("WriteObjectToFile complete");

        clientReadBufferMgr.unregister(clientFileWritePtr);
        clientFileWritePtr = null;
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
        //LOG.info("WriteObjectToFile markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("WriteObjectToFile markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("WriteObjectToFile markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("WriteObjectToFile markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("WriteObjectToFile hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    type: " + operationType);
        clientFileWritePtr.dumpPointerInfo();
        LOG.info("");
    }

}
