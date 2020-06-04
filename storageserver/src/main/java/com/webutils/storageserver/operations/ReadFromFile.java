package com.webutils.storageserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public class ReadFromFile implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(ReadFromFile.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.READ_FROM_FILE;

    /*
     ** Strings used to build the success response for the chunk read
     */
    private final static String SUCCESS_HEADER_1 = "opc-client-request-id: ";
    private final static String SUCCESS_HEADER_2 = "opc-request-id: ";
    private final static String SUCCESS_HEADER_3 = "Content-Length: ";


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
    ** The chunkGetBufferMgr provides the buffers that are used for the following:
    **   - A buffer the write the response header into
    **   - Buffers to hold the requested chunk data
    **   - The filled in buffers that are written out the TCP port back to the Object Server
     */
    private final BufferManager chunkGetBufferMgr;

    private final Operation bufferMetering;
    private final BufferManagerPointer meteringPointer;

    /*
     ** The following operations complete() method will be called when this operation and it's
     **   dependent operation are finished. This allows the upper layer to clean up and
     **   release any resources.
     */
    private final Operation completeCallback;

    /*
     ** The chunkFileReadPtr is used to track ByteBuffer(s) that are filled with client object data and are
     **   ready to be written out the TCP connection to the Object Server.
     */
    private BufferManagerPointer chunkFileReadPtr;

    /*
     ** The following are used to keep track of how much has been written to this Storage Server and
     **   how much is supposed to be written.
     */
    private long bytesToReadFromFile;
    private long fileBytesRead;

    private boolean respHeaderBuilt;
    private int savedSrcPosition;

    /*
     ** The following are used for the file management
     */
    private FileChannel readFileChannel;

    /*
     */
    public ReadFromFile(final RequestContext requestContext, final Operation metering, final BufferManagerPointer meteringPtr,
                       final Operation completeCb) {

        this.requestContext = requestContext;

        this.bufferMetering = metering;
        this.meteringPointer = meteringPtr;

        /*
         ** Since this is data leaving the Storage Server, it uses the write buffer manager
         */
        this.chunkGetBufferMgr = requestContext.getClientWriteBufferManager();
        this.completeCallback = completeCb;

        this.respHeaderBuilt = false;

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
     ** This returns the chunkFileReadPtr. The chunkFileReadPtr is the buffer that chunk data is read into from the
     **   backing storage. The operation that writes the data out the TCP port to the Object Server will depend upon
     **   this pointer to know when data is available to write out the port.
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
        chunkFileReadPtr = chunkGetBufferMgr.register(this, meteringPointer);

        /*
         ** savedSrcPosition is used to handle the case where the data may come back from the file in small pieces
         **   (meaning less than a buffers worth of data)
         */
        savedSrcPosition = 0;

        String filePathNameStr = buildChunkFileName();

        /*
         ** Open up the File for reading
         */
        bytesToReadFromFile = 0;
        File inFile = new File(filePathNameStr);
        try {
            readFileChannel = new FileInputStream(inFile).getChannel();

            try {
                bytesToReadFromFile = readFileChannel.size();
            } catch (IOException io_ex) {
                LOG.error("Unable to obtain file length - " + filePathNameStr + " ex:" + io_ex.getMessage());
            }
        } catch (FileNotFoundException ex) {
            LOG.info("ReadFromFile[" + requestContext.getRequestId() + "] file not found: " + ex.getMessage());

            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Unable to open file - " + filePathNameStr + "\"" +
                    "\r\n}";
            requestContext.getHttpInfo().emitMetric(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            requestContext.getHttpInfo().setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412);

            readFileChannel = null;

            /*
             ** Need to cleanup
             */
            chunkGetBufferMgr.unregister(chunkFileReadPtr);
            chunkFileReadPtr = null;
            return null;
        }

        LOG.info("ReadFromFile[" + requestContext.getRequestId() + "] initialize done - bytesToReadFromFile: " +
                bytesToReadFromFile);

        return chunkFileReadPtr;
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

        while ((readBuffer = chunkGetBufferMgr.peek(chunkFileReadPtr)) != null) {
            if (!respHeaderBuilt) {
                boolean goodResponseSent = buildResponseHeader(readBuffer);

                /*
                ** Done with this buffer, now check if there is data to read from the file
                 */
                readBuffer.flip();
                chunkGetBufferMgr.updateProducerWritePointer(chunkFileReadPtr);

                if (goodResponseSent) {
                    bufferMetering.event();
                } else {
                    /*
                    ** This operation has sent out an error and is all done
                     */
                    requestContext.setAllClientBuffersFilled();
                    completeCallback.event();
                }

                respHeaderBuilt = true;
            } else {
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
                            chunkGetBufferMgr.updateProducerWritePointer(chunkFileReadPtr);
                            savedSrcPosition = 0;

                            LOG.info("ReadFromFile[" + requestContext.getRequestId() + "] read buffer full bytesRead: " +
                                    fileBytesRead + " bytesToReadFromFile: " + bytesToReadFromFile);

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
                        LOG.info("ReadFromFile[" + requestContext.getRequestId() + "] read exception: " + io_ex.getMessage());
                        completeCallback.event();
                    }
                } else {
                    LOG.error("readFileChannel is null");
                }
            } // check if respHeaderBuilt

        } // while()
    }

    /*
     ** This will never be called for the CloseOutRequest. When the execute() method completes, the
     **   RequestContext is no longer "running".
     */
    public void complete() {
        LOG.info("ReadFromFile[" + requestContext.getRequestId() + "] complete");

        if (readFileChannel != null) {
            try {
                readFileChannel.close();
            } catch (IOException ex) {
                LOG.info("ReadFromFile[" + requestContext.getRequestId() + "] close exception: " + ex.getMessage());
            }
            readFileChannel = null;
        }

        if (chunkFileReadPtr != null) {
            chunkGetBufferMgr.unregister(chunkFileReadPtr);
            chunkFileReadPtr = null;
        }
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
        //LOG.info("ReadFromFile[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ReadFromFile[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ReadFromFile[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ReadFromFile[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ReadFromFile[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        chunkFileReadPtr.dumpPointerInfo();
        LOG.info("");
    }

    /*
     ** This builds the response headers for the GET Object command. This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     **   Content-Length
     */
    private boolean buildResponseHeader(final ByteBuffer respBuffer) {
        boolean goodResponseSent = true;
        String responseHeader;

        String opcClientRequestId = requestContext.getHttpInfo().getOpcClientRequestId();
        String opcRequestId = requestContext.getHttpInfo().getOpcRequestId();

        if (opcClientRequestId != null) {
            responseHeader = SUCCESS_HEADER_1 + opcClientRequestId + "\n" + SUCCESS_HEADER_2 + opcRequestId + "\n" +
                    SUCCESS_HEADER_3 + bytesToReadFromFile + "\n\n";
        } else {
            responseHeader = SUCCESS_HEADER_2 + opcRequestId + "\n" + SUCCESS_HEADER_3 + bytesToReadFromFile + "\n\n";
        }

        String tmpStr;
        if ((readFileChannel != null) && (bytesToReadFromFile != 0)) {
            tmpStr = "HTTP/1.1 200 OK" +
                    "\r\n" +
                    "Content-Type: text/html\n" +
                    responseHeader;
        } else {
            goodResponseSent = false;
            String content = "\r\n" +
                    "{\r\n" +
                    "  \"Description\": Chunk Not Found" +
                    "\r\n}";

            if (opcClientRequestId != null) {
                tmpStr = "HTTP/1.1 404" +
                        "\r\n" +
                        SUCCESS_HEADER_2 + opcRequestId + "\n" +
                        "Content-Length: " + content.length() + "\n\n" +
                        content;
            } else {
                tmpStr = "HTTP/1.1 404" +
                        "\r\n" +
                        SUCCESS_HEADER_1 + opcClientRequestId + "\n" +
                        SUCCESS_HEADER_2 + opcRequestId + "\n" +
                        "Content-Length: " + content.length() + "\n\n" +
                        content;
            }
        }

        HttpInfo.str_to_bb(respBuffer, tmpStr);

        return goodResponseSent;
    }

    /*
     ** This builds the filePath to where the chunk of data will be read from.
     **
     ** It is comprised of the chunk location, chunk number, chunk lba and located at
     **   ./logs/StorageServer"IoInterfaceIdentifier"/chunk_"chunk location"_"chunk number"_"chunk lba".dat
     *
     ** FIXME: This method and the one it WriteToFile need to be put into a common place.
     */
    public String buildChunkFileName() {
        int chunkNumber = requestContext.getHttpInfo().getObjectChunkNumber();
        int chunkLba = requestContext.getHttpInfo().getObjectChunkLba();
        String chunkLocation = requestContext.getHttpInfo().getObjectChunkLocation();

        String directory = "./logs/StorageServer" + requestContext.getIoInterfaceIdentifier();

        if ((chunkNumber == -1) || (chunkLba == -1) || (chunkLocation == null)) {
            LOG.error("WriteToFile chunkNumber: " + chunkNumber + " chunkLba: " + chunkLba + " chunkLocation: " + chunkLocation);
            return null;
        }

        return directory + "/" + "/chunk_" + chunkLocation + "_" + chunkNumber + "_" + chunkLba + ".dat";
    }


}
