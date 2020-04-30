package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.buffermgr.ChunkMemoryPool;
import com.webutils.webserver.common.Md5ResultHandler;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ObjectInfo;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SetupObjectGet implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SetupObjectGet.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.SETUP_OBJECT_GET;

    /*
     ** Strings used to build the success response for the chunk write
     */
    private final static String SUCCESS_HEADER_1 = "opc-client-request-id: ";
    private final static String SUCCESS_HEADER_2 = "opc-request-id: ";
    private final static String SUCCESS_HEADER_3 = "Etag: ";
    private final static String SUCCESS_HEADER_4 = "Content-MD5: ";
    private final static String SUCCESS_HEADER_5 = "last-modified: ";
    private final static String SUCCESS_HEADER_6 = "archival-state: ";
    private final static String SUCCESS_HEADER_7 = "version-id ";
    private final static String SUCCESS_HEADER_8 = "Content-Length: ";


    /*
     ** The operations are all tied together via the RequestContext
     */
    private final RequestContext requestContext;

    private final MemoryManager memoryManager;

    private final ChunkMemoryPool chunkMemPool;

    /*
     ** The completeCallback will cause the final response to be sent out.
     */
    private final Operation completeCallback;

    /*
     ** There are two operations required to read data out of the clientReadBufferMgr and process it
     **   The Md5 Digest and the Encryption operations.
     **
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> objectGetHandlerOps;

    private final Md5ResultHandler updater;

    /*
     ** This is used to prevent the Operation setup code from being called multiple times in the execute() method
     */
    private boolean getOperationSetupDone;

    private ObjectInfo objectInfo;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** This is used to setup the initial Operation dependencies required to handle the Storage Server GET
     **   request. This is how chunks of data for an Object are read to the backing storage.
     */
    public SetupObjectGet(final RequestContext requestContext, final MemoryManager mempryManager,
                          final ChunkMemoryPool chunkMemPool, final Operation completeCb) {

        this.requestContext = requestContext;
        this.memoryManager = mempryManager;
        this.chunkMemPool = chunkMemPool;
        this.completeCallback = completeCb;

        /*
         ** Setup the list of Operations currently used to handle the V2 PUT
         */
        this.objectGetHandlerOps = new HashMap<>();

        this.updater = requestContext.getMd5ResultHandler();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
         **
         */
        getOperationSetupDone = false;
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
        return null;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    public void execute() {
        if (!getOperationSetupDone) {
            HttpRequestInfo objectHttpInfo = requestContext.getHttpInfo();

            /*
            ** The ObjectInfo is used to hold the information required to read the object data from the Storage Servers.
             */
            objectInfo = new ObjectInfo(objectHttpInfo);

            ReadObjectChunks readChunks = new ReadObjectChunks(requestContext, memoryManager, chunkMemPool,
                    objectInfo, this);
            objectGetHandlerOps.put(readChunks.getOperationType(), readChunks);
            readChunks.initialize();

            RetrieveObjectInfo retrieveObjectInfo = new RetrieveObjectInfo(requestContext, objectInfo, readChunks, this);
            objectGetHandlerOps.put(retrieveObjectInfo.getOperationType(), retrieveObjectInfo);
            retrieveObjectInfo.initialize();
            retrieveObjectInfo.event();

            getOperationSetupDone = true;
        } else {
            complete();
        }
    }

    /*
     ** This complete() is called when the WriteToFile operation has written all of its buffers
     **   to the file.
     */
    public void complete() {

        LOG.info("SetupObjectGet[" + requestContext.getRequestId() + "] complete()");

        /*
         ** Remove the COMPUTE_MD5_DIGEST operation from the list since it will have already called it's
         **   complete() operation.
         */
        objectGetHandlerOps.remove(OperationTypeEnum.COMPUTE_MD5_DIGEST);

        /*
         ** Call the complete() method for any operations that this one created.
         */
        Collection<Operation> createdOperations = objectGetHandlerOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        objectGetHandlerOps.clear();

        completeCallback.event();

        LOG.info("SetupObjectGet[" + requestContext.getRequestId() + "] completed");
    }


    /*
     ** The following are used to add the Operation to the event thread's event queue. The
     **   Operation can be added to the immediate execution queue or the delayed
     **   execution queue.
     **
     ** The following methods are called by the event thread under a queue mutex.
     **   markRemoveFromQueue - This method is used by the event thread to update the queue
     **     the Operation is on when the operation is removed from the queue.
     **   markAddedToQueue - This method is used when an Operation is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **   hasWaitTimeElapsed - Is this Operation ready to run again to check some timeout condition
     **
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("SetupObjectGet[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SetupObjectGet[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SetupObjectGet[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SetupObjectGet[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SetupObjectGet[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("   -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = objectGetHandlerOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }

    /*
     ** This builds the OK_200 response headers for the GET Object command. This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     **   Content-Md5
     */
    private String buildSuccessHeader() {
        String successHeader;

        String contentMD5 = updater.getComputedMd5Digest();
        String opcClientRequestId = requestContext.getHttpInfo().getOpcClientRequestId();
        String opcRequestId = requestContext.getHttpInfo().getOpcRequestId();

        if (opcClientRequestId != null) {
            successHeader = SUCCESS_HEADER_1 + opcClientRequestId + "\n" + SUCCESS_HEADER_2 + opcRequestId + "\n" +
                    SUCCESS_HEADER_3 + contentMD5 + "\n";
        } else {
            successHeader = SUCCESS_HEADER_2 + opcRequestId + "\n" + SUCCESS_HEADER_3 + contentMD5 + "\n";
        }

        return successHeader;
    }

}
