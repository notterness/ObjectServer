package com.webutils.chunkmgr.operations;

import com.webutils.chunkmgr.http.DeleteChunksContent;
import com.webutils.chunkmgr.requestcontext.ChunkAllocRequestContext;
import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.Sha256ResultHandler;
import com.webutils.webserver.operations.ComputeSha256Digest;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.operations.ParseContentBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class SetupDeleteChunks implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(SetupDeleteChunks.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.SETUP_DELETE_CHUNKS;

    private final ChunkAllocRequestContext requestContext;

    private final Operation metering;

    private final Operation completeCallback;

    private final DeleteChunksContent deleteChunksContent;

    private DeleteChunks deleteChunks;

    private final Sha256ResultHandler updator;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** There are two operations required to read data out of the clientReadBufferMgr and process it
     **   The Md5 Digest and the Encryption operations.
     **
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> handlerOperations;

    private boolean setupMethodDone;
    private boolean waitingOnOperations;

    /*
     ** This is used to setup the initial Operation dependencies required to handle the CreateBucket POST
     **   method.
     ** The completeCb will call the DetermineRequest operation's event() method when the POST completes.
     */
    public SetupDeleteChunks(final ChunkAllocRequestContext requestContext, final Operation metering,
                                 final Operation completeCb) {

        this.requestContext = requestContext;
        this.metering = metering;
        this.completeCallback = completeCb;

        this.deleteChunksContent = new DeleteChunksContent();

        this.updator = requestContext.getSha256ResultHandler();

        /*
         ** Setup the list of Operations currently used to handle the V2 PUT
         */
        handlerOperations = new HashMap<>();

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

        setupMethodDone = false;
        waitingOnOperations = true;

        return null;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    public void execute() {
        if (!setupMethodDone) {
            /*
             ** Add compute Sha-256 Digest as a dependency on the ClientReadBufferManager read pointer.
             */
            BufferManagerPointer readBufferPointer = requestContext.getReadBufferPointer();

            /*
             ** Once the parsing of the DELETE content data has taken place and the Sha-256 digest is determined to be valid,
             **   then the chunks need to be deleted from the ServiceServersDb.StorageServerChunk table and the request
             **   to delete the various chunks sent to the Storage Servers.
             */
            deleteChunks = new DeleteChunks(requestContext, deleteChunksContent, this);
            handlerOperations.put(deleteChunks.getOperationType(), deleteChunks);
            deleteChunks.initialize();

            /*
             ** Setup the Parser to pull the information out of the DELETE content. This will put the information into
             **   a temporary structure and once the Sha-256 digest completes (assuming it is successful) the delete
             **   of the chunks will take place.
             */
            ParseContentBuffers parseContentBuffers = new ParseContentBuffers(requestContext, readBufferPointer,
                    metering, deleteChunksContent, this);
            handlerOperations.put(parseContentBuffers.getOperationType(), parseContentBuffers);
            parseContentBuffers.initialize();

            /*
             ** The ComputeSha256Digest needs to be completed before the SendFinalStatus operation can be woken up
             **   to perform its work.
             **   The SendFinalStatus is dependent upon all the content data being processed and the Sha-256 Digest
             **   having completed.
             */
            List<Operation> callbackList = new LinkedList<>();
            callbackList.add(this);

            ComputeSha256Digest computeSha256Digest = new ComputeSha256Digest(requestContext, callbackList, readBufferPointer, updator);
            handlerOperations.put(computeSha256Digest.getOperationType(), computeSha256Digest);
            computeSha256Digest.initialize();

            /*
             ** Dole out another buffer to read in the content data if there is not data remaining in
             **   the buffer from the HTTP Parsing.
             */
            BufferManager clientReadBufferManager = requestContext.getClientReadBufferManager();
            ByteBuffer remainingBuffer = clientReadBufferManager.peek(readBufferPointer);
            if (remainingBuffer != null) {
                if (remainingBuffer.remaining() > 0) {
                    parseContentBuffers.event();
                } else {
                    metering.event();
                }
            }

            setupMethodDone = true;
        } else if (waitingOnOperations){
            /*
             ** This will be placed on the execute queue twice, once by the ParseContentBuffers operation when the
             **   parsing is complete and a second time when the ComputeSha256Digest has completed.
             */
            if (updator.getSha256DigestComplete() && requestContext.postMethodContentParsed()) {
                LOG.info("SetupDeleteChunks[" + requestContext.getRequestId() + "] Sha-256 and Parsing done");

                /*
                 ** Cleanup the operations
                 */
                Operation contentParser = handlerOperations.get(OperationTypeEnum.PARSE_CONTENT);
                contentParser.complete();
                handlerOperations.remove(OperationTypeEnum.PARSE_CONTENT);

                /*
                 ** Since the Sha-256 digest runs on a compute thread, it handles it's own complete() call. For that
                 **   reason, simply remove it from the PostHandlerOperations map.
                 */
                handlerOperations.remove(OperationTypeEnum.COMPUTE_SHA256_DIGEST);

                if (updator.checkContentSha256()) {
                    deleteChunks.event();
                } else {
                    /*
                     ** There was an error with the passed in or computed Sha-256 digest, so an error needs to be
                     **   returned to the client
                     */
                    complete();
                }

                waitingOnOperations = false;
            } else {
                LOG.info("SetupDeleteChunks[" + requestContext.getRequestId() + "] not completed Sha-256 digestComplete: " +
                        updator.getSha256DigestComplete() + " POST content parsed: " + requestContext.postMethodContentParsed());
            }
        } else {
            /*
             ** This execute() method may be called multiple times. This is really just a place holder to show that nothing
             **   will be done in this case.
             */
        }
    }

    /*
     ** This is called from both the EncryptBuffer and ComputeMd5Digest operations when they have completed their
     **   work.
     */
    public void complete() {
        /*
         ** Call the complete() methods for all of the Operations created to handle the POST method that did not have
         **   ordering dependencies due to the registrations with the BufferManager(s).
         */
        Collection<Operation> createdOperations = handlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        handlerOperations.clear();

        completeCallback.event();

        LOG.info("SetupDeleteChunks[" + requestContext.getRequestId() + "] completed");
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
        //LOG.info("SetupDeleteChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SetupDeleteChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SetupDeleteChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SetupDeleteChunks[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SetupDeleteChunks[" + requestContext.getRequestId() + "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("   -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = handlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }


}
