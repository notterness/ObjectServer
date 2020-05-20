package com.webutils.objectserver.operations;

import com.webutils.objectserver.requestcontext.ObjectServerRequestContext;
import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.Sha256ResultHandler;
import com.webutils.webserver.http.CreateBucketPostContent;
import com.webutils.webserver.operations.ComputeSha256Digest;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.operations.ParsePostContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class SetupCreateBucketPost implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(SetupCreateBucketPost.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.SETUP_CREATE_BUCKET_POST;

    private final ObjectServerRequestContext requestContext;

    private final Operation metering;

    private final Operation completeCallback;

    private final CreateBucketPostContent createBucketPostContent;

    private CreateBucket createBucket;

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
    private final Map<OperationTypeEnum, Operation> PostHandlerOperations;

    private boolean setupMethodDone;
    private boolean waitingOnOperations;

    /*
     ** This is used to setup the initial Operation dependencies required to handle the CreateBucket POST
     **   method.
     ** The completeCb will call the DetermineRequest operation's event() method when the POST completes.
     */
    public SetupCreateBucketPost(final ObjectServerRequestContext requestContext, final Operation metering,
                                 final Operation completeCb) {

        this.requestContext = requestContext;
        this.metering = metering;
        this.completeCallback = completeCb;

        this.createBucketPostContent = new CreateBucketPostContent();

        this.updator = requestContext.getSha256ResultHandler();

        /*
         ** Setup the list of Operations currently used to handle the V2 PUT
         */
        PostHandlerOperations = new HashMap<>();

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
            ** Once the parsing of the POST content data has taken place and the Sha-256 digest is determined to be valid,
            **   then the Bucket and its contents need to be created in the database.
             */
            createBucket = new CreateBucket(requestContext, createBucketPostContent, this);
            PostHandlerOperations.put(createBucket.getOperationType(), createBucket);
            createBucket.initialize();

            /*
            ** Setup the Parser to pull the information out of the POST content. This will put the information into
            **   a temporary structure and once the Sha-256 digest completes (assuming it is successful) the setup
            **   of the Bucket will take place.
             */
            ParsePostContent parseContent = new ParsePostContent(requestContext, readBufferPointer, metering,
                    createBucketPostContent, this);
            PostHandlerOperations.put(parseContent.getOperationType(), parseContent);
            parseContent.initialize();

            /*
             ** The ComputeSha256Digest needs to be completed before the SendFinalStatus operation can be woken up
             **   to perform its work.
             **   The SendFinalStatus is dependent upon all the content data being processed and the Sha-256 Digest
             **   having completed.
             */
            List<Operation> callbackList = new LinkedList<>();
            callbackList.add(this);

            ComputeSha256Digest computeSha256Digest = new ComputeSha256Digest(requestContext, callbackList, readBufferPointer, updator);
            PostHandlerOperations.put(computeSha256Digest.getOperationType(), computeSha256Digest);
            computeSha256Digest.initialize();

            /*
             ** Dole out another buffer to read in the content data if there is not data remaining in
             **   the buffer from the HTTP Parsing.
             */
            BufferManager clientReadBufferManager = requestContext.getClientReadBufferManager();
            ByteBuffer remainingBuffer = clientReadBufferManager.peek(readBufferPointer);
            if (remainingBuffer != null) {
                if (remainingBuffer.remaining() > 0) {
                    parseContent.event();
                } else {
                    metering.event();
                }
            }

            setupMethodDone = true;
        } else if (waitingOnOperations){
            /*
             ** This will be placed on the execute queue twice, once by the ParsePostContent operation when the
             **   parsing is complete and a second time when the ComputeSha256Digest has completed.
             */
            if (updator.getSha256DigestComplete() && requestContext.postMethodContentParsed()) {
                LOG.info("SetupCreateBucketPost[" + requestContext.getRequestId() + "] Sha-256 and Parsing done");

                /*
                ** Cleanup the operations
                 */
                Operation contentParser = PostHandlerOperations.get(OperationTypeEnum.PARSE_POST_CONTENT);
                contentParser.complete();
                PostHandlerOperations.remove(OperationTypeEnum.PARSE_POST_CONTENT);

                /*
                ** Since the Sha-256 digest runs on a compute thread, it handles it's own complete() call. For that
                **   reason, simply remove it from the PostHandlerOperations map.
                 */
                PostHandlerOperations.remove(OperationTypeEnum.COMPUTE_SHA256_DIGEST);

                if (updator.checkContentSha256()) {
                    createBucket.event();
                } else {
                    /*
                    ** There was an error with the passed in or computed Sha-256 digest, so an error needs to be
                    **   returned to the client
                     */
                    complete();
                }

                waitingOnOperations = false;
            } else {
                LOG.info("SetupCreateBucketPost[" + requestContext.getRequestId() + "] not completed Sha-256 digestComplete: " +
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
        Collection<Operation> createdOperations = PostHandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        PostHandlerOperations.clear();

        completeCallback.event();

        LOG.info("SetupCreateBucketPost[" + requestContext.getRequestId() + "] completed");
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
        //LOG.info("SetupCreateBucketPost[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SetupCreateBucketPost[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SetupCreateBucketPost[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SetupCreateBucketPost[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SetupCreateBucketPost[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("   -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = PostHandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }

}
