package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.connectionstate.CasperHttpInfo;
import com.oracle.athena.webserver.connectionstate.HttpMethodEnum;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DetermineRequestType implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(DetermineRequestType.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.DETERMINE_REQUEST_TYPE;

    private final RequestContext requestContext;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    /*
     ** The following map is passed into the RequestContext and it provides a list of all of the Operations that
     **   provide the initial handler for an HTTP Request type. This is setup at the start of execution and
     **   is only populated with handler operations (i.e. V2 PUT).
     */
    private final Map<HttpMethodEnum, Operation> supportedHttpRequests;

    private CasperHttpInfo casperHttpInfo;


    public DetermineRequestType(final RequestContext requestContext, final Map<HttpMethodEnum, Operation> supportedHttpRequests) {

        this.requestContext = requestContext;
        this.supportedHttpRequests = supportedHttpRequests;

        this.casperHttpInfo = this.requestContext.getHttpInfo();

        /*
         ** This starts out not being on any queue
         */
        onDelayedQueue = false;
        onExecutionQueue = false;
        nextExecuteTime = 0;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    /*
     ** This returns the BufferManagerPointer obtained by this operation, if there is one. If this operation
     **   does not use a BufferManagerPointer, it will return null.
     */
    public BufferManagerPointer initialize() {
        return null;
    }

    public void event() {

    }

    public void execute() {
        if (requestContext.getHttpParseError()) {
            /*
            ** Event the send client response operation here so that the final status is sent
             */
        } else {

            /*
             ** Now, based on the HTTP method, figure out the Operation to event that will setup the sequences for the
             **   handling of the request.
             */
            HttpMethodEnum method = casperHttpInfo.getMethod();
            Operation httpRequestSetup = supportedHttpRequests.get(method);
            if (httpRequestSetup != null) {
                LOG.info("DetermineRequestType[" + requestContext.getRequestId() + "] execute() " + method.toString());

                httpRequestSetup.event();
            } else {
                LOG.info("DetermineRequestType[" + requestContext.getRequestId() + "] execute() unsupported request " + method.toString());
            }
        }
    }

    public void complete() {
        /*
        ** This does not have anything that needs to be released or cleaned up, so this is just an
        **   empty method for now.
         */

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
     ** TODO: Might want to switch to using an enum instead of two different booleans to keep track
     **   of which queue the connection is on. It will probably clean up the code some.
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("DetermineRequestType[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (onDelayedQueue) {
            if (!delayedExecutionQueue) {
                LOG.warn("DetermineRequestType[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on delayed queue");
            }

            onDelayedQueue = false;
            nextExecuteTime = 0;
        } else if (onExecutionQueue){
            if (delayedExecutionQueue) {
                LOG.warn("DetermineRequestType[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on workQueue");
            }

            onExecutionQueue = false;
        } else {
            LOG.warn("DetermineRequestType[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            nextExecuteTime = System.currentTimeMillis() + TIME_TILL_NEXT_TIMEOUT_CHECK;
            onDelayedQueue = true;
        } else {
            onExecutionQueue = true;
        }
    }

    public boolean isOnWorkQueue() {
        return onExecutionQueue;
    }

    public boolean isOnTimedWaitQueue() {
        return onDelayedQueue;
    }

    public boolean hasWaitTimeElapsed() {
        long currTime = System.currentTimeMillis();

        if (currTime < nextExecuteTime) {
            return false;
        }

        //LOG.info("DetermineRequestType[" + requestContext.getRequestId() + "] waitTimeElapsed " + currTime);
        return true;
    }

}
