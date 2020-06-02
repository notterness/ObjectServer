package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;

public interface Operation {

    /*
     ** This is how long the Operation should wait until if goes back through the
     **   CHECK_SLOW_CHANNEL state if no other activity is taking place.
     */
    long TIME_TILL_NEXT_TIMEOUT_CHECK = 500;

    /*
    ** A simple way to obtain a identifier for the Operation when it is added to a list of
    **   operations that are being used to complete a request.
     */
    OperationTypeEnum getOperationType();

    /*
    ** An accessor method to get at the Request Id that is kept in the RequestContext, but not
    **   accessible from the Compute and Blocking thread pools
     */
    int getRequestId();

    /*
    ** This returns the BufferManagerPointer obtained by this operation, if there is one. If this operation
    **   does not use a BufferManagerPointer, it will return null.
     */
    BufferManagerPointer initialize();

    void event();

    void execute();

    void complete();

    /*
     ** This is used to add the Operation to the event thread's execute queue. The
     **   Operation can be added to the immediate execution queue or the delayed
     **   execution queue.
     **
     ** The following methods are called by the event thread under a queue mutex.
     **   markRemoveFromQueue - This method is used by the event thread to update the queue
     **     the Operation is on when the connection is removed from the queue.
     **   markAddedToQueue - This method is used when an Operation is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **   hasWaitTimeElapsed - Is this Operation ready to run again to check some timeout condition
     **
     ** TODO: Might want to switch to using an enum instead of two different booleans to keep track
     **   of which queue the connection is on. It will probably clean up the code some.
     */
    boolean isOnWorkQueue();
    boolean isOnTimedWaitQueue();
    void markAddedToQueue(final boolean delayedExecutionQueue);
    void markRemovedFromQueue(final boolean delayedExecutionQueue);
    boolean hasWaitTimeElapsed();

    /*
    ** Debug routine to show the dependencies (basically which Operation created other Operations)
     */
    void dumpCreatedOperations(final int level);
}
