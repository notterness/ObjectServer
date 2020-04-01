package com.webutils.webserver.requestcontext;

import com.webutils.webserver.niosockets.EventPollThread;

public interface RequestContextPool {

    void setThreadAndBaseId(final EventPollThread threadThisRunsOn, final int threadBaseId);

    RequestContext allocateContext(final int threadId);

    void releaseContext(RequestContext requestContext);

    void executeRequestContext(final int threadId);

    /*
    ** Used to release any resources back to the free pool and to insure that the MemoryManager does not have
    **   outstanding allocations.
     */
    void stop(final int threadId);

}
