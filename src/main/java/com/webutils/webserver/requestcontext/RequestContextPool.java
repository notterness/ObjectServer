package com.webutils.webserver.requestcontext;

import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.EventPollThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class RequestContextPool {

    private static final Logger LOG = LoggerFactory.getLogger(RequestContextPool.class);

    protected final WebServerFlavor flavor;
    protected final MemoryManager memoryManager;
    private final String requestOwner;

    protected final Map<Integer, LinkedBlockingQueue<RequestContext>> runningContexts;
    protected final Map<Integer, EventPollThread> threadRequestRunsOn;


    public RequestContextPool(final WebServerFlavor flavor, final MemoryManager memoryManager, final String owner) {
        this.flavor = flavor;
        this.memoryManager = memoryManager;
        this.requestOwner = owner;

        this.runningContexts = new HashMap<>();
        this.threadRequestRunsOn = new HashMap<>();
    }


    public void setThreadAndBaseId(final EventPollThread threadThisRunsOn, final int threadBaseId) {
        threadRequestRunsOn.put(threadBaseId, threadThisRunsOn);

        LinkedBlockingQueue<RequestContext> contextList = new LinkedBlockingQueue<>();
        runningContexts.put(threadBaseId, contextList);
    }

    abstract public RequestContext allocateContext(final int threadId);

    public void releaseContext(final RequestContext requestContext) {
        int threadId = requestContext.getThreadId();

        LinkedBlockingQueue<RequestContext> contextList = runningContexts.get(threadId);
        if (contextList != null) {
            /*
             ** The inspection code complains about this due to the contextList using <ClientTestRequestContext> but the removal is
             **   for <RequestContext>.
             */
            if (contextList.remove(requestContext)) {
                LOG.info("releaseContext(" + requestOwner + ") threadId: " + threadId);
            } else {
                LOG.error("releaseContext(" + requestOwner + ") [" + threadId + "] webServerFlavor: " + flavor.toString() + " requestContext not found");
            }
        } else {
            LOG.error("releaseContext(" + requestOwner + ") [" + threadId + "] webServerFlavor: " + flavor.toString() + " contextList not found");
        }

    }

    /*
     ** Run all the work that has been queued up
     */
    public void executeRequestContext(final int threadId) {

        LinkedBlockingQueue<RequestContext> contextList = runningContexts.get(threadId);
        if (contextList != null) {
            /*
             ** Now check if there is other work to be performed on the connections that does not deal with the
             **   SocketChanel read and write operations
             */
            for (RequestContext runningContext : contextList) {
                runningContext.performOperationWork();
            }
        }
    }

    /*
    ** Used to release any resources back to the free pool and to insure that the MemoryManager does not have
    **   outstanding allocations.
     */
    public void stop(final int threadId) {

        String memoryPoolOwner = requestOwner + "-" + threadId;

        LinkedBlockingQueue<RequestContext> contextList = runningContexts.get(threadId);
        if (contextList != null) {
            /*
             ** Now check if there is other work to be performed on the connections that does not deal with the
             **   SocketChanel read and write operations
             */
            for (RequestContext runningContext : contextList) {
                runningContext.dumpOperations();
            }

            contextList.clear();

            runningContexts.remove(threadId);
        }

        threadRequestRunsOn.remove(threadId);

        if (threadRequestRunsOn.isEmpty() && runningContexts.isEmpty()) {
            memoryManager.verifyMemoryPools(memoryPoolOwner);
        }
    }

}
