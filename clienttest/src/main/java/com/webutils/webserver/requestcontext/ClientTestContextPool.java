package com.webutils.webserver.requestcontext;

import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.niosockets.EventPollThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class ClientTestContextPool implements RequestContextPool {

    private static final Logger LOG = LoggerFactory.getLogger(ClientTestContextPool.class);

    private final WebServerFlavor flavor;
    private final DbSetup dbSetup;

    private MemoryManager memoryManager;


    private final Map<Integer, LinkedList<ClientTestRequestContext>> runningContexts;
    private final Map<Integer, EventPollThread> threadRequestRunsOn;


    public ClientTestContextPool(final WebServerFlavor flavor, final MemoryManager memoryManager, final DbSetup dbSetup) {
        this.flavor = flavor;
        this.memoryManager = memoryManager;
        this.dbSetup = dbSetup;

        this.runningContexts = new HashMap<>();
        this.threadRequestRunsOn = new HashMap<>();
    }

    public void setThreadAndBaseId(final EventPollThread threadThisRunsOn, final int threadBaseId) {
        threadRequestRunsOn.put(threadBaseId, threadThisRunsOn);

        LinkedList<ClientTestRequestContext> contextList = new LinkedList<>();
        runningContexts.put(threadBaseId, contextList);
    }

    /*
     ** This will allocate an ObjectServerRequestContext if this pool has been setup for the running thread.
     */
    public RequestContext allocateContext(final int threadId) {

        ClientTestRequestContext requestContext;

        EventPollThread threadThisRequestRunsOn = threadRequestRunsOn.get(threadId);

        if (threadThisRequestRunsOn != null) {
            LinkedList<ClientTestRequestContext> contextList = runningContexts.get(threadId);

            if (contextList != null) {
                requestContext = new ClientTestRequestContext(flavor, memoryManager, threadThisRequestRunsOn, dbSetup);

                contextList.add(requestContext);
                LOG.info("allocateContext [" + threadId + "] webServerFlavor: " + flavor.toString());
            } else {
                LOG.error("allocateContext [" + threadId + "] webServerFlavor: " + flavor.toString() + "contextList not found");

                requestContext = null;
            }
        } else {
            LOG.error("allocateContext [" + threadId + "] webServerFlavor: " + flavor.toString() + "thread not found");

            requestContext = null;
        }

        return requestContext;
    }

    public void releaseContext(final RequestContext requestContext) {
        runningContexts.remove(requestContext);
    }


    /*
     ** Run all the work that has been queued up
     */
    public void executeRequestContext(final int threadId) {

        LinkedList<ClientTestRequestContext> contextList = runningContexts.get(threadId);
        if (contextList != null) {
            /*
             ** Now check if there is other work to be performed on the connections that does not deal with the
             **   SocketChanel read and write operations
             */
            for (ClientTestRequestContext runningContext : contextList) {
                runningContext.performOperationWork();
            }
        }
    }

    public void stop(final int threadId) {

        String memoryPoolOwner = "ObjectServerContextPool";

        LinkedList<ClientTestRequestContext> contextList = runningContexts.get(threadId);
        if (contextList != null) {
            /*
             ** Now check if there is other work to be performed on the connections that does not deal with the
             **   SocketChanel read and write operations
             */
            for (ClientTestRequestContext runningContext : contextList) {
                runningContext.dumpOperations();
            }

            contextList.clear();

            runningContexts.remove(threadId);
        }

        threadRequestRunsOn.remove(threadId);

        if (threadRequestRunsOn.isEmpty() && runningContexts.isEmpty()) {
            memoryManager.verifyMemoryPools(memoryPoolOwner);
            memoryManager = null;
        }
    }
}
