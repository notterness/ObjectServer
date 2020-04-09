package com.webutils.objectserver.requestcontext;

import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.RequestContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class ObjectServerContextPool implements RequestContextPool {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectServerContextPool.class);

    private final WebServerFlavor flavor;
    private final DbSetup dbSetup;

    private MemoryManager memoryManager;


    private final Map<Integer, LinkedList<ObjectServerRequestContext>> runningContexts;
    private final Map<Integer, EventPollThread> threadRequestRunsOn;


    public ObjectServerContextPool(final WebServerFlavor flavor, final MemoryManager memoryManager, final DbSetup dbSetup) {
        this.flavor = flavor;
        this.memoryManager = memoryManager;
        this.dbSetup = dbSetup;

        this.runningContexts = new HashMap<>();
        this.threadRequestRunsOn = new HashMap<>();
    }

    public void setThreadAndBaseId(final EventPollThread threadThisRunsOn, final int threadBaseId) {
        threadRequestRunsOn.put(threadBaseId, threadThisRunsOn);

        LinkedList<ObjectServerRequestContext> contextList = new LinkedList<>();
        runningContexts.put(threadBaseId, contextList);
    }

    /*
    ** This will allocate an ObjectServerRequestContext if this pool has been setup for the running thread.
     */
    public RequestContext allocateContext(final int threadId) {

        ObjectServerRequestContext requestContext;

        EventPollThread threadThisRequestRunsOn = threadRequestRunsOn.get(threadId);

        if (threadThisRequestRunsOn != null) {
            LinkedList<ObjectServerRequestContext> contextList = runningContexts.get(threadId);

            if (contextList != null) {
                requestContext = new ObjectServerRequestContext(memoryManager, threadThisRequestRunsOn, dbSetup, threadId);

                contextList.add(requestContext);
                LOG.info("allocateContext(ObjectServer) [" + threadId + "] webServerFlavor: " + flavor.toString());
            } else {
                LOG.error("allocateContext(ObjectServer) [" + threadId + "] webServerFlavor: " + flavor.toString() + " threadId: " +
                                threadId + " contextList not found");

                requestContext = null;
            }
        } else {
            LOG.error("allocateContext(ObjectServer) [" + threadId + "] webServerFlavor: " + flavor.toString() + " thread not found");

            requestContext = null;
        }

        return requestContext;
    }

    /*
     ** This will allocate an ObjectServerRequestContext and is used for the test cases where there is not an
     **   EventPollThread used to run the test case. These test cases are when testing is done on a particular
     **   operation or sequence of operations.
     */
    public RequestContext allocateContextNoCheck(final int threadId) {

        ObjectServerRequestContext requestContext;

        requestContext = new ObjectServerRequestContext(memoryManager, null, dbSetup, threadId);

        LinkedList<ObjectServerRequestContext> contextList = runningContexts.get(threadId);
        if (contextList != null) {
            LOG.info("allocateContextNoCheck(ObjectServer) [" + threadId + "] webServerFlavor: " + flavor.toString());
        } else {
            LOG.info("allocateContextNoCheck(ObjectServer) [" + threadId + "] webServerFlavor: " + flavor.toString() + " contextList not found");
            contextList = new LinkedList<>();
            runningContexts.put(threadId, contextList);
        }

        contextList.add(requestContext);

        return requestContext;
    }

    public void releaseContext(final RequestContext requestContext) {
        int threadId = requestContext.getThreadId();

        LinkedList<ObjectServerRequestContext> contextList = runningContexts.get(threadId);
        if (contextList != null) {
            if (!contextList.remove(requestContext)) {
                LOG.error("releaseContext(ObjectServer) [" + threadId + "] webServerFlavor: " + flavor.toString() + " requestContext not found");
            }
        } else {
            LOG.error("releaseContext(ObjectServer) [" + threadId + "] webServerFlavor: " + flavor.toString() + " contextList not found");
        }
    }


    /*
    ** Run all the work that has been queued up
     */
    public void executeRequestContext(final int threadId) {

        LinkedList<ObjectServerRequestContext> contextList = runningContexts.get(threadId);
        if (contextList != null) {
            /*
             ** Now check if there is other work to be performed on the connections that does not deal with the
             **   SocketChanel read and write operations
             */
            for (ObjectServerRequestContext runningContext : contextList) {
                runningContext.performOperationWork();
            }
        }
    }

    public void stop(final int threadId) {

        LOG.info("ObjectServerContextPool stop()");

        String memoryPoolOwner = "ObjectServerContextPool";

        LinkedList<ObjectServerRequestContext> contextList = runningContexts.get(threadId);
        if (contextList != null) {
            /*
             ** Now check if there is other work to be performed on the connections that does not deal with the
             **   SocketChanel read and write operations
             */
            for (ObjectServerRequestContext runningContext : contextList) {
                runningContext.dumpOperations();
            }

            contextList.clear();

            runningContexts.remove(threadId);
        }

        threadRequestRunsOn.remove(threadId);

        if (threadRequestRunsOn.isEmpty() && runningContexts.isEmpty()) {
            LOG.info("ObjectServerContextPool stop() memory pool verify");

            memoryManager.verifyMemoryPools(memoryPoolOwner);
            memoryManager = null;
        }
    }
}
