package com.webutils.objectserver.requestcontext;

import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.RequestContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

public class ObjectServerContextPool extends RequestContextPool {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectServerContextPool.class);

    private final DbSetup dbSetup;

    public ObjectServerContextPool(final WebServerFlavor flavor, final MemoryManager memoryManager, final DbSetup dbSetup) {
        super(flavor, memoryManager, "ObjectServer");
        this.dbSetup = dbSetup;
    }

    /*
    ** This will allocate an ObjectServerRequestContext if this pool has been setup for the running thread.
     */
    public RequestContext allocateContext(final int threadId) {

        ObjectServerRequestContext requestContext;

        EventPollThread threadThisRequestRunsOn = threadRequestRunsOn.get(threadId);

        if (threadThisRequestRunsOn != null) {
            LinkedBlockingQueue<RequestContext> contextList = runningContexts.get(threadId);

            if (contextList != null) {
                requestContext = new ObjectServerRequestContext(memoryManager, threadThisRequestRunsOn, dbSetup,
                        threadId, flavor);

                if (contextList.offer(requestContext)) {
                    LOG.info("allocateContext(ObjectServer) [" + threadId + "] webServerFlavor: " + flavor.toString());
                } else {
                    LOG.error("ERROR - allocateContext(ObjectServer) Unable to offer to Queue threadId: " + threadId);
                    requestContext = null;
                }
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

        requestContext = new ObjectServerRequestContext(memoryManager, null, dbSetup, threadId, flavor);

        LinkedBlockingQueue<RequestContext> contextList = runningContexts.get(threadId);
        if (contextList != null) {
            LOG.info("allocateContextNoCheck(ObjectServer) [" + threadId + "] webServerFlavor: " + flavor.toString());
        } else {
            LOG.info("allocateContextNoCheck(ObjectServer) [" + threadId + "] webServerFlavor: " + flavor.toString() + " contextList not found");
            contextList = new LinkedBlockingQueue<>();
            runningContexts.put(threadId, contextList);
        }

        contextList.add(requestContext);

        return requestContext;
    }

}
