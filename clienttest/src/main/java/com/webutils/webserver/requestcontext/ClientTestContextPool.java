package com.webutils.webserver.requestcontext;

import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.niosockets.EventPollThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

public class ClientTestContextPool extends RequestContextPool {

    private static final Logger LOG = LoggerFactory.getLogger(ClientTestContextPool.class);

    private final DbSetup dbSetup;

    public ClientTestContextPool(final WebServerFlavor flavor, final MemoryManager memoryManager, final DbSetup dbSetup) {
        super(flavor, memoryManager, "ClientTest");
        this.dbSetup = dbSetup;
    }

    /*
     ** This will allocate an ObjectServerRequestContext if this pool has been setup for the running thread.
     */
    public RequestContext allocateContext(final int threadId) {

        ClientTestRequestContext requestContext;

        EventPollThread threadThisRequestRunsOn = threadRequestRunsOn.get(threadId);

        if (threadThisRequestRunsOn != null) {
            LinkedBlockingQueue<RequestContext> contextList = runningContexts.get(threadId);

            if (contextList != null) {
                requestContext = new ClientTestRequestContext(memoryManager, threadThisRequestRunsOn, dbSetup,
                        threadId, flavor);

                if (contextList.offer(requestContext)) {
                    LOG.info("allocateContext(ClientTest) [" + threadId + "]");
                } else {
                    LOG.error("ERROR - allocateContext(ClientTest) Unable to offer to Queue threadId: " + threadId);
                    requestContext = null;
                }
            } else {
                LOG.error("allocateContext(ClientTest) [" + threadId + "] webServerFlavor: " + flavor.toString() + "contextList not found");

                requestContext = null;
            }
        } else {
            LOG.error("allocateContext(ClientTest) [" + threadId + "] webServerFlavor: " + flavor.toString() + "thread not found");

            requestContext = null;
        }

        return requestContext;
    }

}
