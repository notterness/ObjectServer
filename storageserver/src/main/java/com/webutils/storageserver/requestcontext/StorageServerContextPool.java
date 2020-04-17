package com.webutils.storageserver.requestcontext;

import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.RequestContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

public class StorageServerContextPool extends RequestContextPool {

    private static final Logger LOG = LoggerFactory.getLogger(StorageServerContextPool.class);

    private final DbSetup dbSetup;

    public StorageServerContextPool(final WebServerFlavor flavor, final MemoryManager memoryManager, final DbSetup dbSetup) {
        super(flavor, memoryManager, "StorageServer");
        this.dbSetup = dbSetup;
    }

    /*
     ** This will allocate an ObjectServerRequestContext if this pool has been setup for the running thread.
     */
    public RequestContext allocateContext(final int threadId) {

        StorageServerRequestContext requestContext;

        EventPollThread threadThisRequestRunsOn = threadRequestRunsOn.get(threadId);

        if (threadThisRequestRunsOn != null) {
            LinkedBlockingQueue<RequestContext> contextList = runningContexts.get(threadId);

            if (contextList != null) {
                requestContext = new StorageServerRequestContext(memoryManager, threadThisRequestRunsOn, dbSetup,
                        threadId, flavor);

                if (contextList.offer(requestContext)) {
                    LOG.info("allocateContext(StorageServer) [" + threadId + "] webServerFlavor: " + flavor.toString());
                } else {
                    LOG.error("ERROR - allocateContext(StorageServer) Unable to offer to Queue threadId: " + threadId);
                    requestContext = null;
                }
            } else {
                LOG.error("allocateContext(StorageServer) [" + threadId + "] webServerFlavor: " + flavor.toString() + "contextList not found");

                requestContext = null;
            }
        } else {
            LOG.error("allocateContext(StorageServer) [" + threadId + "] webServerFlavor: " + flavor.toString() + "thread not found");

            requestContext = null;
        }

        return requestContext;
    }

}
