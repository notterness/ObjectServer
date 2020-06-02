package com.webutils.objectserver.requestcontext;

import com.webutils.objectserver.http.ObjectServerHttpRequestInfo;
import com.webutils.webserver.buffermgr.ChunkMemoryPool;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.RequestContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

public class ObjectServerContextPool extends RequestContextPool {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectServerContextPool.class);

    private final ServerIdentifierTableMgr serverTableMgr;

    private final ChunkMemoryPool chunkMemPool;

    public ObjectServerContextPool(final WebServerFlavor flavor, final MemoryManager memoryManager,
                                   final ServerIdentifierTableMgr serverTableMgr) {
        super(flavor, memoryManager, "ObjectServer");
        this.serverTableMgr = serverTableMgr;

        this.chunkMemPool = new ChunkMemoryPool(memoryManager, RequestContext.getChunkBufferCount());
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
                ObjectServerHttpRequestInfo httpInfo = new ObjectServerHttpRequestInfo();
                requestContext = new ObjectServerRequestContext(memoryManager, httpInfo, threadThisRequestRunsOn, serverTableMgr,
                        chunkMemPool, threadId, flavor);

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

        ObjectServerHttpRequestInfo httpInfo = new ObjectServerHttpRequestInfo();
        ObjectServerRequestContext requestContext = new ObjectServerRequestContext(memoryManager, httpInfo,
                null, serverTableMgr, chunkMemPool, threadId, flavor);

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

    public void stop(final int threadId) {
        chunkMemPool.reset();

        super.stop(threadId);
    }
}
