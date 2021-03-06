package com.webutils.webserver.requestcontext;

import com.webutils.webserver.http.ClientHttpRequestInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.EventPollThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

public class ClientContextPool extends RequestContextPool {

    private static final Logger LOG = LoggerFactory.getLogger(ClientContextPool.class);

    private final ServerIdentifierTableMgr serverTableMgr;

    public ClientContextPool(final WebServerFlavor flavor, final MemoryManager memoryManager,
                             final ServerIdentifierTableMgr serverTableMgr) {
        super(flavor, memoryManager, "ClientTest");
        this.serverTableMgr = serverTableMgr;
    }

    /*
     ** This will allocate an ObjectServerRequestContext if this pool has been setup for the running thread.
     */
    public ClientRequestContext allocateContext(final int threadId) {

        ClientRequestContext requestContext;

        EventPollThread threadThisRequestRunsOn = threadRequestRunsOn.get(threadId);

        if (threadThisRequestRunsOn != null) {
            LinkedBlockingQueue<RequestContext> contextList = runningContexts.get(threadId);

            if (contextList != null) {
                ClientHttpRequestInfo httpInfo = new ClientHttpRequestInfo();
                requestContext = new ClientRequestContext(memoryManager, httpInfo, threadThisRequestRunsOn, serverTableMgr,
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
