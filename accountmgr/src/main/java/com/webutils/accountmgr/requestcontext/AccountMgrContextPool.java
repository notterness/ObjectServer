package com.webutils.accountmgr.requestcontext;

import com.webutils.accountmgr.http.AccountMgrHttpRequestInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.RequestContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountMgrContextPool extends RequestContextPool {

    private static final Logger LOG = LoggerFactory.getLogger(AccountMgrContextPool.class);

    private final ServerIdentifierTableMgr serverTableMgr;

    public AccountMgrContextPool(final WebServerFlavor flavor, final MemoryManager memoryManager,
                                 final ServerIdentifierTableMgr serverTableMgr) {
        super(flavor, memoryManager, "AccountMgr");
        this.serverTableMgr = serverTableMgr;
    }

    /*
     ** This will allocate an ObjectServerRequestContext if this pool has been setup for the running thread.
     */
    public RequestContext allocateContext(final int threadId) {

        AccountMgrRequestContext requestContext;

        EventPollThread threadThisRequestRunsOn = threadRequestRunsOn.get(threadId);

        if (threadThisRequestRunsOn != null) {
            LinkedBlockingQueue<RequestContext> contextList = runningContexts.get(threadId);

            if (contextList != null) {
                AccountMgrHttpRequestInfo httpInfo = new AccountMgrHttpRequestInfo();
                requestContext = new AccountMgrRequestContext(memoryManager, httpInfo, threadThisRequestRunsOn, serverTableMgr,
                        threadId, flavor);

                if (contextList.offer(requestContext)) {
                    LOG.info("allocateContext(AccountMgr) [" + threadId + "] webServerFlavor: " + flavor.toString());
                } else {
                    LOG.error("ERROR - allocateContext(AccountMgr) Unable to offer to Queue threadId: " + threadId);
                    requestContext = null;
                }
            } else {
                LOG.error("allocateContext(AccountServer) [" + threadId + "] webServerFlavor: " + flavor.toString() + " threadId: " +
                        threadId + " contextList not found");

                requestContext = null;
            }
        } else {
            LOG.error("allocateContext(AccountServer) [" + threadId + "] webServerFlavor: " + flavor.toString() + " thread not found");

            requestContext = null;
        }

        return requestContext;
    }

    public void stop(final int threadId) {
        super.stop(threadId);
    }

}

