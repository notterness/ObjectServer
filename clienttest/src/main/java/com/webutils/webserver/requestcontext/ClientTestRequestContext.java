package com.webutils.webserver.requestcontext;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.IoInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientTestRequestContext extends RequestContext {

    private static final Logger LOG = LoggerFactory.getLogger(ClientTestRequestContext.class);


    /*
     ** The following Map is used to keep track of when the HTTP Request is sent to the
     **   Object Server from the Client Test and it is used by the test code to know that
     **   the HTTP Request has been sent by the client to the Web Server.
     ** The map is based upon the IP address and the TCP Port of the target plus the chunk number.
     */
    private Map<ServerIdentifier, AtomicBoolean> httpRequestSent;


    ClientTestRequestContext(final MemoryManager memoryManager, final EventPollThread threadThisRunsOn, final DbSetup dbSetup,
                             final int threadId) {
        super(memoryManager, threadThisRunsOn, dbSetup, threadId);

        /*
         ** Setup the map for the HTTP Request Sent
         */
        this.httpRequestSent = new HashMap<>();
    }

    public void initializeServer(final IoInterface connection, final int requestId) {
        super.clientConnection = connection;
        super.connectionRequestId = requestId;
    }

    public void cleanupHttpParser() {
    }

    /*
     ** The following are used to keep track of Object Servers and if the HTTP Request has been sent successfully
     **   to it. The setter (setHttpResponseSent() is called by WriteHeaderToStorageServer after the buffer has been
     **   written to the SocketChannel.
     */
    public boolean hasHttpRequestBeenSent(final ServerIdentifier storageServerId) {
        AtomicBoolean responseSent = httpRequestSent.get(storageServerId);
        if (responseSent != null) {
            return responseSent.get();
        }
        return false;
    }

    public void setHttpRequestSent(final ServerIdentifier storageServerId) {
        AtomicBoolean httpSent = new AtomicBoolean(true);
        httpRequestSent.put(storageServerId, httpSent);
    }

    /*
     ** The following are stubs until I sort out how the RequestContext and RequestContext pool objects should be
     **   properly handled.
     */
    public void removeHttpRequestSent(final ServerIdentifier storageServerId) {
        LOG.error("removeHttpRequestSent() Invalid function");
    }

    public boolean hasStorageServerResponseArrived(final ServerIdentifier storageServerId) {
        LOG.error("hasStorageServerResponseArrived() Invalid function");
        return false;
    }

    public int getStorageResponseResult(final ServerIdentifier storageServerId) {
        LOG.error("getStorageResponseResult() Invalid function");
        return -1;
    }

    public void setStorageServerResponse(final ServerIdentifier storageServerId, final int result) {
        LOG.error("setStorageServerResponse() Invalid function");
    }

    public void removeStorageServerResponse(final ServerIdentifier storageServerId) {
        LOG.error("removeStorageServerResponse() Invalid function");
    }

    public BufferManager getStorageServerWriteBufferManager() {
        LOG.error("getStorageServerWriteBufferManager() Invalid function");
        return null;
    }

}
