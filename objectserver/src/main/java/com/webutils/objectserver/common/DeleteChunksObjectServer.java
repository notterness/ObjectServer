package com.webutils.objectserver.common;

import com.webutils.webserver.common.DeleteChunksParams;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/*
** THis is done so that the outputResults can be handled differently between the TestClient and the ObjectServer
 */
public class DeleteChunksObjectServer extends DeleteChunksParams {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteChunksObjectServer.class);

    public DeleteChunksObjectServer(final List<ServerIdentifier> servers) {
        super(servers);
    }

    /*
     ** This displays the results from the DeleteChunks method tht is called by the Object Server.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good (DeleteChunks returns 200 if successful), then display the following:
         **   opc-client-request-id
         **   opc-request-id
         */
        if (httpInfo.getResponseStatus() == HttpStatus.OK_200) {
            LOG.info("Status: 200");
        } else if (httpInfo.getResponseStatus() == HttpStatus.METHOD_NOT_ALLOWED_405) {
            LOG.warn("Status: " + httpInfo.getResponseStatus());
        } else {
            LOG.warn("Status: " + httpInfo.getResponseStatus());
        }
    }

}
