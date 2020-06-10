package com.webutils.webserver.common;

import com.webutils.chunkmgr.common.StorageServerDeleteChunkParams;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientTestDeleteChunkParams extends DeleteChunkParams {

    private static final Logger LOG = LoggerFactory.getLogger(StorageServerDeleteChunkParams.class);

    public ClientTestDeleteChunkParams(final ServerIdentifier server) {

        super(server);
    }

    /*
     ** This displays the results from the StorageServerDeleteChunks method.
     **
     ** TODO: Allow the results to be dumped to a file and possibly allow a format that allows for easier parsing by
     **   the client.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good (StorageServerDeleteChunks returns 200 if successful), then display the following:
         **   opc-client-request-id
         **   opc-request-id
         */
        if (httpInfo.getResponseStatus() == HttpStatus.OK_200) {
            System.out.println("Status: 200");
            String opcClientRequestId = httpInfo.getOpcClientRequestId();
            if (opcClientRequestId != null) {
                System.out.println(HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId);
            }

            String opcRequestId = httpInfo.getOpcRequestId();
            if (opcRequestId != null) {
                System.out.println(HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId);
            }

            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }

        } else if (httpInfo.getResponseStatus() == HttpStatus.METHOD_NOT_ALLOWED_405) {
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String allowableMethods = httpInfo.getAllowableMethods();
            if (allowableMethods != null) {
                System.out.println("Allowed Methods: " + allowableMethods);
            }
        } else {
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

}
