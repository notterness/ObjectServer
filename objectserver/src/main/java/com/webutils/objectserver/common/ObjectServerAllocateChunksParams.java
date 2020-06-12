package com.webutils.objectserver.common;

import com.webutils.webserver.common.AllocateChunksParams;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.http.StorageTierEnum;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectServerAllocateChunksParams extends AllocateChunksParams {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectServerAllocateChunksParams.class);

    public ObjectServerAllocateChunksParams(final StorageTierEnum tier, final int objectChunkNumber) {

        super(tier, objectChunkNumber);
    }

    /*
     ** This displays the results from the AllocateChunks method.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good (Allocate Chunks returns 200 if successful), then output a log statement:
         */
        int status = httpInfo.getResponseStatus();
        if (status == HttpStatus.OK_200) {
            LOG.info("ObjectServerAllocateChunksParams outputResults() OK_200");
        } else if (httpInfo.getResponseStatus() == HttpStatus.METHOD_NOT_ALLOWED_405) {
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String allowableMethods = httpInfo.getAllowableMethods();
            if (allowableMethods != null) {
                System.out.println("Allowed Methods: " + allowableMethods);
            }
        } else {
            LOG.info("ObjectServerAllocateChunksParams outputResults() " + status);
        }
    }

}
