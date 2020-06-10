package com.webutils.chunkmgr.common;

import com.webutils.webserver.common.DeleteChunkParams;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageServerDeleteChunkParams extends DeleteChunkParams {

    private static final Logger LOG = LoggerFactory.getLogger(StorageServerDeleteChunkParams.class);

    public StorageServerDeleteChunkParams(final ServerIdentifier server) {

        super(server);
    }

    /*
     ** This displays the results from the StorageServerDeleteChunk method.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
    }

}
