package com.webutils.chunkmgr.http;

import com.webutils.webserver.http.HttpRequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkMgrHttpRequestInfo extends HttpRequestInfo {

    private static final Logger LOG = LoggerFactory.getLogger(ChunkMgrHttpRequestInfo.class);

    public ChunkMgrHttpRequestInfo() {
        super();
    }

    /*
     ** When the headers have been completely read in, that will be the time to insure it is valid
     **   and the field values make sense.
     */
    public void setHeaderComplete() {

        switch (httpMethod) {
            case PUT_METHOD:
                break;

            case POST_METHOD:
                break;

            case GET_METHOD:
                break;

            case DELETE_METHOD:
                break;

            default:
                LOG.warn("Illegal method: " + httpMethod.toString());
                break;
        }

        headerComplete = true;
    }

}
