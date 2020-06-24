package com.webutils.chunkmgr.http;

import com.webutils.webserver.http.HttpMethodEnum;
import com.webutils.webserver.http.HttpRequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkMgrHttpRequestInfo extends HttpRequestInfo {

    private static final Logger LOG = LoggerFactory.getLogger(ChunkMgrHttpRequestInfo.class);

    private static final String LIST_SERVERS_METHOD = "servers";
    private static final String LIST_CHUNKS_METHOD = "chunks";

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
                String listType = getListType();
                if (isHealthCheck()) {
                    /*
                     ** This is the Health Check request meaning the URI had "/health" in it.
                     */
                    LOG.info("Setting httpMethod to HEALTH_CHECK");
                    httpMethod = HttpMethodEnum.HEALTH_CHECK;
                } else if (listType != null) {
                    if (listType.equals(LIST_CHUNKS_METHOD)) {
                        httpMethod = HttpMethodEnum.LIST_CHUNKS_METHOD;
                    } else if (listType.equals(LIST_SERVERS_METHOD)) {
                        httpMethod = HttpMethodEnum.LIST_SERVERS_METHOD;
                    }
                }
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
