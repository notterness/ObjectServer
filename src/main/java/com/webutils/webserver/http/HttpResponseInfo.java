package com.webutils.webserver.http;

import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpResponseInfo extends HttpInfo {

    private static final Logger LOG = LoggerFactory.getLogger(HttpResponseInfo.class);

    private static final String OPC_CONTENT_MD5 = "opc-content-md5";

    public HttpResponseInfo(final RequestContext requestContext) {
        super(requestContext);
    }

    /*
    ** The HTTP response passes back the Md5 Digest in the "opc-content-md5" header instead of
    **   the "Content-MD5" header.
     */
    public String getResponseContentMd5 () {
        String md5 = getHeaderString(OPC_CONTENT_MD5);
        if ((md5 != null) && md5.isEmpty()) {
            md5 = null;
        }

        return md5;
    }


}
