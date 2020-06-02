package com.webutils.webserver.common;

import com.google.common.io.BaseEncoding;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseMd5ResultHandler extends Md5ResultHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseMd5ResultHandler.class);

    public ResponseMd5ResultHandler(final RequestContext requestContext) {
        super(requestContext, null);

    }

    /*
     ** This extracts the expected MD5 digest from the response headers if it exists and then it validates that
     **   it is the correct length.
     ** Assuming it is found and it is the correct length the header digest string will be returned.
     */
    public String validateMD5Header(final HttpResponseInfo httpResponseInfo) {
        String md5FromResponseHeader = httpResponseInfo.getResponseContentMd5();
        if (md5FromResponseHeader == null) {
            LOG.warn("opc-content-md5 header not provided");
            return null;
        }

        try {
            byte[] bytes = BaseEncoding.base64().decode(md5FromResponseHeader);
            if (bytes.length != 16) {
                LOG.warn("The value of the opc-content-md5 header '" + md5FromResponseHeader +
                        "' was not the correct length after base-64 decoding");
                return null;
            }
        } catch (IllegalArgumentException ia_ex) {
            LOG.warn("The value of the opc-content-md5 header '" + md5FromResponseHeader +
                    "' was not the correct length after base-64 decoding");
            return null;
        }

        return md5FromResponseHeader;
    }

    public boolean checkContentMD5(final String md5DigestFromResponse) {
        if (md5DigestFromResponse == null)
        {
            LOG.warn("checkContentMd5() [" + requestContext.getRequestId() + "] md5DigestFromResponse is null");
            return false;
        }

        if (!md5DigestComplete.get()) {
            LOG.warn("Content-MD5 [" + requestContext.getRequestId() + "] no computed Md5 digest: " + md5FromHeader);
            return false;
        }

        if (!md5DigestFromResponse.equals(computedMd5Digest)) {
            LOG.warn("opc-content-md5 [" + requestContext.getRequestId() + "] did not match computed. expected: " +
                    md5DigestFromResponse + " computed: " + computedMd5Digest);

            return false;
        }

        LOG.info("checkContentMd5() [" + requestContext.getRequestId() + "] passed");
        return true;
    }

}
