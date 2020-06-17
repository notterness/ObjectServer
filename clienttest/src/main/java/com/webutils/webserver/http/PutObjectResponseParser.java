package com.webutils.webserver.http;

import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PutObjectResponseParser extends ContentParser {

    private static final Logger LOG = LoggerFactory.getLogger(PutObjectResponseParser.class);

    public PutObjectResponseParser() {
        super();

        /*
         ** Fill in the list of required attributes so they are easy to check
         */
        requiredAttributes.add(HttpInfo.OPC_REQUEST_ID);
        requiredAttributes.add(HttpInfo.CONTENT_MD5);
        requiredAttributes.add(HttpResponseInfo.RESPONSE_HEADER_ETAG);
        requiredAttributes.add(HttpResponseInfo.RESPONSE_LAST_MODIFIED);
    }

    /*
     ** This is used to validate that the required fields are present for the PutObject method.
     **   The required fields are:
     **     "opc-request-id"
     **     "last-modified"
     **     "etag"
     **     "Content-Md5"
     **
     ** NOTE: At some point it might be worth validating that there are no unexpected attributes passed in. The other
     **   thing to validate would be the contents of the attributes to make sure garbage data is not provided.
     */
    public boolean validateContentData() {
        contentValid = true;

        /*
         ** First make sure that the bracketDepth is 0 to insure the brackets are properly paired.
         */
        if (bracketDepth == 0) {
            /*
             ** Make sure the required information is present
             */
            for (String attribute : requiredAttributes) {
                if (!params.containsKey(attribute)) {
                    contentValid = false;

                    LOG.error("Missing required attribute: " + attribute);
                    break;
                }
            }

            if (getStorageTier() == StorageTierEnum.INVALID_TIER) {
                contentValid = false;
            }
        } else {
            LOG.error("Invalid bracketDepth: " + bracketDepth);
            contentValid = false;
        }
        if (!contentValid) {
            clearAllMaps();
        }

        return contentValid;
    }


}
