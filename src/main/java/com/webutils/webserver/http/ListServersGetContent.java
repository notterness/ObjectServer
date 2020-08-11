package com.webutils.webserver.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/*
** The content data for the ListServers call takes the following form:
*
** {
**  "data": [
**   "service-name": "account-mgr-service"
**     {
**        "storage-id": "-1"
**        "server-ip": "localhost/127.0.0.1"
**        "server-port": "5003"
**        "storage-server-read-errors": "0"
**     }
**   ]
** }
 */
public class ListServersGetContent extends ContentParser {

    private static final Logger LOG = LoggerFactory.getLogger(ListServersGetContent.class);

    public ListServersGetContent() {
        super();

        /*
         ** Fill in the list of required attributes so they are easy to check
         */
    }

    /*
     ** This is used to validate that the required fields are present for the ListServer method.
     **   The required fields are:
     **     "object-chunk-number" - The chunks being allocated on various storage servers to provide the redundancy for
     **        the specified chunk for an object.
     **     "storageTier" - The storage tier that this storage server can provide chunks for.
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

        } else {
            LOG.error("Invalid bracketDepth: " + bracketDepth);
            contentValid = false;
        }
        if (!contentValid) {
            clearAllMaps();
        }

        return contentValid;
    }

    /*
     ** Debug method to display the information that has been stored in the various maps from the information in the
     **   Create Bucket POST operation.
     */
    public void dumpMaps() {
        LOG.info("bucketParams");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            LOG.info("    " + entry.getKey() + " : " + entry.getValue());
        }

        LOG.info("freeFormTags");
        for (Map.Entry<String, String> entry : freeformTags.entrySet()) {
            LOG.info("    " + entry.getKey() + " : " + entry.getValue());
        }

        LOG.info("definedTags");
        for (Map.Entry<String, Map<String, String>> entry : definedTags.entrySet()) {
            LOG.info("    sub-category - " + entry.getKey());

            Map<String, String> subCategory = entry.getValue();
            for (Map.Entry<String, String> subCategoryEntry : subCategory.entrySet()) {
                LOG.info("        " + subCategoryEntry.getKey() + " : " + subCategoryEntry.getValue());
            }
        }
    }

}
