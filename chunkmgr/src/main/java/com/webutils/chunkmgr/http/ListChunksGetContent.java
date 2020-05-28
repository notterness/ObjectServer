package com.webutils.chunkmgr.http;

import com.webutils.webserver.http.ParseRequestContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ListChunksGetContent extends ParseRequestContent {

    private static final Logger LOG = LoggerFactory.getLogger(ListChunksGetContent.class);

    public ListChunksGetContent() {
        super();

    }

    /*
     ** This is used to validate that the required fields are present for the AllocateChunks method.
     **   The required fields are:
     **     "object-chunk-number" - The chunks being allocated on various storage servers to provide the redundancy for
     **        the specified chunk for an object.
     **     "storageTier" - The storage tier that this storage server can provide chunks for.
     **
     ** NOTE: At some point it might be worth validating that there are no unexpected attributes passed in. The other
     **   thing to validate would be the contents of the attributes to make sure garbage data is not provided.
     */
    public boolean validateContentData() {
        boolean valid = true;

        /*
         ** First make sure that the bracketDepth is 0 to insure the brackets are properly paired.
         */
        if (bracketDepth == 0) {
            /*
             ** Make sure the required information is present
             */
            for (String attribute : requiredAttributes) {
                if (!params.containsKey(attribute)) {
                    valid = false;

                    LOG.error("Missing required attribute: " + attribute);
                    break;
                }
            }

        } else {
            LOG.error("Invalid bracketDepth: " + bracketDepth);
            valid = false;
        }
        if (!valid) {
            clearAllMaps();
        }

        return valid;
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
