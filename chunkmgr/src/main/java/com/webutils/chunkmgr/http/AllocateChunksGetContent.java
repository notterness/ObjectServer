package com.webutils.chunkmgr.http;

import com.webutils.webserver.http.ParseRequestContent;
import com.webutils.webserver.http.StorageTierEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class AllocateChunksGetContent extends ParseRequestContent {
    private static final Logger LOG = LoggerFactory.getLogger(AllocateChunksGetContent.class);

    private static final String OBJECT_CHUNK_NUMBER = "object-chunk-number";


    public AllocateChunksGetContent() {
        super();

        /*
         ** Fill in the list of required attributes so they are easy to check
         */
        requiredAttributes.add(STORAGE_TIER_ATTRIBUTE);
        requiredAttributes.add(OBJECT_CHUNK_NUMBER);
    }

    /*
     ** This is used to validate that the required fields are present for the Create Bucket operation.
     **   The required fields are:
     **     "storage-server-name" - This is the storage server name
     **     "storage-server-ip" - The IP address of the storage server
     **     "storage-server-port" - The TCP Port of the storage server
     **     "allocated-chunks" - The number of chunks that this storage server can have data written to.
     **     "storageTier" - The storage tier that this storage server can provide chunks for.
     **
     ** NOTE: At some point it might be worth validating that there are no unexpected attributes passed in. The other
     **   thing to validate would be the contents of the attributes to make sure garbage data is not provided.
     */
    public boolean validatePostContentData() {
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

    /*
     ** Pull the "object-chunk-number" out and validate that it is a positive integer
     */
    public int getObjectChunkNumber() {
        String chunkNumberStr = params.get(OBJECT_CHUNK_NUMBER);
        int chunkNumber = -1;

        if (chunkNumberStr != null) {
            try {
                chunkNumber = Integer.parseInt(chunkNumberStr);

                /*
                 ** TODO: Determine valid range for port numbers
                 */
                if (chunkNumber < 0) {
                    LOG.warn(OBJECT_CHUNK_NUMBER + " must be a positive integer - " + chunkNumber);
                    chunkNumber = -1;
                }
            } catch (NumberFormatException ex) {
                LOG.warn("Server Port is invalid: " + chunkNumberStr);
            }
        } else {
            LOG.warn(OBJECT_CHUNK_NUMBER + "attribute is missing");
        }
        return chunkNumber;
    }

}
