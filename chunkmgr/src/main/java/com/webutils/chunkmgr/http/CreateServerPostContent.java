package com.webutils.chunkmgr.http;

import com.webutils.webserver.http.ContentParser;
import com.webutils.webserver.http.StorageTierEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CreateServerPostContent extends ContentParser {

    private static final Logger LOG = LoggerFactory.getLogger(CreateServerPostContent.class);

    private static final String STORAGE_SERVER_IP = "storage-server-ip";
    private static final String STORAGE_SERVER_PORT = "storage-server-port";
    private static final String STORAGE_SERVER_NAME = "storage-server-name";
    private static final String STORAGE_SERVER_NUM_CHUNKS = "allocated-chunks";


    public CreateServerPostContent() {
        super();

        /*
         ** Fill in the list of required attributes so they are easy to check
         */
        requiredAttributes.add(STORAGE_SERVER_NAME);
        requiredAttributes.add(STORAGE_SERVER_IP);
        requiredAttributes.add(STORAGE_SERVER_PORT);
        requiredAttributes.add(STORAGE_SERVER_NUM_CHUNKS);
        requiredAttributes.add(STORAGE_TIER_ATTRIBUTE);
    }

    /*
     ** This is used to validate that the required fields are present for the CreateServer method.
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

            /*
            ** Validate that the Port, Number of Chunks are integers
             */
            if (getServerPort() == -1) {
                contentValid = false;
            }

            /*
            ** For Storage Servers, validate that the "allocated-chunks" is a positive value and that the
            **   "storageTier" is not set to INVALID_TIER.
            **
            ** NOTE: For the purposes of this implementation, all Storage Server names must start with
            **   "storage-server-". This makes it easier to validate certain attributes and to provide some level
            **   of consistency.
             */
            if (getServerName().contains("storage-server-")) {
                if (getServerNumChunks() == 0) {
                    LOG.warn("There must be at least one chunk per Storage Server chunks: 0");

                    contentValid = false;
                }

                if (getStorageTier() == StorageTierEnum.INVALID_TIER) {
                    contentValid = false;
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

    public String getServerName() {
        return params.get(STORAGE_SERVER_NAME);
    }

    public String getCompartmentId() {
        return params.get(COMPARTMENT_ID_ATTRIBUTE);
    }

    public String getServerIP() {
        return params.get(STORAGE_SERVER_IP);
    }

    /*
    ** Pull the "storage-server-port" out and validate that it is a positive integer
     */
    public int getServerPort() {
        String portStr = params.get(STORAGE_SERVER_PORT);
        int port = -1;

        if (portStr != null) {
            try {
                port = Integer.parseInt(portStr);

                /*
                ** TODO: Determine valid range for port numbers
                 */
                if (port < 0) {
                    LOG.warn(STORAGE_SERVER_PORT + " must be a positive integer - " + port);
                    port = -1;
                }
            } catch (NumberFormatException ex) {
                LOG.warn("Server Port is invalid: " + portStr);
            }
        } else {
            LOG.warn(STORAGE_SERVER_PORT + "attribute is missing");
        }
        return port;
    }

    /*
    ** This will return 0 if the "allocated-chunks" attribute is missing or it is set to a negative
    **   number.
     */
    public int getServerNumChunks() {
        String numberOfChunksStr = params.get(STORAGE_SERVER_NUM_CHUNKS);
        int chunks = 0;

        if (numberOfChunksStr != null) {
            try {
                chunks = Integer.parseInt(numberOfChunksStr);

                /*
                ** Must be at least 1 chunk for a Storage Server
                 */
                if (chunks < 1) {
                    chunks = 0;
                }
            } catch (NumberFormatException ex) {
                LOG.warn("Number of chunks is invalid: " + numberOfChunksStr);
            }
        } else {
            LOG.warn(STORAGE_SERVER_NUM_CHUNKS + " attribute is missing");
        }
        return chunks;
    }

}
