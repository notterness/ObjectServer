package com.webutils.chunkmgr.http;

import com.webutils.webserver.http.ParseRequestContent;
import com.webutils.webserver.http.StorageTierEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CreateServerPostContent extends ParseRequestContent {

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

            /*
            ** Validate that the Port, Number of Chunks are integers
             */
            if (getServerPort() == -1) {
                valid = false;
            }

            if (getServerNumChunks() == -1) {
                valid = false;
            }

            if (getStorageTier() == StorageTierEnum.INVALID_TIER) {
                valid = false;
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

    public int getServerNumChunks() {
        String numberOfChunksStr = params.get(STORAGE_SERVER_NUM_CHUNKS);
        int chunks = -1;

        if (numberOfChunksStr != null) {
            try {
                chunks = Integer.parseInt(numberOfChunksStr);

                /*
                ** Must be at least 1 chunk for a Storage Server
                 */
                if (chunks < 1) {
                    LOG.warn("There must be at least one chunk per Storage Server chunks: " + chunks);
                    chunks = -1;
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
