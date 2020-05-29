package com.webutils.webserver.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Map;

public class AllocateChunksResponseContent extends ContentParser {

    private static final Logger LOG = LoggerFactory.getLogger(AllocateChunksResponseContent.class);

    private static final String SERVER_NAME = "storage-server-name";
    private static final String STORAGE_ID = "storage-id";
    private static final String SERVER_IP = "storage-server-ip";
    private static final String SERVER_PORT = "storage-server-port";
    private static final String CHUNK_ID = "chunk-id";
    private static final String CHUNK_UID = "chunk-uid";
    private static final String CHUNK_LBA = "chunk-lba";
    private static final String CHUNK_LOCATION = "chunk-location";

    protected final LinkedList<String> requiredSubAttributes;

    /*
    ** The response for the AllocateChunks request to the ChunkMgr service looks like the following:
    **
    **   {
    **      "chunk-chunkIndex":
    **       {
    **          "storage-server-name": "  server.getServerName()  "
    **          "storage-id": " server.getServerId() "
    **          "storage-server-ip": " server.getServerIpAddress().toString() "
    **          "storage-server-port": " server.getServerTcpPort() "
    **          "chunk-id": " server.getChunkId() "
    **          "chunk-uid": " server.getChunkUID() "
    **          "chunk-lba": " server.getChunkLBA() "
    **          "chunk-location": " + server.getChunkLocation() "
    **       }
     */

    public AllocateChunksResponseContent() {
        super();

        /*
         ** Fill in the list of required sub-attributes so they are easy to check
         */
        requiredSubAttributes = new LinkedList<>();

        requiredSubAttributes.add(SERVER_NAME);
        requiredSubAttributes.add(STORAGE_ID);
        requiredSubAttributes.add(SERVER_IP);
        requiredSubAttributes.add(SERVER_PORT);
        requiredSubAttributes.add(CHUNK_ID);
        requiredSubAttributes.add(CHUNK_UID);
        requiredSubAttributes.add(CHUNK_LBA);
        requiredSubAttributes.add(CHUNK_LOCATION);
    }

    /*
     ** This is used to validate that the required fields are present for the AllocateChunks method.
     **   The required fields are all present within the "sub-category":
     **
     ** NOTE: At some point it might be worth validating that there are no unexpected attributes passed in. The other
     **   thing to validate would be the contents of the attributes to make sure garbage data is not provided.
     */
    public boolean validateContentData() {
        boolean valid = true;

        /*
         ** First make sure that the bracketDepth is 0 to insure the brackets are properly paired after the parsing
         **   and extraction of the information was completed.
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
        LOG.info("Params");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            LOG.info("    " + entry.getKey() + " : " + entry.getValue());
        }

        LOG.info("chunkAllocations");
        for (Map.Entry<String, Map<String, String>> entry : chunkAllocations.entrySet()) {
            LOG.info("    Allocation: " + entry.getKey());

            Map<String, String> subCategory = entry.getValue();
            for (Map.Entry<String, String> subCategoryEntry : subCategory.entrySet()) {
                LOG.info("        " + subCategoryEntry.getKey() + " : " + subCategoryEntry.getValue());
            }
        }
    }

    /*
     ** Pull the "chunk-id" out and validate that it is a positive integer. This is the value that uniquely represents
     **   the chunk in the ServiceServersDb.StorageServerChunk table.
     */
    public int getChunkId() {
        String chunkNumberStr = params.get(CHUNK_ID);
        int chunkNumber = -1;

        if (chunkNumberStr != null) {
            try {
                chunkNumber = Integer.parseInt(chunkNumberStr);

                /*
                 ** Make sure it is a positive integer
                 */
                if (chunkNumber < 0) {
                    LOG.warn(CHUNK_ID + " must be a positive integer - " + chunkNumber);
                    chunkNumber = -1;
                }
            } catch (NumberFormatException ex) {
                LOG.warn("Chunk ID is invalid: " + chunkNumberStr);
            }
        } else {
            LOG.warn(CHUNK_ID + "attribute is missing");
        }
        return chunkNumber;
    }


}
