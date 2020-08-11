package com.webutils.webserver.http;

import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class AllocateChunksResponseContent extends ContentParser {

    private static final Logger LOG = LoggerFactory.getLogger(AllocateChunksResponseContent.class);

    protected final LinkedList<String> requiredSubAttributes;

    /*
    ** The response for the AllocateChunks request to the ChunkMgr service looks like the following:
    **
    **   {
    **      "chunk-chunkIndex":
    **       {
    **          "service-name": "  server.getServerName()  "
    **          "storage-id": " server.getServerId() "
    **          "server-ip": " server.getServerIpAddress().toString() "
    **          "server-port": " server.getServerTcpPort() "
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

        requiredSubAttributes.add(SERVICE_NAME);
        requiredSubAttributes.add(STORAGE_ID);
        requiredSubAttributes.add(SERVER_IP);
        requiredSubAttributes.add(SERVER_PORT);
        requiredSubAttributes.add(HttpInfo.CHUNK_ID);
        requiredSubAttributes.add(CHUNK_UID);
        requiredSubAttributes.add(HttpInfo.CHUNK_LBA);
        requiredSubAttributes.add(HttpInfo.CHUNK_LOCATION);
    }

    /*
     ** This is used to validate that the required fields are present for the AllocateChunks method.
     **   The required fields are all present within the "sub-category":
     **
     ** NOTE: At some point it might be worth validating that there are no unexpected attributes passed in. The other
     **   thing to validate would be the contents of the attributes to make sure garbage data is not provided.
     */
    public boolean validateContentData() {
        contentValid = true;

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
                    contentValid = false;

                    LOG.error("Missing required attribute: " + attribute);
                    break;
                }
            }

        } else {
            LOG.error("Invalid bracketDepth: " + bracketDepth);
            contentValid = false;
        }

        for (Map.Entry<String, Map<String, String>> entry : chunkAllocations.entrySet()) {
            Map<String, String> subCategory = entry.getValue();

            for (String subAttribute: requiredSubAttributes) {
                if (subCategory.get(subAttribute) == null) {
                    LOG.warn("AllocateChunks missing required sub-attribute: " + subAttribute);
                    contentValid = false;
                }
            }
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
    ** This pulls the information passed back in the response content from the AllocateChunks method and puts it
    **   into the ServerIdentifier class (actually a list of ServerIdentifiers).
     */
    public void extractAllocations(final List<ServerIdentifier> servers, final int chunkNumber) {
        for (Map.Entry<String, Map<String, String>> entry : chunkAllocations.entrySet()) {

            Map<String, String> subCategory = entry.getValue();

            String serverName = getStr(subCategory, SERVICE_NAME);
            InetAddress inetAddress = getServerIp(subCategory);
            if (inetAddress != null) {
                int port = getServerPort(subCategory);
                ServerIdentifier server = new ServerIdentifier(serverName, inetAddress, port, chunkNumber);

                LOG.info("extractContent() serverName: " + serverName + " chunkUID: " + getStr(subCategory, CHUNK_UID));

                /*
                 ** Need to add the rest of the fields
                 */
                server.setChunkId(getId(subCategory, HttpInfo.CHUNK_ID));
                server.setChunkLBA(getChunkLba(subCategory));
                server.setChunkUID(getStr(subCategory, CHUNK_UID));
                server.setChunkLocation(getStr(subCategory, HttpInfo.CHUNK_LOCATION));
                server.setServerId(getId(subCategory, STORAGE_ID));

                servers.add(server);
            }
        }

        LOG.info("extractContent() servers found: " + servers.size());
    }

    public void cleanup() {
        /*
         ** Done with the data, clear all the places it is held
         */
        clearAllMaps();
    }


    /*
    ** This is used to obtain the "storage-id" and "chunk-id". This converts the String into an integer and validates
    **   that it is a positive integer.
    ** The "chunk-id" is the value that uniquely represents the chunk in the ServiceServersDb.StorageServerChunk table.
     */
    private int getId(final Map<String, String> subCategory, final String requestedId) {
        String idStr = subCategory.get(requestedId);
        int id = -1;

        if (idStr != null) {
            try {
                id = Integer.parseInt(idStr);

                /*
                 ** Make sure it is a positive integer
                 */
                if (id < 0) {
                    LOG.warn(requestedId + " must be a positive integer - " + id);
                    id = -1;
                }
            } catch (NumberFormatException ex) {
                LOG.warn("Chunk ID is invalid: " + idStr);
            }
        } else {
            LOG.warn(requestedId + " attribute is missing");
        }
        return id;
    }

    /*
    ** The following is used to obtain the "chunk-lba" for the allocated chunk
     */
    private int getChunkLba(final Map<String, String> subCategory) {
        String lbaStr = subCategory.get(HttpInfo.CHUNK_LBA);
        int lba = -1;

        if (lbaStr != null) {
            try {
                lba = Integer.parseInt(lbaStr);

                /*
                 ** Make sure it is a positive integer
                 */
                if (lba < 0) {
                    LOG.warn("chunk-lba must be a positive integer - " + lba);
                    lba = -1;
                }
            } catch (NumberFormatException ex) {
                LOG.warn("chunk-lba is invalid: " + lbaStr);
            }
        } else {
            LOG.warn("chunk-lba attribute is missing");
        }
        return lba;
    }

}
