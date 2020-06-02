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

    public void extractAllocations(final List<ServerIdentifier> servers, final int chunkNumber) {
        for (Map.Entry<String, Map<String, String>> entry : chunkAllocations.entrySet()) {

            Map<String, String> subCategory = entry.getValue();

            String serverName = getStr(subCategory, SERVER_NAME);
            InetAddress inetAddress = getServerIp(subCategory);
            if (inetAddress != null) {
                int port = getChunkServerPort(subCategory);
                ServerIdentifier server = new ServerIdentifier(serverName, inetAddress, port, chunkNumber);

                /*
                 ** Need to add the rest of the fields
                 */
                server.setChunkId(getId(subCategory, CHUNK_ID));
                server.setChunkLBA(getChunkLba(subCategory));
                server.setChunkUID(getStr(subCategory, CHUNK_UID));
                server.setChunkLocation(getStr(subCategory, CHUNK_LOCATION));
                server.setServerId(getId(subCategory, STORAGE_ID));

                servers.add(server);
            }
        }

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
        String lbaStr = subCategory.get(CHUNK_LBA);
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

    /*
     ** The following is used to obtain the "storage-server-port" for the allocated chunk
     */
    private int getChunkServerPort(final Map<String, String> subCategory) {
        String portStr = subCategory.get(SERVER_PORT);
        int port = -1;

        if (portStr != null) {
            try {
                port = Integer.parseInt(portStr);

                /*
                 ** Make sure it is a positive integer
                 */
                if (port < 0) {
                    LOG.warn("storage-server-port must be a positive integer - " + port);
                    port = -1;
                }
            } catch (NumberFormatException ex) {
                LOG.warn("storage-server-port is invalid: " + portStr);
            }
        } else {
            LOG.warn("storage-server-port attribute is missing");
        }
        return port;
    }

    /*
    ** The following is used to extract the following Strings from the response:
    **   storage-server-name
    **   chunk-uid
    **   chunk-location
     */
    private String getStr(final Map<String, String> subCategory, final String requestedStr) {
        String str = subCategory.get(requestedStr);

        if (str == null) {
            LOG.warn(requestedStr + " attribute is missing");
        }
        return str;
    }

    /*
    ** The following is used to pull out the "storage-server-ip". This needs to handle the case where the hostname
    **   string is something like:
    **     "localhost/127.0.0.1"
    **    This is the purpose of the StringTokenizer() used below.
     */
    private InetAddress getServerIp(final Map<String, String> subCategory) {
        String str = subCategory.get(SERVER_IP);

        LOG.info("serverIp: " + str);

        InetAddress inetAddress = null;
        if (str != null) {
            String hostName;

            StringTokenizer stk = new StringTokenizer(str, " /");
            if (stk.hasMoreTokens()) {
                hostName = stk.nextToken();
            } else {
                hostName = str;
            }

            try {
                inetAddress = InetAddress.getByName(hostName);
            } catch (UnknownHostException ex) {
                LOG.warn("IP address results in unknown host: " + str + " ex: " + ex.getMessage());
            }
        } else {
            LOG.warn(SERVER_IP + " attribute is missing");
        }

        return inetAddress;
    }

}
