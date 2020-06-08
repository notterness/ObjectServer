package com.webutils.webserver.http;

import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DeleteChunksResponseParser extends ContentParser{

    private static final Logger LOG = LoggerFactory.getLogger(DeleteChunksResponseParser.class);

    protected final LinkedList<String> requiredSubAttributes;

    /*
     ** The response for the DeleteChunks request to the ChunkMgr service looks like the following:
     **
     * TODO: Should the request actually return the chunks that were deleted?
     **   {
     **      "chunk-chunkIndex":
     **       {
     **          "storage-server-name": "  server.getServerName()  "
     **          "chunk-uid": " server.getChunkUID() "
     **       }
     */

    public DeleteChunksResponseParser() {
        super();

        /*
         ** Fill in the list of required sub-attributes so they are easy to check
         */
        requiredSubAttributes = new LinkedList<>();

        requiredSubAttributes.add(SERVER_NAME);
        requiredSubAttributes.add(CHUNK_UID);
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
     ** This pulls the information passed back in the response content from the DeleteChunksResponseParser method and puts it
     **   into the ChunkDeleteInfo class (actually a list of ServerIdentifiers).
     */
    public void extractContent(final List<ServerIdentifier> servers, final int chunkNumber) {
        for (Map.Entry<String, Map<String, String>> entry : chunkAllocations.entrySet()) {

            Map<String, String> subCategory = entry.getValue();

            String serverName = getStr(subCategory, SERVER_NAME);

            LOG.info("extractContent() serverName: " + serverName + " chunkUID: " + getStr(subCategory, CHUNK_UID));
        }

         /*
         ** Done with the data, clear all the places it is held
         */
        clearAllMaps();
    }


    /*
     ** The following is used to extract the following Strings from the response:
     **   storage-server-name
     **   chunk-etag
     **   chunk-location
     */
    private String getStr(final Map<String, String> subCategory, final String requestedStr) {
        String str = subCategory.get(requestedStr);

        if (str == null) {
            LOG.warn(requestedStr + " attribute is missing");
        }
        return str;
    }

}
