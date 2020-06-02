package com.webutils.webserver.http;

import org.eclipse.jetty.http.BadMessageException;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public abstract class HttpRequestInfo extends HttpInfo {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestInfo.class);

    private static final String STORAGE_TIER_HEADER = "storageTier";
    private static final String SERVER_NAME_HEADER = "storage-server-name";
    private static final String CHUNK_STATUS_HEADER = "chunk-status";

    /*
    ** The objectId is the identifier to access the Object record within the ObjectStorageDd after it has been created. It is
    **   generated during the INSERT operation on the table and uses an AUTO_INCREMENT field.
    ** The objectUID is String that represents the Object and is also generated during the INSERT operation using the
    **   UUID() MySQL function.
    **
    ** NOTE: These variables are only filled in for operations that operate on Objects
     */
    private int objectId;
    private String objectUID;

    private String successResponseContent;
    private String successResponseHeaders;

    public HttpRequestInfo() {

        super();

        /*
        ** Set the objectId to -1 to indicate that it is not valid.
         */
        objectId = -1;
    }


    /*
     ** Clear out all of the String fields and release the memory
     */
    void reset() {

        objectId = -1;
        objectUID = null;

        successResponseContent = null;
        successResponseHeaders = null;
    }


    public void setObjectId(final int id, final String uid) {
        objectId = id;
        objectUID = uid;
    }

    public int getObjectId() { return objectId; }

    public String getObjectUID() { return objectUID; }

    /*
     ** These are the setters and getters for response headers and response content
     */
    public void setResponseHeaders(final String responseHeaders) {
        successResponseHeaders = responseHeaders;
    }

    public void setResponseContent(final String responseContent) {
        successResponseContent = responseContent;
    }

    public String getResponseHeaders() {
        return Objects.requireNonNullElse(successResponseHeaders, "");
    }

    public String getResponseContent() {
        return Objects.requireNonNullElse(successResponseContent, "");
    }

    /*
     ** When the headers have been completely read in, that will be the time to insure it is valid
     **   and the field values make sense.
     */
    public abstract void setHeaderComplete();

    /*
     ** This function will pull out the various information in the HTTP header fields and add it to
     ** the associated string within this object.
     */
    public void addHeaderValue(HttpField field) {
        super.addHeaderValue(field);

        /*
         ** The CONTENT_LENGTH is parsed out early as it is used in the headers parsed callback to
         **   setup the next stage of the connection pipeline.
         */
        final String fieldName = field.getName().toLowerCase();

        int result = fieldName.indexOf(CONTENT_LENGTH.toLowerCase());
        if (result != -1) {
            try {
                contentLengthReceived = true;
                contentLength = Integer.parseInt(field.getValue());

                /*
                 ** TODO: Are there specific limits for the Content-Length that need to be validated
                 */
                if (contentLength < 0) {
                    contentLength = 0;
                    parseFailureCode = HttpStatus.RANGE_NOT_SATISFIABLE_416;
                    parseFailureReason = HttpStatus.getMessage(parseFailureCode);

                    LOG.info("Invalid Content-Length [" + requestId +  "] code: " +
                            parseFailureCode + " reason: " + parseFailureReason);
                }
            } catch (NumberFormatException num_ex) {
                LOG.info("addHeaderValue() " + field.getName() + " " + num_ex.getMessage());
            }
        }
    }

    /*
    ** This needs to inform the RequestContext that there was a parsing error for the HTTP request.
     */
    public void httpHeaderError(final BadMessageException failure) {
        super.httpHeaderError(failure);
    }

    /*
    ** Special parsing for the ListChunks and ListServers methods
     */
    public String getServerName() {
        List<String> serverNames = headers.get(SERVER_NAME_HEADER);

        if ((serverNames == null) || (serverNames.size() != 1)) {
            return null;
        }

        /*
         ** Validate the format of the location to insure it is only valid characters (letter, numbers, '_',
         **   '-', '/', and '.')
         */
        String serverName = serverNames.get(0);
        if (!serverName.matches("^[a-zA-Z0-9-_./]*$")) {
            serverName = null;
        }
        return serverName;
    }

    /*
     ** The possible Storage Tiers are:
     **
     **   Invalid - If this header was not passed in or had an invalid tier.
     **   Standard - 3 copies of the data within the same data center
     **   Intelligent-Tiering - Moves data between fast and slow disk depending on access patterns. Always uses 3
     **     copies of the data.
     **   Standard-IA (Infrequent Access) - 3 copies on slow disk
     **   OneZone (Another form of Infrequent Access with less redundacy) - 2 copies of the data on slow disk.
     **   Archive (slower access than Standard-IA) -
     **   DeepArchive (slowest access of all, data may be kept on offline storage) -
     **
     ** NOTE: If the "storageTier" header is not set, then the default is "Invalid"
     */
    public StorageTierEnum getStorageTier() {
        List<String> storageTiers = headers.get(STORAGE_TIER_HEADER);

        if ((storageTiers == null) || (storageTiers.size() != 1)) {
            return StorageTierEnum.INVALID_TIER;
        }

        String storageTierStr = storageTiers.get(0);
        StorageTierEnum storageTier;

        if (storageTierStr == null) {
            storageTier = StorageTierEnum.INVALID_TIER;
        } else {
            storageTier = StorageTierEnum.fromString(storageTierStr);
        }

        return storageTier;
    }

    /*
     ** The possible Chunk Status values are:
     **
     **   INVALID - If this header was not passed in or had an invalid status.
     **   ALLOCATED - The chunk has been allocated and is in use on a Storage Server to hold data
     **   DELETED - The chunk has been deleted (meaning the object is no longer available to the client), but it has
     **     not been cleaned up on the Storage Server yet.
     **   AVAILABLE - The chunk is available to be allocated.
     **
     ** NOTE: If the "chunk-status" header is not set, then the default is "INVALID"
     */
    public ChunkStatusEnum getChunkStatus() {
        List<String> chunkStatus = headers.get(CHUNK_STATUS_HEADER);

        if ((chunkStatus == null) || (chunkStatus.size() != 1)) {
            return ChunkStatusEnum.INVALID_CHUNK_STATUS;
        }

        String chunkStatusStr = chunkStatus.get(0);
        ChunkStatusEnum status;

        if (chunkStatusStr == null) {
            status = ChunkStatusEnum.INVALID_CHUNK_STATUS;
        } else {
            status = ChunkStatusEnum.fromString(chunkStatusStr);
        }

        return status;
    }

}
