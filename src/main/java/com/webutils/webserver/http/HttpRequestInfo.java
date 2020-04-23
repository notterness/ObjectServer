package com.webutils.webserver.http;

import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class HttpRequestInfo extends HttpInfo {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestInfo.class);

     /*
     ** This defines the redundancy and placement for the Objects within a bucket
     */
    private StorageTierEnum storageTier;

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

    /*
    ** This map is used to hold the Object Name, Bucket Name and Tenancy Name for the created objected.
    ** For Storage Servers, there is also a Test Type that is used to force certain behaviors in the
    **   Storage Server's responses (i.e. disconnect the connection).
     */
    private final Map<String, String> putObjectInfoMap;


    public HttpRequestInfo(final RequestContext requestContext) {

        super(requestContext);

        putObjectInfoMap = new HashMap<>(3);

        /*
        ** Set the objectId to -1 to indicate that it is not valid.
        ** Set the storageTier to the default value of STANDARD_TIER. The value for the storageTier comes from the
        **   Bucket that the Object is being stored in.
         */
        objectId = -1;
        storageTier = StorageTierEnum.STANDARD_TIER;
    }


    /*
     ** Clear out all of the String fields and release the memory
     */
    void reset() {

        storageTier = StorageTierEnum.STANDARD_TIER;
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
        if (successResponseHeaders == null) {
            return "";
        }

        return successResponseHeaders;
    }

    public String getResponseContent() {
        if (successResponseContent == null) {
            return "";
        }

        return successResponseContent;
    }

}
