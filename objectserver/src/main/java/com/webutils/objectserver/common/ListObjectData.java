package com.webutils.objectserver.common;

import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.mysql.BucketTableMgr;
import com.webutils.webserver.mysql.NamespaceTableMgr;
import com.webutils.webserver.mysql.ObjectTableMgr;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
** This holds the information to complete the ListObject method and to return data to the client
 */
public class ListObjectData {

    private static final Logger LOG = LoggerFactory.getLogger(ListObjectData.class);


    private static final List<String> validFields = new ArrayList<>(List.of("name", "etag", "version", "md5", "size", "time-created", "tier"));

    private final RequestContext requestContext;
    private final HttpRequestInfo listHttpInfo;
    private final List<String> fieldsToDisplay;

    private final String tenancyUID;
    private final int opcRequestId;

    private final WebServerFlavor flavor;

    public ListObjectData(final RequestContext requestContext, final HttpRequestInfo listHttpInfo,
                          final List<String> objectFields, final String tenancyUID) {

        this.requestContext = requestContext;
        this.listHttpInfo = listHttpInfo;

        this.fieldsToDisplay = new LinkedList<>();
        validateFields(objectFields);

        this.tenancyUID = tenancyUID;

        this.flavor = requestContext.getWebServerFlavor();
        this.opcRequestId = requestContext.getRequestId();
    }

    public void execute() {
        String queryStr = buildQueryStr();
        if (queryStr != null) {
            ObjectTableMgr objectMgr = new ObjectTableMgr(flavor, requestContext);

            List<Map<String, String>> objectList = new LinkedList<>();
            objectMgr.retrieveMappedObjectInfo(queryStr, objectList);
            requestContext.getHttpInfo().setResponseContent(buildObjectResponse(objectList));

            /*
            ** Build the response headers. For the ListObjects method the following are returned:
            **   opc-client-request-id
            **   opc-request-id
             */
            String opcClientRequestId = requestContext.getHttpInfo().getOpcClientRequestId();
            int opcRequestId = requestContext.getRequestId();

            String successHeaders;
            if (opcClientRequestId != null) {
                successHeaders = "opc-client-request-id: " + opcClientRequestId + "\n" +
                        "opc-request-id: " + opcRequestId + "\n";
            } else {
                successHeaders = "opc-request-id: " + opcRequestId + "\n";
            }
            requestContext.getHttpInfo().setResponseHeaders(successHeaders);
        }
    }

    /*
    ** The current list of valid fields are:
    **    "name" - This is the object name
    **    "etag" - This is the objectUID in the object database table.
    **    "version" - The version of the object
    **    "md5" - The Md5 digest for the object
    **    "size" - The size in bytes of the object
    **    "time-created" - The createTime from the object database table
    **    "tier" - This is the StorageTierEnum for the storageType
     */
    private void validateFields(final List<String> objectFields) {

        for (String objectField : objectFields) {
            String lowerCaseItem = objectField.toLowerCase();
            if (validFields.contains(lowerCaseItem)) {
                fieldsToDisplay.add(lowerCaseItem);
            }
        }
    }

    private String buildQueryStr() {
        String namespace = listHttpInfo.getNamespace();
        String bucketName = listHttpInfo.getBucket();

        if ((bucketName == null) || (namespace == null)) {
            String failureMessage = "{\r\n  \"code\":" + HttpStatus.BAD_REQUEST_400 +
                    "\r\n  \"message\": \"ListObject missing required attributes\" + " +
                    "\r\n  \"bucketName\": \"" + bucketName + "\"" +
                    "\r\n \"namespaceName\": \"" + namespace + "\"" +
                    "\r\n}";
            LOG.warn(failureMessage);
            listHttpInfo.setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
            return null;
        }

        LOG.info("buildQueryStr() namespace: " + namespace + " bucketName: " + bucketName);

        /*
         ** First need to validate that the namespace exists
         */
        NamespaceTableMgr namespaceMgr = new NamespaceTableMgr(flavor);
        String namespaceUID = namespaceMgr.getNamespaceUID(namespace, tenancyUID);
        if (namespaceUID == null) {
            LOG.warn("Unable to execute ListObject method - invalid namespace: " + namespace);

            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Namespace not found\"" +
                    "\r\n  \"namespaceName\": \"" + namespace + "\"" +
                    "\r\n}";
            listHttpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return null;
        }

        /*
         ** Now make sure the Bucket that this Object is stored within actually exists.
         */
        BucketTableMgr bucketMgr = new BucketTableMgr(flavor, opcRequestId, listHttpInfo);
        int bucketId = bucketMgr.getBucketId(bucketName, namespaceUID);
        if (bucketId == -1) {
            LOG.warn("Unable to execute ListObject method - invalid bucket: " + bucketName);

            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Bucket not found\"" +
                    "\r\n  \"bucketName\": \"" + bucketName + "\"" +
                    "\r\n}";
            listHttpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return null;
        }

        String queryStr = "SELECT objectId, objectName, versionId, contentLength, storageType, contentMd5, createTime, " +
                "lastUpdateTime, BIN_TO_UUID(objectUID) objectUID, deleteMarker FROM object WHERE bucketId = " + bucketId;
        return queryStr;
    }

    private String buildObjectResponse(final List<Map<String, String>> objectList) {
        StringBuilder responseStr = new StringBuilder("{\r\n  \"data\": [\r\n");
        int displayFields = fieldsToDisplay.size();
        int retrievedObjects = objectList.size();

        int objectCount = 0;

        for (Map<String, String> currObject : objectList) {
            responseStr.append("    {\r\n");

            int count = 0;
            for (String fieldName : fieldsToDisplay) {
                String objectField = currObject.get(fieldName);
                if (objectField != null) {
                    responseStr.append("      \"").append(fieldName).append("\": \"").append(objectField).append("\"");
                }

                count++;
                if (count == displayFields) {
                    responseStr.append("\r\n");
                } else {
                    responseStr.append(",\r\n");
                }
            }

            objectCount++;
            if (objectCount != retrievedObjects) {
                responseStr.append("    },\r\n");
            } else {
                responseStr.append("    }\r\n");
            }
        }

        responseStr.append("  ]\r\n}");

        return responseStr.toString();
    }

}
