package com.webutils.webserver.mysql;

import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.StorageTierEnum;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class ObjectTableMgr extends ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(BucketTableMgr.class);

    private final static String CREATE_OBJECT_1 = "INSERT INTO object VALUES ( NULL, '";       // objectName
    private final static String CREATE_OBJECT_2 = "', '";                                      // opcClientRequestId not null
    private final static String CREATE_OBJECT_2_NULL = "', NULL, ";                            // contentLength when opcClientRequestId is null
    private final static String CREATE_OBJECT_3 = "', ";                                       // contentLength
    private final static String CREATE_OBJECT_4 = " , ";                                       // storageType
    private final static String CREATE_OBJECT_5 = " , CURRENT_TIMESTAMP(), NULL, 0, CURRENT_TIMESTAMP(), UUID_TO_BIN(UUID()),";
    private final static String CREATE_OBJECT_6 = " (SELECT bucketId FROM bucket WHERE bucketUID = UUID_TO_BIN('";
    private final static String CREATE_OBJECT_7 = "') ), (SELECT namespaceId FROM customerNamespace WHERE namespaceUID = UUID_TO_BIN('";
    private final static String CREATE_OBJECT_8 = "') ) )";

    private final static String GET_OBJECT_UID_1 = "SELECT BIN_TO_UUID(objectUID) objectUID FROM object WHERE objectName = '";
    private final static String GET_OBJECT_UID_2 = "' AND bucketId = ( SELECT bucketId FROM bucket WHERE bucketUID = UUID_TO_BIN('";
    private final static String GET_OBJECT_UID_3 = "' ) )";

    private final static String GET_OBJECT_UID_USING_ID = "SELECT BIN_TO_UUID(objectUID) objectUID FROM object WHERE objectId = ";

    private final static String GET_OBJECT_ID_1 = "SELECT objectId FROM object WHERE objectName = '";
    private final static String GET_OBJECT_ID_2 = "' AND bucketId = ( SELECT bucketId FROM bucket WHERE bucketUID = UUID_TO_BIN('";
    private final static String GET_OBJECT_ID_3 = "' ) )";

    private final static String GET_OBJECT_CREATE_TIME = "SELECT createTime FROM object WHERE objectID = ";

    private final static String GET_OPC_CLIENT_ID = "SELECT opcClientRequestId FROM object WHERE objectId = ";

    private final static String SUCCESS_HEADER_1 = "opc-client-request-id: ";
    private final static String SUCCESS_HEADER_2 = "opc-request-id: ";
    private final static String SUCCESS_HEADER_3 = "opc-content-md5: ";
    private final static String SUCCESS_HEADER_4 = "ETag: ";
    private final static String SUCCESS_HEADER_5 = "last-modified: ";

    /*
     ** The opcRequestId is used to track the request through the system. It is uniquely generated for each
     **   request connection.
     */
    private final int opcRequestId;


    public ObjectTableMgr(final WebServerFlavor flavor, final int requestId) {
        super(flavor);

        LOG.info("ObjectTableMgr() flavor: " + flavor + " requestId: " + requestId);
        this.opcRequestId = requestId;
    }

    public int createObjectEntry(final HttpRequestInfo objectCreateInfo, final String tenancyUID) {
        int status = HttpStatus.OK_200;

        /*
         ** Obtain the fields required to build the Object table entry. Verify that the required fields are not missing.
         */
        String objectName = objectCreateInfo.getObject();
        String namespace = objectCreateInfo.getNamespace();
        String bucketName = objectCreateInfo.getBucket();
        String opcClientRequestId = objectCreateInfo.getOpcClientRequestId();
        int contentLength = objectCreateInfo.getContentLength();

        if ((objectName == null) || (bucketName == null) || (namespace == null)) {
            String failureMessage = "\"PUT Object missing required attributes\",\n  \"objectName\": \"" + objectName +
                    "\",\n  \"bucketName\": \"" + bucketName + "\",\n \"namespaceName\": \"" + namespace + "\"";
            LOG.warn(failureMessage);
            objectCreateInfo.setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
            return HttpStatus.BAD_REQUEST_400;
        }

        LOG.info("createObjectEntry() objectName: " + objectName + " bucketName: " + bucketName);

        /*
        ** First need to validate that the namespace exists
         */
        NamespaceTableMgr namespaceMgr = new NamespaceTableMgr(flavor);
        String namespaceUID = namespaceMgr.getNamespaceUID(namespace, tenancyUID);
        if (namespaceUID == null) {
            LOG.warn("Unable to create Object: " + objectName + " - invalid namespace: " + namespace);

            String failureMessage = "\"Namespace not found\"\n  \"namespaceName\": \"" + namespace + "\"";
            objectCreateInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        /*
        ** Now make sure the Bucket that this Object is stored within actually exists.
         */
        BucketTableMgr bucketMgr = new BucketTableMgr(flavor, opcRequestId, objectCreateInfo);
        String bucketUID = bucketMgr.getBucketUID(bucketName, namespaceUID);
        if (bucketUID == null) {
            LOG.warn("Unable to create Object: " + objectName + " - invalid bucket: " + bucketName);

            String failureMessage = "\"Bucket not found\",\n  \"bucketName\": \"" + bucketName + "\"";
            objectCreateInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        StorageTierEnum tier = bucketMgr.getBucketStorageTier(bucketUID);

        String createObjectStr;
        if (opcClientRequestId != null) {
            createObjectStr = CREATE_OBJECT_1 + objectName + CREATE_OBJECT_2 + opcClientRequestId + CREATE_OBJECT_3 +
                    contentLength + CREATE_OBJECT_4 + tier.toInt() + CREATE_OBJECT_5 + CREATE_OBJECT_6 + bucketUID +
                    CREATE_OBJECT_7 + namespaceUID + CREATE_OBJECT_8;
        } else {
            createObjectStr = CREATE_OBJECT_1 + objectName + CREATE_OBJECT_2_NULL +
                    contentLength + CREATE_OBJECT_4 + tier.toInt() + CREATE_OBJECT_5 + CREATE_OBJECT_6 + bucketUID +
                    CREATE_OBJECT_7 + namespaceUID + CREATE_OBJECT_8;
        }

        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(createObjectStr);
            } catch (SQLException sqlEx) {
                LOG.error("createObjectEntry() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + createObjectStr);
                System.out.println("SQLException: " + sqlEx.getMessage());

                objectCreateInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, "\"SQL Exception\"");
                status = HttpStatus.INTERNAL_SERVER_ERROR_500;
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("createObjectEntry() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeObjectStorageDbConn(conn);
        }

        /*
        ** When the request is successful, there are a number of headers to return.
         */
        if (status == HttpStatus.OK_200) {
            objectCreateInfo.setResponseHeaders(buildSuccessHeader(objectCreateInfo, objectName, bucketUID));
        }

        return status;
    }

    /*
     ** This obtains the Bucket UID. It will return NULL if the Bucket does not exist.
     **
     ** NOTE: A Bucket is unique to a region and a namespace (there is one Object Storage namespace per region per
     **   tenancy).
     */
    public String getObjectUID(final String objectName, final String bucketUID) {
        String getObjectUIDStr = GET_OBJECT_UID_1 + objectName + GET_OBJECT_UID_2 + bucketUID + GET_OBJECT_UID_3;

        return getUID(getObjectUIDStr);
    }

    public String getObjectUID(final int objectId) {
        String getObjectUIDStr = GET_OBJECT_UID_USING_ID + objectId;

        return getUID(getObjectUIDStr);
    }

    private int getObjectId(final String objectName, final String bucketUID) {
        String getObjectIdStr = GET_OBJECT_ID_1 + objectName + GET_OBJECT_ID_2 + bucketUID + GET_OBJECT_ID_3;

        return getId(getObjectIdStr);
    }

    private String getObjectCreationTime(final int objectId) {
        String getCreateTime = GET_OBJECT_CREATE_TIME + objectId;

        return getSingleStr(getCreateTime);
    }

    private String getOpcClientRequestId(final int objectId) {
        String getOpClientId = GET_OPC_CLIENT_ID + objectId;

        return getSingleStr(getOpClientId);
    }

    /*
     ** This builds the OK_200 response headers for the PUT Object command. This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     **   opc-content-md5
     **   ETag - This is the generated objectUID that is unique to this object
     **   last-modified - The Date/Time this object was created.
     */
    private String buildSuccessHeader(final HttpRequestInfo objectCreateInfo, final String objectName, final String bucketUID) {
        String successHeader;

        int objectId = getObjectId(objectName, bucketUID);
        if (objectId != -1) {
            String createTime = getObjectCreationTime(objectId);
            String objectUID = getObjectUID(objectId);
            String opcClientId = objectCreateInfo.getOpcClientRequestId();
            String contentMD5 = objectCreateInfo.getContentMd5();

            if (opcClientId != null) {
                successHeader = SUCCESS_HEADER_1 + opcClientId + "\n" + SUCCESS_HEADER_2 + opcRequestId + "\n" +
                        SUCCESS_HEADER_3 + contentMD5 + "\n" + SUCCESS_HEADER_4 + objectUID + "\n" +
                        SUCCESS_HEADER_5 + createTime + "\n";
            } else {
                successHeader = SUCCESS_HEADER_2 + opcRequestId + "\n" + SUCCESS_HEADER_3 + contentMD5 + "\n" +
                        SUCCESS_HEADER_4 + objectUID + "\n" + SUCCESS_HEADER_5 + createTime + "\n";
            }

            LOG.info(successHeader);
        } else {
            successHeader = null;
        }

        return successHeader;
    }

}
