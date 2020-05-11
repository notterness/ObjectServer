package com.webutils.webserver.mysql;

import com.google.common.io.BaseEncoding;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.StorageTierEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class ObjectTableMgr extends ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(BucketTableMgr.class);

    /*
    ** Fields for the prepared statement are:
    **   1 - objectName (String)
    **   2 - versionId  (String)
    **   3 - opcClientRequestId (String, may be NULL)
    **   4 - contentLength (int)
    **   5 - storageType (int)
    **   6 - contentMd5  (String)
    **   7 - bucketUID (String)
    **   8 - namespaceUID (String)
     */
    private final static String CREATE_OBJ_1 = "INSERT INTO object VALUES ( NULL, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(), NULL, 0, CURRENT_TIMESTAMP(), UUID_TO_BIN(UUID()), 0,";
    private final static String CREATE_OBJ_2 = " (SELECT bucketId FROM bucket WHERE bucketUID = UUID_TO_BIN(?)), ";
    private final static String CREATE_OBJ_3 = " (SELECT namespaceId FROM customerNamespace WHERE namespaceUID = UUID_TO_BIN(?)) )";

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
    private final static String SUCCESS_HEADER_6 = "version-id: ";

    private final static String RETRIEVE_OBJECT_QUERY_1 = "SELECT * FROM object WHERE objectName = '";
    private final static String RETRIEVE_OBJECT_QUERY_2 = "' AND bucketId = ";

    private final static String DELETE_OBJECT_USING_ID = "DELETE FROM object WHERE objectId = ";

    private final static String UPDATE_LAST_READ_ACCESS = "UPDATE object SET readAccessCount = readAccessCount + 1, lastReadAccessTime = CURRENT_TIMESTAMP() WHERE objectId =";

    private final static String GET_HIGHEST_VERSION_OBJECT = "SELECT o1.objectId, o1.objectName, o1.versionId FROM object o1 " +
            "WHERE o1.versionId = (SELECT MAX(o2.versionId) FROM object o2 WHERE o2.objectName = ? AND " +
            "o2.bucketId = (SELECT bucketId FROM bucket WHERE bucketUID = UUID_TO_BIN(?)))";

    /*
     ** The opcRequestId is used to track the request through the system. It is uniquely generated for each
     **   request connection.
     */
    private final RequestContext requestContext;
    private final int opcRequestId;


    public ObjectTableMgr(final WebServerFlavor flavor, final RequestContext requestContext) {
        super(flavor);

        //LOG.info("ObjectTableMgr() flavor: " + flavor + " requestId: " + requestId);
        this.requestContext = requestContext;
        this.opcRequestId = requestContext.getRequestId();
    }

    /*
     ** This is used to create an object representation in the ObjectStorageDb database.
     **
     ** TODO: Handle the "if-match", "if-none-match" headers. Create a function to generate a new object versionId.
     */
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

            String failureMessage = "\"Namespace not found\",\n  \"namespaceName\": \"" + namespace + "\"";
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
        String contentMd5 = objectCreateInfo.getContentMd5();
        if (!contentMd5.equals("NULL")) {
            byte[] md5DigestBytes = BaseEncoding.base64().decode(contentMd5);
            if (md5DigestBytes.length != 16) {
                LOG.warn("The value of the digest '" + contentMd5 + "' incorrect length after base-64 decoding");
                return HttpStatus.PRECONDITION_FAILED_412;
            }
        }

        /*
        ** Check if this object already exists and the if-none-match header is set to "*"
         */
        int objectId = getObjectId(objectName, bucketUID);
        if (objectId == -1) {
            boolean ifNoneMatch = objectCreateInfo.getIfNoneMatch();

            if (ifNoneMatch) {
                objectCreateInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, "\"Object already present\"");
                return HttpStatus.INTERNAL_SERVER_ERROR_500;
            } else {
                /*
                ** Need to update the versionId
                 */
                int id = getHighestVersionObject(objectName, bucketUID);
                objectCreateInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, "\"Object already present\"");
                return HttpStatus.INTERNAL_SERVER_ERROR_500;
            }
        }


        String createObjectStr = CREATE_OBJ_1 + CREATE_OBJ_2 + CREATE_OBJ_3;

        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                /*
                 ** Fields for the prepared statement are:
                 **   1 - objectName (String)
                 **   2 - versionId  (String)
                 **   3 - opcClientRequestId (String, may be NULL)
                 **   4 - contentLength (int)
                 **   5 - storageType (int)
                 **   6 - contentMd5  (BINARY)
                 **   7 - bucketUID (String)
                 **   8 - namespaceUID (String)
                 */
                stmt = conn.prepareStatement(createObjectStr);
                stmt.setString(1, objectName);
                stmt.setString(2, "0");
                LOG.info("createObjectEntry opcClientRequestId: " + opcClientRequestId);
                if (opcClientRequestId != null) {
                    stmt.setString(3, opcClientRequestId);
                } else {
                    stmt.setNull(3, Types.VARCHAR);
                }

                stmt.setInt(4, contentLength);
                stmt.setInt(5, tier.toInt());

                if (!contentMd5.equals("NULL")) {
                    byte[] md5DigestBytes = BaseEncoding.base64().decode(contentMd5);
                    stmt.setBytes(6, md5DigestBytes);
                } else {
                    stmt.setNull(6, Types.BINARY);
                }


                stmt.setString(7, bucketUID);
                stmt.setString(8, namespaceUID);
                stmt.executeUpdate();
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
            /*
             ** Set the objectId in the HttpRequestInfo object so it can easily be accessed when writing out the
             **   chunk information.
             */
            int id = getObjectId(objectName, bucketUID);
            if (id != -1) {
                String objectUID = getObjectUID(id);
                objectCreateInfo.setObjectId(id, objectUID);
                objectCreateInfo.setResponseHeaders(buildSuccessHeader(objectCreateInfo, objectName, bucketUID));
            } else {
                objectCreateInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, "\"Unable to obtain objectId\"");
                status = HttpStatus.INTERNAL_SERVER_ERROR_500;
            }
        }

        return status;
    }

    /*
    ** This retrieves the information needed to access an object. It is used for the GET operations.
    **
    ** TODO: The ObjectInfo should be a list and then this command can support the following
    **   GetObject
    **   HeadObject
    **   ListObjects
    **   ListObjectVersions
     */
    public int retrieveObjectInfo(final HttpRequestInfo objectHttpInfo, final ObjectInfo info, final String tenancyUID) {
        String objectName = info.getObjectName();
        String bucketName = info.getBucketName();
        String namespace = info.getNamespace();

        if ((objectName == null) || (bucketName == null) || (namespace == null)) {
            String failureMessage = "\"GET Object missing required attributes\",\n  \"objectName\": \"" + objectName +
                    "\",\n  \"bucketName\": \"" + bucketName + "\",\n \"namespaceName\": \"" + namespace + "\"";
            LOG.warn(failureMessage);
            objectHttpInfo.setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
            return HttpStatus.BAD_REQUEST_400;
        }

        /*
         ** First need to validate that the namespace exists
         */
        NamespaceTableMgr namespaceMgr = new NamespaceTableMgr(flavor);
        String namespaceUID = namespaceMgr.getNamespaceUID(namespace, tenancyUID);
        if (namespaceUID == null) {
            LOG.warn("Unable to find Object: " + objectName + " - invalid namespace: " + namespace);

            String failureMessage = "\"Namespace not found\",\n  \"namespaceName\": \"" + namespace + "\"";
            objectHttpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        /*
         ** Now make sure the Bucket that this Object is stored within actually exists.
         */
        BucketTableMgr bucketMgr = new BucketTableMgr(flavor, opcRequestId, objectHttpInfo);
        int bucketId = bucketMgr.getBucketId(bucketName, namespaceUID);
        if (bucketId == -1) {
            LOG.warn("Unable to find Object: " + objectName + " - invalid bucket: " + bucketName);

            String failureMessage = "\"Bucket not found\",\n  \"bucketName\": \"" + bucketName + "\"";
            objectHttpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        LOG.info("retrieveObjectInfo() objectName: " + objectName + " bucketName: " + bucketName);

        int objectId = -1;

        Connection conn = getObjectStorageDbConn();
        if (conn != null) {
            String queryStr = RETRIEVE_OBJECT_QUERY_1 + objectName + RETRIEVE_OBJECT_QUERY_2 + bucketId;
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();
                if (stmt.execute(queryStr)) {
                    rs = stmt.getResultSet();
                }
            } catch (SQLException sqlEx) {
                LOG.error("getUID() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + queryStr);
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (rs != null) {
                    try {
                        int count = 0;
                        while (rs.next()) {
                            /*
                            ** Check if any of the records for the objects are in the process of being deleted.
                             */
                            boolean deleteMarker = rs.getBoolean(13);
                            if (!deleteMarker) {
                                if (count == 0) {
                                    /*
                                     ** objectId is the unique identifier for the object
                                     */
                                    objectId = rs.getInt(1);
                                    info.setObjectId(objectId);

                                    /*
                                     ** versionId - field 3
                                     */
                                    String versionId = rs.getString(3);
                                    info.setVersionId(versionId);

                                    /*
                                     ** Field 5 (INT) - contentLength
                                     */
                                    info.setContentLength(rs.getInt(5));

                                    /*
                                     ** Field 6 (BINARY(16)) - contentMd5
                                     */
                                    byte[] md5Bytes = rs.getBytes(6);
                                    if (!rs.wasNull() && (md5Bytes != null)) {
                                        String md5DigestStr = BaseEncoding.base64().encode(md5Bytes);
                                        info.setContentMd5(md5DigestStr);
                                    } else {
                                        info.setContentMd5(null);
                                    }

                                    /*
                                     ** Field 11 - lastUpdateTime
                                     */
                                    String lastModifiedTime = rs.getString(11);
                                    info.setLastModified(lastModifiedTime);
                                }
                                count++;
                            }
                        }

                        if (count != 1) {
                            LOG.warn("retrieveObjectInfo() invalid responses count: " + count);
                            objectId = -1;
                        }
                    } catch (SQLException sqlEx) {
                        LOG.error("retrieveObjectInfo() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("retrieveObjectInfo() SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                        objectId = -1;
                    }

                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("retrieveObjectInfo() rs close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("retrieveObjectInfo() SQL conn rs.close() SQLException: " + sqlEx.getMessage());
                    }
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("retrieveObjectInfo() stmt close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
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
        ** Now use the objectId to get a list of chunks for this object
         */
        int status = HttpStatus.OK_200;
        if (objectId != -1) {
            StorageChunkTableMgr chunkTableMgr = new StorageChunkTableMgr(flavor, objectHttpInfo);
            chunkTableMgr.getChunks(objectId, info.getChunkList());

            /*
             ** Verify that this object actually has some data associated with it (meaning there is at least one chunk
             **   that needs to be read from a StorageServer).
             */
            if (info.getChunkList().isEmpty()) {
                LOG.warn("Object has not data associated with it: " + objectName);
                String failureMessage = "\"Object data not found\",\n  \"objectName\": \"" + objectName + "\"";
                objectHttpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
                status = HttpStatus.PRECONDITION_FAILED_412;
            }
        } else {
            LOG.warn("Object not found: " + objectName);
            String failureMessage = "\"Object not found\",\n  \"objectName\": \"" + objectName + "\"";
            objectHttpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            status = HttpStatus.PRECONDITION_FAILED_412;
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

    /*
    ** The full way to obtain unique identifier for an Object
     */
    public int getObjectId(final HttpRequestInfo objectCreateInfo, final String tenancyUID) {
        /*
         ** Obtain the fields required to build the Object table entry. Verify that the required fields are not missing.
         */
        String objectName = objectCreateInfo.getObject();
        String namespace = objectCreateInfo.getNamespace();
        String bucketName = objectCreateInfo.getBucket();

        if ((objectName == null) || (bucketName == null) || (namespace == null)) {
            String failureMessage = "\"getObjectId() missing required attributes\",\n  \"objectName\": \"" + objectName +
                    "\",\n  \"bucketName\": \"" + bucketName + "\",\n \"namespaceName\": \"" + namespace + "\"";
            LOG.warn(failureMessage);
            objectCreateInfo.setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
            return HttpStatus.BAD_REQUEST_400;
        }

        LOG.info("getObjectId() objectName: " + objectName + " bucketName: " + bucketName);

        /*
         ** First need to validate that the namespace exists
         */
        NamespaceTableMgr namespaceMgr = new NamespaceTableMgr(flavor);
        String namespaceUID = namespaceMgr.getNamespaceUID(namespace, tenancyUID);
        if (namespaceUID == null) {
            LOG.warn("Unable to retrieve Object ID: " + objectName + " - invalid namespace: " + namespace);

            String failureMessage = "\"Namespace not found\",\n  \"namespaceName\": \"" + namespace + "\"";
            objectCreateInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        /*
         ** Now make sure the Bucket that this Object is stored within actually exists.
         */
        BucketTableMgr bucketMgr = new BucketTableMgr(flavor, opcRequestId, objectCreateInfo);
        String bucketUID = bucketMgr.getBucketUID(bucketName, namespaceUID);
        if (bucketUID == null) {
            LOG.warn("Unable to retrieve Object ID: " + objectName + " - invalid bucket: " + bucketName);

            String failureMessage = "\"Bucket not found\",\n  \"bucketName\": \"" + bucketName + "\"";
            objectCreateInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

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

    public void deleteObject(final int objectId) {
        String deleteObjStr = DELETE_OBJECT_USING_ID + objectId;

        executeSqlStatement(deleteObjStr);
    }

    public void updateLastReadAccess(final int objectId) {
        LOG.info("updateLastReadAccess() objectId: " + objectId);
        String updateReadAccessStr = UPDATE_LAST_READ_ACCESS + objectId;

        executeSqlStatement(updateReadAccessStr);
    }

    private int getHighestVersionObject(final String objectName, final String bucketUID) {

        int objectId = -1;

        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;
            ResultSet rs = null;

            try {
                /*
                 ** Fields for the prepared statement are:
                 **   1 - objectName (String)
                 **   2 - bucketUID  (String)
                 */
                stmt = conn.prepareStatement(GET_HIGHEST_VERSION_OBJECT);
                stmt.setString(1, objectName);
                stmt.setString(2, bucketUID);
                rs = stmt.executeQuery();

            } catch (SQLException sqlEx) {
                LOG.error("getHighestVersionObject() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + GET_HIGHEST_VERSION_OBJECT);
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (rs != null) {
                    try {
                        int count = 0;
                        while (rs.next()) {
                            /*
                             ** The rs.getInt(1) is the objectId
                             */
                            objectId = rs.getInt(1);
                            String name = rs.getString(2);
                            int versionId = rs.getInt(3);

                            LOG.info("getHighestVersionObject() objectId: " + objectId + " objectName: " + name + " versionId: " + versionId);
                            count++;
                        }

                        if (count != 1) {
                            LOG.warn("getSingleStr() too many responses count: " + count);
                        }
                    } catch (SQLException sqlEx) {
                        System.out.println("getSingleStr() SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("getSingleStr() SQL conn rs.close() SQLException: " + sqlEx.getMessage());
                    }
                }

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

         return objectId;
    }

    /*
     ** This builds the OK_200 response headers for the Storage Server PUT Object command. This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     **   opc-content-md5
     **   ETag - Unique ID for this version of the object
     **   last-modified -
     **   version-id - There can be multiple versions of the same object present
     */
    private String buildSuccessHeader(final HttpRequestInfo objectCreateInfo, final String objectName, final String bucketUID) {
        String successHeader;

        int objectId = getObjectId(objectName, bucketUID);
        if (objectId != -1) {
            String createTime = getObjectCreationTime(objectId);
            String objectUID = getObjectUID(objectId);
            String opcClientId = objectCreateInfo.getOpcClientRequestId();
            String contentMD5 = requestContext.getMd5ResultHandler().getComputedMd5Digest();

            if (opcClientId != null) {
                successHeader = SUCCESS_HEADER_1 + opcClientId + "\n" + SUCCESS_HEADER_2 + opcRequestId + "\n" +
                        SUCCESS_HEADER_3 + contentMD5 + "\n" + SUCCESS_HEADER_4 + objectUID + "\n" +
                        SUCCESS_HEADER_5 + createTime + "\n";
            } else {
                successHeader = SUCCESS_HEADER_2 + opcRequestId + "\n" + SUCCESS_HEADER_3 + contentMD5 + "\n" +
                        SUCCESS_HEADER_4 + objectUID + "\n" + SUCCESS_HEADER_5 + createTime + "\n";
            }

            //LOG.info(successHeader);
        } else {
            successHeader = null;
        }

        return successHeader;
    }

}
