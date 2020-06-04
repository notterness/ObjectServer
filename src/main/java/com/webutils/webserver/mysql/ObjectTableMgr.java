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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObjectTableMgr extends ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectTableMgr.class);

    /*
    ** Fields for the prepared statement are:
    **   1 - objectName (String)
    **   2 - prefix (String, may be NULL)
    **   3 - versionId  (String)
    **   4 - opcClientRequestId (String, may be NULL)
    **   5 - contentLength (int)
    **   6 - storageType (int)
    **   7 - contentMd5  (String)
    **   8 - bucketId (int)
    **   9 - namespaceUID (String)
     */
    private final static String CREATE_OBJ_1 = "INSERT INTO object VALUES ( NULL, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(), NULL, 0, CURRENT_TIMESTAMP(), UUID_TO_BIN(UUID()), 0,";
    private final static String CREATE_OBJ_2 = " ?, (SELECT namespaceId FROM customerNamespace WHERE namespaceUID = UUID_TO_BIN(?)) )";

    private final static String GET_OBJECT_UID_1 = "SELECT BIN_TO_UUID(objectUID) objectUID FROM object WHERE objectName = '";
    private final static String GET_OBJECT_UID_2 = "' AND bucketId = ( SELECT bucketId FROM bucket WHERE bucketUID = UUID_TO_BIN('";
    private final static String GET_OBJECT_UID_3 = "' ) )";

    private final static String GET_OBJECT_UID_USING_ID = "SELECT BIN_TO_UUID(objectUID) objectUID FROM object WHERE objectId = ";

    private final static String GET_OBJECT_ID_1 = "SELECT objectId FROM object WHERE objectName = '";
    private final static String GET_OBJECT_ID_2 = "' AND bucketId = ";

    private final static String GET_OBJECT_CREATE_TIME = "SELECT createTime FROM object WHERE objectID = ";

    private final static String GET_OPC_CLIENT_ID = "SELECT opcClientRequestId FROM object WHERE objectId = ";

    private final static String SUCCESS_HEADER_1 = "opc-client-request-id: ";
    private final static String SUCCESS_HEADER_2 = "opc-request-id: ";
    private final static String SUCCESS_HEADER_3 = "opc-content-md5: ";
    private final static String SUCCESS_HEADER_4 = "ETag: ";
    private final static String SUCCESS_HEADER_5 = "last-modified: ";
    private final static String SUCCESS_HEADER_6 = "version-id: ";

    private final static String RETRIEVE_OBJECT_QUERY = "SELECT * FROM object WHERE objectId = ";

    private final static String DELETE_OBJECT_USING_ID = "DELETE FROM object WHERE objectId = ";

    private final static String UPDATE_LAST_READ_ACCESS = "UPDATE object SET readAccessCount = readAccessCount + 1, lastReadAccessTime = CURRENT_TIMESTAMP() WHERE objectId =";

    private final static String GET_HIGHEST_VERSION_OBJECT = "SELECT o1.objectId, o1.objectName, o1.versionId FROM object o1 " +
            "WHERE o1.versionId = ( SELECT MAX(o2.versionId) FROM object o2 WHERE o2.objectName = ? AND o2.bucketId = ? ) " +
            "AND o1.objectName = ?";

    private final static String GET_NUMBER_OF_OBJECTS = "SELECT COUNT(*) FROM object WHERE objectName = ? AND bucketId = ?";

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
    public int createObjectEntry(final HttpRequestInfo objectHttpInfo, final int tenancyId) {
        int status = HttpStatus.OK_200;

        /*
         ** Obtain the fields required to build the Object table entry. Verify that the required fields are not missing.
         */
        String objectName = objectHttpInfo.getObject();
        String opcClientRequestId = objectHttpInfo.getOpcClientRequestId();
        String prefix = null;
        int contentLength = objectHttpInfo.getContentLength();

        int bucketId = validateAndGetBucketId(objectHttpInfo, objectName, tenancyId);
        if (bucketId == -1) {
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        BucketTableMgr bucketMgr = new BucketTableMgr(flavor, opcRequestId, objectHttpInfo);
        StorageTierEnum tier = bucketMgr.getBucketStorageTier(bucketId);

        String namespace = objectHttpInfo.getNamespace();
        NamespaceTableMgr namespaceMgr = new NamespaceTableMgr(flavor);
        String namespaceUID = namespaceMgr.getNamespaceUID(namespace, tenancyId);

        /*
        ** Verify that the content md5 is a valid digest
         */
        String contentMd5 = objectHttpInfo.getContentMd5();
        if (!contentMd5.equals("NULL")) {
            byte[] md5DigestBytes = BaseEncoding.base64().decode(contentMd5);
            if (md5DigestBytes.length != 16) {
                LOG.warn("The value of the digest '" + contentMd5 + "' incorrect length after base-64 decoding");

                /*
                ** This should be an event
                 */
                String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                        "\r\n  \"message\": \"Incorrect Md5 Digest afdter base-64 decoding\"" +
                        "\r\n  \"Content-MD5\": \"" + contentMd5 + "\"" +
                        "\r\n}";
                objectHttpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);

                return HttpStatus.PRECONDITION_FAILED_412;
            }
        }

        /*
        ** Check if this object already exists and the "if-none-match header" is set to "*"
         */
        int versionId = 0;
        int objectCount = getObjectCount(objectName, bucketId);
        if (objectCount > 0) {
            boolean ifNoneMatch = objectHttpInfo.getIfNoneMatch();

            if (ifNoneMatch) {
                objectHttpInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, "\"Object already present\"");
                return HttpStatus.INTERNAL_SERVER_ERROR_500;
            } else {
                /*
                 ** Need to update the versionId
                 */
                versionId = getHighestVersionObject(objectName, bucketId, false);
            }
        }

        String createObjectStr = CREATE_OBJ_1 + CREATE_OBJ_2;

        Connection conn = getObjectStorageDbConn();

        int objectUniqueId = -1;

        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                /*
                 ** Fields for the prepared statement are:
                 **   1 - objectName (String)
                 **   2 - prefix (String, may be NULL)
                 **   3 - versionId  (String)
                 **   4 - opcClientRequestId (String, may be NULL)
                 **   5 - contentLength (int)
                 **   6 - storageType (int)
                 **   7 - contentMd5  (BINARY)
                 **   8 - bucketId (int)
                 **   9 - namespaceUID (String)
                 */
                stmt = conn.prepareStatement(createObjectStr, Statement.RETURN_GENERATED_KEYS);
                stmt.setString(1, objectName);

                LOG.info("createObjectEntry prefix: " + prefix);
                if (prefix != null) {
                    stmt.setString(2, prefix);
                } else {
                    stmt.setNull(2, Types.VARCHAR);
                }

                stmt.setString(3, Integer.toString(versionId));

                LOG.info("createObjectEntry opcClientRequestId: " + opcClientRequestId + " tier: " + tier.toInt());
                if (opcClientRequestId != null) {
                    stmt.setString(4, opcClientRequestId);
                } else {
                    stmt.setNull(4, Types.VARCHAR);
                }

                stmt.setInt(5, contentLength);
                stmt.setInt(6, tier.toInt());

                if (!contentMd5.equals("NULL")) {
                    byte[] md5DigestBytes = BaseEncoding.base64().decode(contentMd5);
                    stmt.setBytes(7, md5DigestBytes);
                } else {
                    stmt.setNull(7, Types.BINARY);
                }


                stmt.setInt(8, bucketId);
                stmt.setString(9, namespaceUID);
                stmt.executeUpdate();

                ResultSet rs = stmt.getGeneratedKeys();
                if (rs.next()){
                    objectUniqueId = rs.getInt(1);
                }
                rs.close();
            } catch (SQLException sqlEx) {
                LOG.error("createObjectEntry() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + createObjectStr);
                System.out.println("SQLException: " + sqlEx.getMessage());

                String failureMessage = "{\r\n  \"code\": \"" + HttpStatus.INTERNAL_SERVER_ERROR_500 + "\"" +
                        "\r\n  \"message\": \"SQL error: unable to create object - " + objectName + "\"" +
                        "\r\n}";
                objectHttpInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, failureMessage);

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
            LOG.info("createObjectEntry() objectId: " + objectUniqueId);

            if (objectUniqueId != -1) {
                String objectUID = getObjectUID(objectUniqueId);
                objectHttpInfo.setObjectId(objectUniqueId, objectUID);
                objectHttpInfo.setResponseHeaders(buildSuccessHeader(objectHttpInfo, objectUniqueId));
            } else {
                String failureMessage = "{\r\n  \"code\": \"" + HttpStatus.INTERNAL_SERVER_ERROR_500 + "\"" +
                        "\r\n  \"message\": \"SQL error: unable to obtain objectId - " + objectName + "\"" +
                        "\r\n}";
                objectHttpInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, failureMessage);

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
    public int retrieveObjectInfo(final HttpRequestInfo objectHttpInfo, final ObjectInfo info, final int tenancyId) {
        String objectName = info.getObjectName();

        int bucketId = validateAndGetBucketId(objectHttpInfo, objectName, tenancyId);
        if (bucketId == -1) {
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        LOG.info("retrieveObjectInfo() objectName: " + objectName);

        int objectId = getSpecifiedObject(objectName, bucketId, objectHttpInfo);
        if (objectId == -1) {
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        Connection conn = getObjectStorageDbConn();
        if (conn != null) {
            String queryStr = RETRIEVE_OBJECT_QUERY + objectId;
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();
                if (stmt.execute(queryStr)) {
                    rs = stmt.getResultSet();
                }
            } catch (SQLException sqlEx) {
                LOG.error("retrieveObjectInfo() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
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
                            boolean deleteMarker = rs.getBoolean("deleteMarker");
                            if (!deleteMarker) {
                                if (count == 0) {
                                    /*
                                     ** objectId is the unique identifier for the object
                                     */
                                    objectId = rs.getInt(1);
                                    info.setObjectId(objectId);

                                    /*
                                     ** versionId - field 4
                                     */
                                    String versionId = rs.getString(4);
                                    info.setVersionId(versionId);

                                    /*
                                     ** Field 6 (INT) - contentLength
                                     */
                                    info.setContentLength(rs.getInt(6));

                                    /*
                                     ** Field 7 (BINARY(16)) - contentMd5
                                     */
                                    byte[] md5Bytes = rs.getBytes(8);
                                    if (!rs.wasNull() && (md5Bytes != null)) {
                                        String md5DigestStr = BaseEncoding.base64().encode(md5Bytes);
                                        info.setContentMd5(md5DigestStr);
                                    } else {
                                        info.setContentMd5(null);
                                    }

                                    /*
                                     ** Field 12 - lastUpdateTime
                                     */
                                    String lastModifiedTime = rs.getString(12);
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
                LOG.warn("Object has no data associated with it: " + objectName);

                String failureMessage = "{\r\n  \"code\": \"" + HttpStatus.PRECONDITION_FAILED_412 + "\"" +
                        "\r\n  \"message\": \"Object data not found - " + objectName + "\"" +
                        "\r\n}";
                objectHttpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
                status = HttpStatus.PRECONDITION_FAILED_412;
            }
        } else {
            LOG.warn("Object not found: " + objectName);

            String failureMessage = "{\r\n  \"code\": \"" + HttpStatus.PRECONDITION_FAILED_412 + "\"" +
                    "\r\n  \"message\": \"Object not found - " + objectName + "\"" +
                    "\r\n}";
            objectHttpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            status = HttpStatus.PRECONDITION_FAILED_412;
        }

        return status;
    }

    /*
     ** This retrieves the information needed to provide the information for the ListObject method.
     **
     */
    public void retrieveMappedObjectInfo(final String queryStr, final List<Map<String, String>> objectList) {

        Connection conn = getObjectStorageDbConn();
        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();
                if (stmt.execute(queryStr)) {
                    rs = stmt.getResultSet();
                }
            } catch (SQLException sqlEx) {
                LOG.error("retrieveMappedObjectInfo() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + queryStr);
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (rs != null) {
                    try {
                        while (rs.next()) {
                            /*
                             ** Check if any of the records for the objects are in the process of being deleted.
                             */
                            boolean deleteMarker = rs.getBoolean("deleteMarker");
                            if (!deleteMarker) {
                                HashMap<String, String> objectDetails = new HashMap<>();

                                /*
                                ** objectId is the unique identifier for the object
                                 */
                                objectDetails.put("unique-id", rs.getString("objectId"));

                                /*
                                ** objectName
                                 */
                                objectDetails.put("name", rs.getString("objectName"));

                                /*
                                ** versionId - field 3
                                 */
                                objectDetails.put("version", rs.getString("versionId"));

                                /*
                                ** Field 5 (INT) - contentLength
                                 */
                                objectDetails.put("size", rs.getString("contentLength"));

                                /*
                                ** Field 6 (INT) - storageType enum
                                 */
                                int storageType = rs.getInt("storageType");
                                String storageTier = StorageTierEnum.fromInt(storageType).toString();
                                objectDetails.put("tier", storageTier);

                                /*
                                ** Field 6 (BINARY(16)) - contentMd5
                                */
                                byte[] md5Bytes = rs.getBytes("contentMd5");
                                if (!rs.wasNull() && (md5Bytes != null)) {
                                    String md5DigestStr = BaseEncoding.base64().encode(md5Bytes);
                                    objectDetails.put("md5", md5DigestStr);
                                }

                                /*
                                ** Field 7 (TIMESTAMP) - createTime
                                 */
                                objectDetails.put("time-created", rs.getString("createTime"));

                                /*
                                ** Field 11 - lastUpdateTime
                                 */
                                objectDetails.put("last-update", rs.getString("lastUpdateTime"));

                                /*
                                ** Field 12 (BINARY) - objectUID (etag)
                                 */
                                objectDetails.put("etag", rs.getString("objectUID"));

                                objectList.add(objectDetails);
                            }
                        }

                    } catch (SQLException sqlEx) {
                        LOG.error("retrieveMappedObjectInfo() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("retrieveObjectInfo() SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("retrieveMappedObjectInfo() rs close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("retrieveObjectInfo() SQL conn rs.close() SQLException: " + sqlEx.getMessage());
                    }
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("retrieveMappedObjectInfo() stmt close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeObjectStorageDbConn(conn);
        }
    }

    public void deleteObjectInfo(final HttpRequestInfo objectHttpInfo, final int tenancyId) {
        /*
         ** Obtain the fields required to build the Object table entry. Verify that the required fields are not missing.
         */
        String objectName = objectHttpInfo.getObject();

        int bucketId = validateAndGetBucketId(objectHttpInfo, objectName, tenancyId);
        if (bucketId == -1) {
            return;
        }

        LOG.info("deleteObjectInfo() objectName: " + objectName);

        int objectId = getSpecifiedObject(objectName, bucketId, objectHttpInfo);
        if (objectId == -1) {
            return;
        }

        /*
        ** Delete all the chunk information associated with this object
         */
        StorageChunkTableMgr chunkTableMgr = new StorageChunkTableMgr(flavor, objectHttpInfo);
        chunkTableMgr.deleteChunksFromObject(objectId);

        /*
        ** Delete the object
         */
        deleteObject(objectId);
    }

    /*
     ** This obtains the Bucket UID. It will return NULL if the Bucket does not exist.
     **
     ** NOTE: A Bucket is unique to a region and a namespace (there is one Object Storage namespace per region per
     **   tenancy).
     */
    private String getObjectUID(final String objectName, final String bucketUID) {
        String getObjectUIDStr = GET_OBJECT_UID_1 + objectName + GET_OBJECT_UID_2 + bucketUID + GET_OBJECT_UID_3;

        String uid = getUID(getObjectUIDStr);
        if (uid == null) {
            LOG.warn("getObjectUID(1) objectUID is null");
        }

        return uid;
    }

    public String getObjectUID(final int objectId) {
        String getObjectUIDStr = GET_OBJECT_UID_USING_ID + objectId;

        String uid = getUID(getObjectUIDStr);
        if (uid == null) {
            LOG.warn("getObjectUID(2) objectUID is null");
        }

        return uid;
    }


    private int getObjectId(final String objectName, final int bucketId) {
        String getObjectIdStr = GET_OBJECT_ID_1 + objectName + GET_OBJECT_ID_2 + bucketId;

        int id = getId(getObjectIdStr);
        if (id == -1) {
            LOG.warn("getObjectId(2) objectId is -1");
        }

        return id;
    }


    /*
    ** The full way to obtain unique identifier for an Object. This will return -1 is there are multiple object records
    **   that use the same name (i.e. multiple versions of the same object).
     */
    private int getObjectId(final HttpRequestInfo objectHttpInfo, final int tenancyId) {
        /*
         ** Obtain the fields required to build the Object table entry. Verify that the required fields are not missing.
         */
        String objectName = objectHttpInfo.getObject();


        int bucketId = validateAndGetBucketId(objectHttpInfo, objectName, tenancyId);
        if (bucketId == -1) {
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        LOG.info("getObjectId() objectName: " + objectName);

        String getObjectIdStr = GET_OBJECT_ID_1 + objectName + GET_OBJECT_ID_2 + bucketId;

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

    /*
    ** This will return the objectId of the specified (by Object Name and bucketUID) object with the highest versionId
    **   if the returnObjectId boolean is set to true.
    ** If the returnObjectId boolean is set to false, it returns the next versionId to use when creating a new object.
     */
    private int getHighestVersionObject(final String objectName, final int bucketId, final boolean returnObjectId) {

        int objectId = -1;
        int versionId = 0;

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
                stmt.setInt(2, bucketId);
                stmt.setString(3, objectName);
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
                            String versionIdStr = rs.getString(3);
                            try {
                                versionId = Integer.parseInt(versionIdStr);
                                versionId++;
                            } catch (NumberFormatException ex) {
                                LOG.warn("Invalid versionId: " + versionIdStr + " to convert to int");
                            }

                            LOG.info("getHighestVersionObject() objectId: " + objectId + " objectName: " + name + " versionId: " + versionId);
                            count++;
                        }

                        if (count != 1) {
                            LOG.warn("getHighestVersionObject() too many responses count: " + count);
                        }
                    } catch (SQLException sqlEx) {
                        System.out.println("getHighestVersionObject() SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("getHighestVersionObject() SQL conn rs.close() SQLException: " + sqlEx.getMessage());
                    }
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("getHighestVersionObject() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeObjectStorageDbConn(conn);
        }

        if (returnObjectId) {
            return objectId;
        } else {
            return versionId;
        }
    }

    private int getObjectCount(final String objectName, final int bucketId) {

        int objectCount = 0;

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
                stmt = conn.prepareStatement(GET_NUMBER_OF_OBJECTS);
                stmt.setString(1, objectName);
                stmt.setInt(2, bucketId);
                rs = stmt.executeQuery();

            } catch (SQLException sqlEx) {
                LOG.error("getObjectCount() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
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
                            objectCount = rs.getInt(1);
                            count++;
                        }

                        if (count != 1) {
                            LOG.warn("getObjectCount() too many responses count: " + count);
                        }
                    } catch (SQLException sqlEx) {
                        System.out.println("getObjectCount() SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("getObjectCount() SQL conn rs.close() SQLException: " + sqlEx.getMessage());
                    }
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("getObjectCount() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeObjectStorageDbConn(conn);
        }

        return objectCount;
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
    private String buildSuccessHeader(final HttpRequestInfo objectCreateInfo, final int objectId) {
        String successHeader;

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

        return successHeader;
    }

    /*
    ** This is used to pick a specific object version to return. The criteria for picking the object are as follows:
    **
    **   1) If the "if-match" header is present, then use the ETag passed in to obtain the requested object
    **   2) If the "if-match" is missing and the "versionId" header is set, use the versionId to obtain the
    **      requested object.
    **   3) If neither the "if-match" and "versionId" headers are present, return the highest version of the object.
     */
    private int getSpecifiedObject(final String objectName, final int bucketId, final HttpRequestInfo objectHttpInfo) {
        String ifMatch = objectHttpInfo.getIfMatchUid();
        int objectVersionId = objectHttpInfo.getVersionId();
        int objectId;

        if (ifMatch != null) {
            String queryStr = "SELECT objectId FROM object WHERE objectUID = UUID_TO_BIN('" + ifMatch + "')";
            objectId = getId(queryStr);

            if (objectId == -1) {
                LOG.warn("Unable to find Object: " + objectName + " if-match: " + ifMatch);

                String failureMessage = "{\r\n  \"code\": \"" + HttpStatus.PRECONDITION_FAILED_412 + "\"" +
                        "\r\n  \"message\": \"Object not found - " + objectName + "\"" +
                        "\r\n  \"if-match\": \"" + ifMatch + "\"" +
                        "\r\n}";
                objectHttpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            }
        } else if (objectVersionId != -1) {
            String queryStr = "SELECT objectId FROM object WHERE objectName = '" + objectName + "' AND bucketId = " +
                    bucketId + " AND versionId = '" + objectVersionId + "'";
            objectId = getId(queryStr);

            if (objectId == -1) {
                LOG.warn("Unable to find Object: " + objectName + " versionId: " + objectVersionId);

                String failureMessage = "{\r\n  \"code\": \"" + HttpStatus.PRECONDITION_FAILED_412 + "\"" +
                        "\r\n  \"message\": \"Object not found - " + objectName + "\"" +
                        "\r\n  \"versionId\": \"" + objectVersionId + "\"" +
                        "\r\n}";
                objectHttpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            }
        } else {
            objectId = getHighestVersionObject(objectName, bucketId, true);
            if (objectId == -1) {
                LOG.warn("Unable to find Object: " + objectName);

                String failureMessage = "{\r\n  \"code\": \"" + HttpStatus.PRECONDITION_FAILED_412 + "\"" +
                        "\r\n  \"message\": \"Object not found - " + objectName + "\"" +
                        "\r\n}";
                objectHttpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            }
        }

        return objectId;
    }

    /*
    ** This consolidates some basic checking required of parameters:
    **    tenancyUID - The identifier of the Tenancy that the namespace must reside within.
    **    namespace - Required to determine that the bucket is valid and it is associated with the namespace
    **    bucketName - The bucket must reside within the namespace and the object must reside within the bucket.
     */
    private int validateAndGetBucketId(final HttpRequestInfo objectInfo, final String objectName, final int tenancyId) {

        String namespace = objectInfo.getNamespace();
        String bucketName = objectInfo.getBucket();

        if ((objectName == null) || (bucketName == null) || (namespace == null)) {
            String failureMessage = "{\r\n  \"code\":" + HttpStatus.BAD_REQUEST_400 +
                    "\r\n  \"message\": \"Object missing required attributes\" + " +
                    "\r\n  \"objectName\": \"" + objectName + "\"" +
                    "\r\n  \"bucketName\": \"" + bucketName + "\"" +
                    "\r\n \"namespaceName\": \"" + namespace + "\"" +
                    "\r\n}";
            LOG.warn(failureMessage);
            objectInfo.setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
            return -1;
        }

        LOG.info("createObjectEntry() objectName: " + objectName + " bucketName: " + bucketName);

        /*
         ** First need to validate that the namespace exists
         */
        NamespaceTableMgr namespaceMgr = new NamespaceTableMgr(flavor);
        String namespaceUID = namespaceMgr.getNamespaceUID(namespace, tenancyId);
        if (namespaceUID == null) {
            LOG.warn("Unable to create Object: " + objectName + " - invalid namespace: " + namespace);

            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Namespace not found\"" +
                    "\r\n  \"namespaceName\": \"" + namespace + "\"" +
                    "\r\n}";
            objectInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return -1;
        }

        /*
         ** Now make sure the Bucket that this Object is stored within actually exists.
         */
        BucketTableMgr bucketMgr = new BucketTableMgr(flavor, opcRequestId, objectInfo);
        int bucketId = bucketMgr.getBucketId(bucketName, namespaceUID);
        if (bucketId == -1) {
            LOG.warn("Unable to create Object: " + objectName + " - invalid bucket: " + bucketName);

            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Bucket not found\"" +
                    "\r\n  \"bucketName\": \"" + bucketName + "\"" +
                    "\r\n}";
            objectInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
        }

        return bucketId;
    }
}
