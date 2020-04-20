package com.webutils.webserver.mysql;

import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.PostContentData;
import com.webutils.webserver.http.StorageTierEnum;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BucketTableMgr extends ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(BucketTableMgr.class);

    /*
    ** FIXME: Need to account for the opc-client-request-id, currently set to NULL
     */
    private final static String CREATE_BUCKET_1 = "INSERT INTO bucket VALUES ( NULL, '";   // bucketName
    private final static String CREATE_BUCKET_2 = "', '";                                  // compartmentId
    private final static String CREATE_BUCKET_3 = "', '";                                  // storageTier
    private final static String CREATE_BUCKET_4 = "', ";                                   // objectEventsEnabled
    private final static String CREATE_BUCKET_5 = ", NULL, CURRENT_TIMESTAMP(), UUID_TO_BIN(UUID()), (SELECT namespaceId FROM customerNamespace WHERE namespaceUID = UUID_TO_BIN('";
    private final static String CREATE_BUCKET_6 = "') ) )";

    private final static String GET_BUCKET_UID_1 = "SELECT BIN_TO_UUID(bucketUID) bucketUID FROM bucket WHERE bucketName = '";
    private final static String GET_BUCKET_UID_2 = "' AND namespaceId = ( SELECT namespaceId FROM customerNamespace WHERE namespaceUID = UUID_TO_BIN('";
    private final static String GET_BUCKET_UID_3 = "' ) )";

    private final static String GET_BUCKET_UID_USING_ID = "SELECT bucketUID FROM bucket WHERE bucketId = ";

    private final static String GET_BUCKET_ID_1 = "SELECT bucketId FROM bucket WHERE bucketName = '";
    private final static String GET_BUCKET_ID_2 = "' AND namespaceId = ( SELECT namespaceId FROM customerNamespace WHERE namespaceUID = UUID_TO_BIN('";
    private final static String GET_BUCKET_ID_3 = "' ) )";

    private final static String GET_BUCKET_ID_FROM_UID_1 = "SELECT bucketId FROM bucket WHERE bucketUID = UUID_TO_BIN('";
    private final static String GET_BUCKET_ID_FROM_UID_2 = "' )";

    private final static String GET_BUCKET_STORAGE_TIER_1 = "SELECT storageTier FROM bucket WHERE bucketUID = UUID_TO_BIN('";
    private final static String GET_BUCKET_STORAGE_TIER_2 = "')";

    private final static String GET_BUCKET_CREATE_TIME = "SELECT createTime FROM bucket WHERE bucketID = ";

    private final static String GET_OPC_CLIENT_ID = "SELECT opcClientRequestId FROM bucket WHERE bucketId = ";

    private final static String SUCCESS_HEADER_1 = "opc-client-request-id: ";
    private final static String SUCCESS_HEADER_2 = "opc-request-id: ";
    private final static String SUCCESS_HEADER_3 = "ETag: ";
    private final static String SUCCESS_HEADER_4 = "Location: ";

    /*
    ** The opcRequestId is used to track the request through the system. It is uniquely generated for each
    **   request connection.
     */
    private final int opcRequestId;

    /*
    ** The HttpRequestInfo is needed to allow errors to be logged and passed back to the client
     */
    private final HttpRequestInfo httpRequestInfo;

    public BucketTableMgr(final WebServerFlavor flavor, final int requestId, final HttpRequestInfo requestInfo) {
        super(flavor);

        LOG.info("BucketTableMgr() flavor: " + flavor + " requestId: " + requestId);
        this.opcRequestId = requestId;
        this.httpRequestInfo = requestInfo;
    }

    public int createBucketEntry(final PostContentData bucketConfigData, final String namespaceUID) {
        int status = HttpStatus.OK_200;

        /*
         ** Obtain the fields required to build the Bucket table entry
         */
        String bucketName = bucketConfigData.getBucketName();

        /*
        ** Verify tht this bucket has not already been created
         */
        String bucketUID = getBucketUID(bucketName, namespaceUID);
        if (bucketUID != null) {
            /*
            ** Need to return a 409 return code (Conflict)
             */
            LOG.warn("Bucket already exists name: " + bucketName);

            String errorMessage = "CreateBucket bucket already exists name: " + bucketName + " UID: " + bucketUID;
            httpRequestInfo.setParseFailureCode(HttpStatus.CONFLICT_409, errorMessage);

            return HttpStatus.CONFLICT_409;
        }

        String compartmentId = bucketConfigData.getCompartmentId();
        String storageTier = bucketConfigData.getStorageTier();
        int eventsEnabled = bucketConfigData.getObjectEventsEnabled();

        if ((bucketName == null) || (compartmentId == null)) {
            LOG.error("createBucketEntry() null required attributes");

            httpRequestInfo.setParseFailureCode(HttpStatus.BAD_REQUEST_400, "CreateBucket missing attributes name: " + bucketName);
            return HttpStatus.BAD_REQUEST_400;
        }

        LOG.info("createBucket() bucketName: " + bucketName);

        String createBucketStr = CREATE_BUCKET_1 + bucketName + CREATE_BUCKET_2 + compartmentId + CREATE_BUCKET_3 +
                storageTier + CREATE_BUCKET_4 + eventsEnabled + CREATE_BUCKET_5 + namespaceUID + CREATE_BUCKET_6;

        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(createBucketStr);
            } catch (SQLException sqlEx) {
                LOG.error("createBucketEntry() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + createBucketStr);
                System.out.println("SQLException: " + sqlEx.getMessage());

                httpRequestInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, "SQL error");
                status = HttpStatus.INTERNAL_SERVER_ERROR_500;
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("createBucketEntry() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database table for the bucket.
             */
            closeObjectStorageDbConn(conn);
        }

        /*
        ** Now fill in the Tag key value pair tables associated with this bucket.
         */
        bucketUID = getBucketUID(bucketName, namespaceUID);
        if (status == HttpStatus.OK_200) {
            if (bucketUID != null) {
                LOG.info("bucketUID: " + bucketUID);

                int id = getBucketId(bucketName, namespaceUID);
                if (id != -1) {
                    BucketTagTableMgr tagMgr = new BucketTagTableMgr(flavor);

                    tagMgr.createBucketTags(bucketConfigData, id);
                }
            } else {
                httpRequestInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, "SQL error - unable to obtain bucket ETag");
                status = HttpStatus.INTERNAL_SERVER_ERROR_500;
            }
        }

        if (status == HttpStatus.OK_200) {
            httpRequestInfo.setResponseHeaders(buildSuccessHeader(httpRequestInfo, bucketUID));
        }

        return status;
    }

    /*
     ** This obtains the Bucket UID. It will return NULL if the Bucket does not exist.
     **
     ** NOTE: A Bucket is unique to a region and a namespace (there is one Object Storage namespace per region per
     **   tenancy).
     */
    public String getBucketUID(final String bucketName, final String namespaceUID) {
        String getBucketUIDStr = GET_BUCKET_UID_1 + bucketName + GET_BUCKET_UID_2 + namespaceUID + GET_BUCKET_UID_3;

        return getUID(getBucketUIDStr);
    }

    public String getBucketUID(final int bucketId) {
        String getBucketUIDStr = GET_BUCKET_UID_USING_ID + bucketId;

        return getUID(getBucketUIDStr);
    }

    private int getBucketId(final String bucketName, final String namespaceUID) {
        String getBucketIdStr = GET_BUCKET_ID_1 + bucketName + GET_BUCKET_ID_2 + namespaceUID + GET_BUCKET_ID_3;

        return getId(getBucketIdStr);
    }

    private int getBucketId(final String bucketUID) {
        String getBucketIdStr = GET_BUCKET_ID_FROM_UID_1 + bucketUID + GET_BUCKET_ID_FROM_UID_2;

        return getId(getBucketIdStr);
    }

    private String getBucketCreationTime(final int bucketId) {
        String getBucketCreateTime = GET_BUCKET_CREATE_TIME + bucketId;

        return getSingleStr(getBucketCreateTime);
    }

    private String getOpcClientRequestId(final int bucketId) {
       String getOpClientId = GET_OPC_CLIENT_ID + bucketId;

       return getSingleStr(getOpClientId);
    }

    public StorageTierEnum getBucketStorageTier(final String bucketUID) {
        String storageTierStr = null;

        String bucketStorageTierQuery = GET_BUCKET_STORAGE_TIER_1 + bucketUID + GET_BUCKET_STORAGE_TIER_2;

        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();
                if (stmt.execute(bucketStorageTierQuery)) {
                    rs = stmt.getResultSet();
                }
            } catch (SQLException sqlEx) {
                LOG.error("getBucketStorageTier() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + bucketStorageTierQuery);
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (rs != null) {
                    try {
                        int count = 0;
                        while (rs.next()) {
                            /*
                             ** The rs.getString(1) is the String format of the UID.
                             */
                            storageTierStr = rs.getString(1);
                            LOG.info("Requested storageTier: " + storageTierStr);

                            count++;
                        }

                        if (count != 1) {
                            storageTierStr = null;
                            LOG.warn("getBucketStorageTier() too many responses count: " + count);
                        }
                    } catch (SQLException sqlEx) {
                        System.out.println("getBucketStorageTier() SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("getBucketStorageTier() SQL conn rs.close() SQLException: " + sqlEx.getMessage());
                    }
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("getBucketStorageTier() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeObjectStorageDbConn(conn);
        }

        StorageTierEnum tier;

        if (storageTierStr != null) {
            tier = StorageTierEnum.fromString(storageTierStr);
        } else {
            tier = StorageTierEnum.INVALID_TIER;
        }

        return tier;
    }

    /*
    ** This builds the OK_200 response headers for the POST CreateBucket command. This returns the following headers:
    **
    **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
    **   opc-request-id
    **   ETag - This is the generated objectUID that is unique to this object
    **   Location - Full path to this bucket
     */
    private String buildSuccessHeader(final HttpRequestInfo objectCreateInfo, final String bucketUID) {
        String successHeader;

        int bucketId = getBucketId(bucketUID);
        if (bucketId != -1) {
            String opcClientId = objectCreateInfo.getOpcClientRequestId();

            /*
            ** FIXME: Need to add in the Location for the full path to the bucket
             */
            if (opcClientId != null) {
                successHeader = SUCCESS_HEADER_1 + opcClientId + "\n" + SUCCESS_HEADER_2 + opcRequestId + "\n" +
                        SUCCESS_HEADER_3 + bucketUID + "\n" + SUCCESS_HEADER_4 + "test" + "\n";
            } else {
                successHeader = SUCCESS_HEADER_2 + opcRequestId + "\n" + SUCCESS_HEADER_3 + bucketUID + "\n" +
                        SUCCESS_HEADER_4 + "test" + "\n";
            }
            LOG.info(successHeader);
        } else {
            successHeader = null;
        }

        return successHeader;
    }
}
