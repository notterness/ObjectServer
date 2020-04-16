package com.webutils.webserver.mysql;

import com.webutils.webserver.http.PostContentData;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class BucketTableMgr extends ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(BucketTableMgr.class);

    private final static String CREATE_BUCKET_1 = "INSERT INTO bucket VALUES ( NULL, '";
    private final static String CREATE_BUCKET_2 = "', '";
    private final static String CREATE_BUCKET_3 = "', ";
    private final static String CREATE_BUCKET_4 = ", UUID_TO_BIN(UUID()), (SELECT namespaceId FROM customerNamespace WHERE namespaceUID = UUID_TO_BIN('";
    private final static String CREATE_BUCKET_5 = "') ) )";

    private final static String GET_BUCKET_UID_1 = "SELECT BIN_TO_UUID(bucketUID) bucketUID FROM bucket WHERE bucketName = '";
    private final static String GET_BUCKET_UID_2 = "' AND namespaceId = ( SELECT namespaceId FROM customerNamespace WHERE namespaceUID = UUID_TO_BIN('";
    private final static String GET_BUCKET_UID_3 = "' ) )";

    private final static String GET_BUCKET_ID_1 = "SELECT bucketId FROM bucket WHERE bucketName = '";
    private final static String GET_BUCKET_ID_2 = "' AND namespaceId = ( SELECT namespaceId FROM customerNamespace WHERE namespaceUID = UUID_TO_BIN('";
    private final static String GET_BUCKET_ID_3 = "' ) )";

    public BucketTableMgr(final WebServerFlavor flavor) {
        super(flavor);
    }

    public boolean createBucketEntry(final PostContentData bucketConfigData, final String namespaceUID) {
        boolean success = true;

        /*
         ** Obtain the fields required to build the Bucket table entry
         */
        String bucketName = bucketConfigData.getBucketName();
        String compartmentId = bucketConfigData.getCompartmentId();
        int eventsEnabled = bucketConfigData.getObjectEventsEnabled();

        if ((bucketName == null) || (compartmentId == null)) {
            LOG.error("createBucketEntry() null required attributes");
            return false;
        }

        String createBucketStr = CREATE_BUCKET_1 + bucketName + CREATE_BUCKET_2 + compartmentId + CREATE_BUCKET_3 +
                eventsEnabled + CREATE_BUCKET_4 + namespaceUID + CREATE_BUCKET_5;

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

                success = false;
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
             ** Close out this connection as it was only used to create the database tables.
             */
            closeObjectStorageDbConn(conn);
        }

        /*
        ** Now fill in the Tag key value pair tables associated with this bucket.
         */
        if (success) {
            String bucketUID = getBucketUID(bucketName, namespaceUID);
            if (bucketUID != null) {
                LOG.info("bucketUID: " + bucketUID);
            }

            int id = getBucketId(bucketName, namespaceUID);
            if (id != -1) {
                BucketTagTableMgr tagMgr = new BucketTagTableMgr(flavor);

                tagMgr.createBucketTags(bucketConfigData, id);
            }
        }
        return success;
    }

    /*
     ** This obtains the Tenancy UID. It will return NULL if the Tenancy does not exist.
     **
     ** NOTE: A Tenancy is unique when the tenancyName and CustomerName are combined. It is legal for multiple customers
     **   to use the same tenancyName.
     */
    public String getBucketUID(final String bucketName, final String namespaceUID) {
        String getBucketUIDStr = GET_BUCKET_UID_1 + bucketName + GET_BUCKET_UID_2 + namespaceUID + GET_BUCKET_UID_3;

        return getUID(getBucketUIDStr);
    }

    private int getBucketId(final String bucketName, final String namespaceUID) {
        String getBucketIdStr = GET_BUCKET_ID_1 + bucketName + GET_BUCKET_ID_2 + namespaceUID + GET_BUCKET_ID_3;

        return getId(getBucketIdStr);
    }
}
