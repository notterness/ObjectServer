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

    private final static String GET_OBJECT_ID_1 = "SELECT objectId FROM object WHERE objectName = '";
    private final static String GET_OBJECT_ID_2 = "' AND bucketId = ( SELECT bucketId FROM bucket WHERE bucketUID = UUID_TO_BIN('";
    private final static String GET_OBJECT_ID_3 = "' ) )";

    public ObjectTableMgr(final WebServerFlavor flavor) {
        super(flavor);
    }

    public int createObjectEntry(final HttpRequestInfo objectCreateInfo, final String tenancyUID) {
        int status = HttpStatus.OK_200;

        /*
         ** Obtain the fields required to build the Object table entry. Verify that the required fields are not missing.
         */
        String objectName = objectCreateInfo.getObject();
        String namespace = objectCreateInfo.getNamespace();
        String bucketName = objectCreateInfo.getBucket();
        String opcRequestId = objectCreateInfo.getOpcClientRequestId();
        int contentLength = objectCreateInfo.getContentLength();

        if ((objectName == null) || (bucketName == null) || (namespace == null)) {
            LOG.error("createObjectEntry() null required attributes objectName: " + objectName + "bucketName: " +
                    bucketName + " namespace: " + namespace);
            return HttpStatus.BAD_REQUEST_400;
        }

        /*
        ** First need to validate that the namespace exists
         */
        NamespaceTableMgr namespaceMgr = new NamespaceTableMgr(flavor);
        String namespaceUID = namespaceMgr.getNamespaceUID(namespace, tenancyUID);
        if (namespaceUID == null) {
            LOG.warn("Unable to create Object: " + objectName + " - invalid namespace: " + namespace);

            objectCreateInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, "Namespace not found :" + namespace);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        /*
        ** Now make sure the Bucket that this Object is stored within actually exists.
         */
        BucketTableMgr bucketMgr = new BucketTableMgr(flavor, objectCreateInfo);
        String bucketUID = bucketMgr.getBucketUID(bucketName, namespaceUID);
        if (bucketUID == null) {
            LOG.warn("Unable to create Object: " + objectName + " - invalid bucket: " + bucketName);

            objectCreateInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, "Bucket not found :" + bucketName);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        StorageTierEnum tier = bucketMgr.getBucketStorageTier(bucketUID);

        String createObjectStr;
        if (opcRequestId != null) {
            createObjectStr = CREATE_OBJECT_1 + objectName + CREATE_OBJECT_2 + opcRequestId + CREATE_OBJECT_3 +
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

                objectCreateInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, "SQL Exception");
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

    private int getObjectId(final String objectName, final String bucketUID) {
        String getObjectIdStr = GET_OBJECT_ID_1 + objectName + GET_OBJECT_ID_2 + bucketUID + GET_OBJECT_ID_3;

        return getId(getObjectIdStr);
    }

}
