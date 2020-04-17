package com.webutils.webserver.mysql;

import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TenancyTableMgr extends ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(TenancyTableMgr.class);

    private final static String CREATE_TENANCY_1 = "INSERT INTO customerTenancy VALUES ( NULL, '";
    private final static String CREATE_TENANCY_2 = "', '";
    private final static String CREATE_TENANCY_3 = "', UUID_TO_BIN(UUID()) )";

    private final static String GET_TENANCY_UID_1 = "SELECT BIN_TO_UUID(tenancyUID) tenancyUID FROM customerTenancy WHERE tenancyName = '";
    private final static String GET_TENANCY_UID_2 = "' AND customerName = '";
    private final static String GET_TENANCY_UID_3 = "'";

    public TenancyTableMgr(final WebServerFlavor flavor) {
        super(flavor);
    }

    public boolean createTenancyEntry(final String customerName, final String tenancyName) {
        boolean success = true;

        /*
        ** First check if there is already a record for this Tenancy and Customer
         */
        String tenancyUID = getTenancyUID(customerName, tenancyName);
        if (tenancyUID != null) {
            LOG.warn("This Tenancy already exists. customer: " + customerName + " tenancyName: " + tenancyName);
            return true;
        }


        String createTenancyStr = CREATE_TENANCY_1 + tenancyName + CREATE_TENANCY_2 + customerName + CREATE_TENANCY_3;

        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(createTenancyStr);
            } catch (SQLException sqlEx) {
                LOG.error("createTenancyEntry() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + createTenancyStr);
                System.out.println("SQLException: " + sqlEx.getMessage());

                success = false;
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("createTenancyEntry() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeObjectStorageDbConn(conn);
        }

        return success;
    }

    /*
     ** This obtains the Tenancy UID. It will return NULL if the Tenancy does not exist.
     **
     ** NOTE: A Tenancy is unique when the tenancyName and CustomerName are combined. It is legal for multiple customers
     **   to use the same tenancyName.
     */
    public String getTenancyUID(final String customerName, final String tenancyName) {
        String getNamespaceUIDStr = GET_TENANCY_UID_1 + tenancyName + GET_TENANCY_UID_2 + customerName + GET_TENANCY_UID_3;

        return getUID(getNamespaceUIDStr);
    }

}
