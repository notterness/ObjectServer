package com.webutils.webserver.mysql;

import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class TenancyTableMgr extends AccessControlDb {

    private static final Logger LOG = LoggerFactory.getLogger(TenancyTableMgr.class);

    private final static String CREATE_TENANCY = "INSERT INTO CustomerTenancy VALUES ( NULL, ?, ?, UUID_TO_BIN(UUID()), " +
            " SHA2(?, 512), CURRENT_TIMESTAMP(), 0, NULL )";

    private final static String GET_TENANCY_UID_1 = "SELECT BIN_TO_UUID(tenancyUID) tenancyUID FROM CustomerTenancy WHERE tenancyName = '";
    private final static String GET_TENANCY_UID_2 = "' AND customerName = '";
    private final static String GET_TENANCY_UID_3 = "'";

    private final static String GET_TENANCY_UID = "SELECT BIN_TO_UUID(tenancyUID) tenancyUID FROM CustomerTenancy WHERE tenancyId = ";

    private final static String GET_TENANCY_ID_1 = "SELECT tenancyId FROM CustomerTenancy WHERE tenancyName = '";
    private final static String GET_TENANCY_ID_2 = "' AND customerName = '";
    private final static String GET_TENANCY_ID_3 = "'";

    private final static String GET_PASSPHRASE = "SELECT UNHEX(tenancyPassphrase) tenancyPassphrase FROM CustomerTenancy " +
            "WHERE customerName = ? AND tenancyName = ?";
    private final static String GET_TENANCY_PASSPHRASE = "SELECT UNHEX(tenancyPassphrase) tenancyPassphrase FROM CustomerTenancy " +
            "WHERE tenancyId = ";


    private int objectUniqueId;

    /*
    ** This is used to access various fields and information from the CustomerTenancy table
     */
    public TenancyTableMgr(final WebServerFlavor flavor) {
        super(flavor);
        this.objectUniqueId = -1;
    }

    public boolean createTenancyEntry(final String customerName, final String tenancyName, final String customerPassphrase) {
        boolean success = true;

        /*
        ** First check if there is already a record for this Tenancy and Customer
         */
        String tenancyUID = getTenancyUID(customerName, tenancyName);
        if (tenancyUID != null) {
            LOG.warn("This Tenancy already exists. customer: " + customerName + " tenancyName: " + tenancyName);
            return true;
        }


        Connection conn = getAccessControlDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                /*
               ** Fields to fill in are:
               **   1 - customerName
               **   2 - tenancyName
               **   3 - tenancyPassphrase (generated using the customerPassphrase)
                 */
                stmt = conn.prepareStatement(CREATE_TENANCY, Statement.RETURN_GENERATED_KEYS);
                stmt.setString(1, customerName);
                stmt.setString(2, tenancyName);
                stmt.setString(3, customerPassphrase);
                stmt.executeUpdate();

                ResultSet rs = stmt.getGeneratedKeys();
                if (rs.next()){
                    objectUniqueId = rs.getInt(1);
                }
                rs.close();
            } catch (SQLException sqlEx) {
                LOG.error("createTenancyEntry() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
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
            closeAccessControlDbConn(conn);
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
        String getTenancyUIDStr = GET_TENANCY_UID_1 + tenancyName + GET_TENANCY_UID_2 + customerName + GET_TENANCY_UID_3;

        return getUID(getTenancyUIDStr);
    }

    public String getTenancyUID(final int tenancyId) {
        String getTenancyUIDStr = GET_TENANCY_UID + tenancyId;

        return getUID(getTenancyUIDStr);
    }


    /*
     ** This obtains the tenancyId. It will return -1 if the Tenancy does not exist.
     **
     ** NOTE: A Tenancy is unique when the tenancyName and CustomerName are combined. It is legal for multiple customers
     **   to use the same tenancyName.
     */
    public int getTenancyId(final String customerName, final String tenancyName) {
        String getTenancyIdStr = GET_TENANCY_ID_1 + tenancyName + GET_TENANCY_ID_2 + customerName + GET_TENANCY_ID_3;

        return getId(getTenancyIdStr);
    }

    /*
    ** Obtain the tenancyPassphrase
     */
    public String getTenancyPassphrase(final String customerName, final String tenancyName) {
        String passphrase = null;

        Connection conn = getAccessControlDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.prepareStatement(GET_PASSPHRASE);
                stmt.setString(1, customerName);
                stmt.setString(2, tenancyName);
                rs = stmt.executeQuery();
            } catch (SQLException sqlEx) {
                LOG.error("getUID() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (rs != null) {
                    try {
                        int count = 0;
                        while (rs.next()) {
                            /*
                             ** The rs.getString(1) is the String format of the passphrase.
                             */
                            passphrase = rs.getString(1);
                            LOG.info("Requested Tenancy Passphrase: " + passphrase);

                            count++;
                        }

                        if (count != 1) {
                            passphrase = null;
                            LOG.warn("getTenancyPassphrase() incorrect response count: " + count);
                        }
                    } catch (SQLException sqlEx) {
                        System.out.println("getTenancyPassphrase() SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("getTenancyPassphrase() SQL conn rs.close() SQLException: " + sqlEx.getMessage());
                    }
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("getTenancyPassphrase() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeAccessControlDbConn(conn);
        }

        return passphrase;
    }

    public String getTenancyPassphrase(final int tenancyId) {
        String getTenancyPassphraseStr = GET_TENANCY_PASSPHRASE + tenancyId;

        return getSingleStr(getTenancyPassphraseStr);
    }

}
