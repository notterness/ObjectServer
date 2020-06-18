package com.webutils.webserver.mysql;

import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Objects;

public class UserTableMgr extends AccessControlDb {

    private static final Logger LOG = LoggerFactory.getLogger(UserTableMgr.class);

    private final static String CREATE_USER = "INSERT INTO TenancyUser VALUES ( NULL, AES_ENCRYPT(?, ?), AES_ENCRYPT(?, ?), UUID_TO_BIN(UUID()), ?," +
            " NULL, CURRENT_TIMESTAMP(), 0, NULL, 0, NULL, 0, NULL, ? )";

    private final static String SET_ACCESS_TOKEN = "UPDATE TenancyUser SET accessToken = MD5(?) WHERE userId = ?";

    private final static String GET_USER_UID = "SELECT BIN_TO_UUID(userUID) userUID FROM TenancyUser WHERE userId = ";

    private final static String GET_ACCESS_TOKEN = "SELECT HEX(accessToken) accessToken FROM TenancyUser WHERE userName = " +
            "AES_ENCRYPT(?, ?) AND password = AES_ENCRYPT(?, ?) AND tenancyId = ?";

    private final static String GET_TENANCY_FROM_TOKEN = "SELECT tenancyId FROM TenancyUser WHERE accessToken = UNHEX(?)";

    private final static String GET_USER_ID = "SELECT userId FROM TenancyUser WHERE userName = AES_ENCRYPT(?, ?) AND tenancyId = ?";

    private final static String GET_USER_NAME_AND_ID = "SELECT userName, tenancyId FROM TenancyUser WHERE accessToken = UNHEX(?)";

    private int objectUniqueId;

    /*
     ** This is used to access various fields and information from the TenancyUser+ table
     */
    public UserTableMgr(final WebServerFlavor flavor) {

        super(flavor);
        this.objectUniqueId = -1;
    }

    public int createTenancyUser(final HttpRequestInfo httpInfo) {

        String customerName = httpInfo.getCustomerName();
        String tenancyName = httpInfo.getTenancy();
        if ((customerName == null) || (tenancyName == null)) {
            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Unable to access Tenancy information\"" +
                    "\r\n  \"customer-name\": \"" + Objects.requireNonNullElse(customerName, "null") + "\"" +
                    "\r\n  \"tenancy-name\": \"" + Objects.requireNonNullElse(tenancyName, "null") + "\"" +
                    "\r\n}";
            httpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        TenancyTableMgr tenancyMgr = new TenancyTableMgr(flavor);
        int tenancyId = tenancyMgr.getTenancyId(customerName, tenancyName);

        String tenancyUID = tenancyMgr.getTenancyUID(tenancyId);
        String passphrase = tenancyMgr.getTenancyPassphrase(tenancyId);

        if ((tenancyUID == null) || (passphrase == null)) {
            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Unable to access Tenancy information\"" +
                    "\r\n}";
            httpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        String userName = httpInfo.getUserName();
        String password = httpInfo.getUserPassword();
        if ((userName == null) || (password == null)) {
            String tmp;
            if (password == null) {
                tmp = "null";
            } else {
                tmp = "****";
            }

            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Unable to create user account\"" +
                    "\r\n  \"user-name\": \"" + Objects.requireNonNullElse(userName, "null") + "\"" +
                    "\r\n  \"user-password\": \"" + tmp + "\"" +
                    "\r\n}";
            httpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        /*
         ** Check if this was already created
         */
        int userId = getUserId(userName, passphrase, tenancyId);
        if (userId != -1) {
            System.out.println("User already created");
            return HttpStatus.OK_200;
        }

        int accessRights = 0;

        /*
        ** The fields for the CREATE_USER are:
        **   1 - userName
        **   2 - passphrase
        **   3 - password
        **   4 - passphrase
        **   5 - accessRights
        **   6 - tenancyId
         */
        Connection conn = getAccessControlDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(CREATE_USER, Statement.RETURN_GENERATED_KEYS);
                stmt.setString(1, userName);
                stmt.setString(2, passphrase);
                stmt.setString(3, password);
                stmt.setString(4, passphrase);
                stmt.setInt(5, accessRights);
                stmt.setInt(6, tenancyId);
                stmt.executeUpdate();

                ResultSet rs = stmt.getGeneratedKeys();
                if (rs.next()){
                    objectUniqueId = rs.getInt(1);
                }
                rs.close();
            } catch (SQLException sqlEx) {
                LOG.error("createTenancyUser() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("createTenancyUser() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
            ** Now create the accessToken and save it away. The accessToken is made up from the following:
            **        MD5(userUID + "." + tenancyUID + "." + accessRights)
             */
            String userUID = getUserUID(objectUniqueId);
            if (userUID == null) {
                String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                        "\r\n  \"message\": \"Unable to create user account - userUID null\"" +
                        "\r\n}";
                httpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
                return HttpStatus.PRECONDITION_FAILED_412;
            }

            String accessToken = userUID + "." + tenancyUID + "." + accessRights;
            PreparedStatement updateStmt = null;

            try {
                /*
                 ** Fields to fill in are:
                 **   1 - accessToken String
                 **   2 - userId
                 */
                updateStmt = conn.prepareStatement(SET_ACCESS_TOKEN);
                updateStmt.setString(1, accessToken);
                updateStmt.setInt(2, objectUniqueId);
                updateStmt.executeUpdate();
            } catch (SQLException sqlEx) {
                LOG.error("createTenancyUser() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (updateStmt != null) {
                    try {
                        updateStmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("createTenancyUser() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the TenancyUser and then save the TenancyUser.accessKey.
             */
            closeAccessControlDbConn(conn);
        }

        return HttpStatus.OK_200;
    }

    public String getUserUID(final int userId) {
        String getUserUIDStr = GET_USER_UID + userId;

        return getUID(getUserUIDStr);
    }

    public String getAccessToken(final HttpRequestInfo httpInfo) {

        String customerName = httpInfo.getCustomerName();
        String tenancyName = httpInfo.getTenancy();
        if ((customerName == null) || (tenancyName == null)) {
            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Unable to access Tenancy information\"" +
                    "\r\n  \"customer-name\": \"" + Objects.requireNonNullElse(customerName, "null") + "\"" +
                    "\r\n  \"tenancy-name\": \"" + Objects.requireNonNullElse(tenancyName, "null") + "\"" +
                    "\r\n}";
            httpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return null;
        }

        TenancyTableMgr tenancyMgr = new TenancyTableMgr(flavor);
        int tenancyId = tenancyMgr.getTenancyId(customerName, tenancyName);

        String passphrase = tenancyMgr.getTenancyPassphrase(tenancyId);

        if (passphrase == null) {
            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Unable to access Tenancy information\"" +
                    "\r\n}";
            httpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return null;
        }

        String userName = httpInfo.getUserName();
        String password = httpInfo.getUserPassword();
        if ((userName == null) || (password == null)) {
            String tmp;
            if (password == null) {
                tmp = "null";
            } else {
                tmp = "****";
            }

            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Missing user account information\"" +
                    "\r\n  \"user-name\": \"" + Objects.requireNonNullElse(userName, "null") + "\"" +
                    "\r\n  \"user-password\": \"" + tmp + "\"" +
                    "\r\n}";
            httpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return null;
        }

        Connection conn = getAccessControlDbConn();

        String accessTokenStr = null;

        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                /*
                 ** The fields for the GET_ACCESS_TOKEN are:
                 **   1 - userName
                 **   2 - passphrase
                 **   3 - password
                 **   4 - passphrase
                 **   5 - tenancyId
                 */
                stmt = conn.prepareStatement(GET_ACCESS_TOKEN);
                stmt.setString(1, userName);
                stmt.setString(2, passphrase);
                stmt.setString(3, password);
                stmt.setString(4, passphrase);
                stmt.setInt(5, tenancyId);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    accessTokenStr = rs.getString(1);
                }
                rs.close();
            } catch (SQLException sqlEx) {
                LOG.error("getAccessToken() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("getAccessToken() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            closeAccessControlDbConn(conn);
        }

        return accessTokenStr;
    }

    public int getTenancyFromAccessToken(final String accessToken) {
        int tenancyId = -1;

        Connection conn = getAccessControlDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                /*
                 ** The fields for the GET_TENANCY_FROM_KEY are:
                 **   1 - accessToken
                 */
                stmt = conn.prepareStatement(GET_TENANCY_FROM_TOKEN);
                stmt.setString(1, accessToken);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    tenancyId = rs.getInt(1);
                }
                rs.close();
            } catch (SQLException sqlEx) {
                LOG.error("getTenancyFromAccessToken() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("getTenancyFromAccessToken() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            closeAccessControlDbConn(conn);
        }

        return tenancyId;
    }

    public int getUserId(final String userName, final String passphrase, final int tenancyId) {
        int userId = -1;

        Connection conn = getAccessControlDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                /*
                 ** The fields for the GET_USER_ID are:
                 **   1 - userName
                 **   2 - passphrase
                 **   3 - tenancyId
                 */
                stmt = conn.prepareStatement(GET_USER_ID);
                stmt.setString(1, userName);
                stmt.setString(2, passphrase);
                stmt.setInt(3, tenancyId);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    userId = rs.getInt(1);
                }
                rs.close();
            } catch (SQLException sqlEx) {
                LOG.error("getUserId() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("getUserId() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            closeAccessControlDbConn(conn);
        }

        return userId;
    }

    public String getCreatedByFromAccessToken(final String accessToken) {
        String createdBy = null;

        if (accessToken != null) {
            String userName = null;
            int tenancyId = -1;

            Connection conn = getAccessControlDbConn();

            if (conn != null) {
                PreparedStatement stmt = null;

                try {
                    /*
                     ** The fields for the GET_TENANCY_FROM_KEY are:
                     **   1 - accessToken
                     */
                    stmt = conn.prepareStatement(GET_USER_NAME_AND_ID);
                    stmt.setString(1, accessToken);
                    ResultSet rs = stmt.executeQuery();
                    if (rs.next()) {
                        userName = rs.getString(1);
                        tenancyId = rs.getInt(2);
                    }
                    rs.close();
                } catch (SQLException sqlEx) {
                    LOG.error("getTenancyFromAccessToken() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                    System.out.println("SQLException: " + sqlEx.getMessage());
                } finally {
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException sqlEx) {
                            LOG.error("getTenancyFromAccessToken() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                            System.out.println("SQLException: " + sqlEx.getMessage());
                        }
                    }
                }

                closeAccessControlDbConn(conn);
            }

            /*
             ** Now obtain the Tenancy name from the
             */
            if (userName != null) {
                TenancyTableMgr tenancyMgr = new TenancyTableMgr(flavor);

                String tenancyName = tenancyMgr.getTenancyName(tenancyId);
                if (tenancyName != null) {
                    createdBy = tenancyName + ".user." + userName;
                }
            } else {
                LOG.warn("Unable to locate user from accessToken");
            }
        } else {
            LOG.warn("accessToekn is null");
        }

        return createdBy;
    }


    private int validateFields(final HttpRequestInfo httpInfo) {

        String customerName = httpInfo.getCustomerName();
        String tenancyName = httpInfo.getTenancy();
        if ((customerName == null) || (tenancyName == null)) {
            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Unable to access Tenancy information\"" +
                    "\r\n  \"customer-name\": \"" + Objects.requireNonNullElse(customerName, "null") + "\"" +
                    "\r\n  \"tenancy-name\": \"" + Objects.requireNonNullElse(tenancyName, "null") + "\"" +
                    "\r\n}";
            httpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        TenancyTableMgr tenancyMgr = new TenancyTableMgr(flavor);
        int tenancyId = tenancyMgr.getTenancyId(customerName, tenancyName);

        String tenancyUID = tenancyMgr.getTenancyUID(tenancyId);
        String passphrase = tenancyMgr.getTenancyPassphrase(tenancyId);

        if ((tenancyUID == null) || (passphrase == null)) {
            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Unable to access Tenancy information\"" +
                    "\r\n}";
            httpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        String userName = httpInfo.getUserName();
        String password = httpInfo.getUserPassword();
        if ((userName == null) || (password == null)) {
            String tmp;
            if (password == null) {
                tmp = "null";
            } else {
                tmp = "****";
            }

            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Missing user account information\"" +
                    "\r\n  \"user-name\": \"" + Objects.requireNonNullElse(userName, "null") + "\"" +
                    "\r\n  \"user-password\": \"" + tmp + "\"" +
                    "\r\n}";
            httpInfo.setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            return HttpStatus.PRECONDITION_FAILED_412;
        }

        return HttpStatus.OK_200;
    }
}
