package com.webutils.webserver.mysql;

import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class AccessControlDb {

    private static final Logger LOG = LoggerFactory.getLogger(AccessControlDb.class);

    private static final String userName = "root";
    private static final String password = "ktm300exc";

    private static final String ACCESS_CONTROL_DB_NAME = "AccessControlDb";

    private static final String ACCESS_DB_USER = "AccessDbUser";
    private static final String ACCESS_DB_PASSWORD = "rwt25nX1";

    /*
     ** This is the Tenancy table. A tenancy is unique per customer, but the tenancyName does not have to be unique
     **   across all customers.
     ** The tenancyPassphrase is generated using the SHA2('user provided passphrase', 512). This passphrase is what is
     **   then used by the AES_ENCRYPT() and AES_DECRYPT() function to secure the user names and passwords.
     */
    private static final String CREATE_TENANCY_TABLE = "CREATE TABLE IF NOT EXISTS CustomerTenancy (" +
            " tenancyId INT AUTO_INCREMENT, " +
            " customerName VARCHAR(255) NOT NULL, " +
            " tenancyName VARCHAR(255) NOT NULL, " +
            " tenancyUID BINARY(16) NOT NULL, " +
            " tenancyPassphrase BINARY(128) NOT NULL, " +
            " createdTime TIMESTAMP NOT NULL, " +
            " disabled BOOL NOT NULL, " +
            " disabledTime TIMESTAMP, " +
            " PRIMARY KEY (tenancyName)," +
            " KEY (tenancyId)" +
            ")";

    /*
    ** The accessToken is used by the client to access the Object Server. It is passed in as one of the headers for
    **   all of the methods supported by the Object Server. The accessToken validates the user and is used to determine
    **   the Tenancy that is being accessed. The accessToken is made up from the following:
    *        MD5(userUID + "." + tenancyUID + "." + accessRights)
     */
    private static final String CREATE_TENANCY_USER_TABLE = "CREATE TABLE IF NOT EXISTS TenancyUser (" +
            " userId INT AUTO_INCREMENT, " +
            " userName VARBINARY(256) NOT NULL, " +
            " password VARBINARY(256) NOT NULL, " +
            " userUID BINARY(16) NOT NULL, " +
            " accessRights INT NOT NULL, "  +
            " accessToken BINARY(32), " +
            " accountCreated TIMESTAMP NOT NULL, " +
            " disabled BOOL NOT NULL, " +
            " disabledTime TIMESTAMP, " +
            " deleted BOOL NOT NULL, " +
            " deletedTime TIMESTAMP, " +
            " failedLoginCount INT NOT NULL, " +
            " firstFailedLogin TIMESTAMP, " +
            " tenancyId INT NOT NULL, " +
            " FOREIGN KEY (tenancyId)" +
            "   REFERENCES CustomerTenancy(tenancyId)" +
            "   ON DELETE CASCADE, " +
            " PRIMARY KEY (userId)" +
            ")";



    private static final String CREATE_ACCESS_DB_USER = "CREATE USER '" + ACCESS_DB_USER + "'@'localhost'" +
            " IDENTIFIED BY '" + ACCESS_DB_PASSWORD + "'";

    private static final String PRIVILEGE_SERVICE_SERVERS_USER = "GRANT ALL PRIVILEGES ON " + ACCESS_CONTROL_DB_NAME +
            ".* TO '" + ACCESS_DB_USER + "'@'localhost'";

    private static final String DROP_ACCESS_CONTROL_USER = "DROP USER '" + ACCESS_DB_USER + "'@'localhost'";
    private static final String DROP_ACCESS_CONTROL_DB = "DROP DATABASE " + ACCESS_CONTROL_DB_NAME;

    private static final String kubeJDBCDatabaseConnect = "jdbc:mysql://host.docker.internal/AccessControlDb?serverTimeZone=US/Mountain";
    private static final String execJDBCDatabaseConnect = "jdbc:mysql://localhost/AccessControlDb?serverTimezone=US/Mountain";
    private static final String kubeJDBCConnect = "jdbc:mysql://host.docker.internal/?serverTimeZone=US/Mountain";
    private static final String execJDBCConnect = "jdbc:mysql://localhost/?serverTimezone=US/Mountain";

    /*
     ** Is this running within a Docker Container?
     */
    protected final WebServerFlavor flavor;

    public AccessControlDb(final WebServerFlavor flavor) {
        LOG.info("AccessControlDb() WebServerFlavor: " + flavor.toString());
        this.flavor = flavor;
    }

    /*
     ** INTEGRATION_KUBERNETES_TESTS is used by the Client Tests when running within a Docker container and accessing
     **   the Object Server and mock Storage Servers that are running in a Kubernetes POD.
     */
    public boolean isDockerImage() {
        return ((flavor == WebServerFlavor.DOCKER_OBJECT_SERVER_TEST) ||
                (flavor == WebServerFlavor.DOCKER_STORAGE_SERVER_TEST) ||
                (flavor == WebServerFlavor.INTEGRATION_KUBERNETES_TESTS));
    }

    public boolean isKubernetesImage() {
        return ((flavor == WebServerFlavor.KUBERNETES_OBJECT_SERVER_TEST) ||
                (flavor == WebServerFlavor.KUBERNETES_STORAGE_SERVER_TEST));
    }

    /*
     */
    public void checkAndSetupAccessControls() {

        /*
         */
        if (createAccessControlDb()) {
            createAccessControlTables();
        }
    }

    /*
     ** This is used to delete the AccessControlDb and its users
     */
    public void dropDatabase() {
        Connection conn = null;
        int vendorError;
        String jdbcConnect;

        if (isDockerImage() || isKubernetesImage()) {
            jdbcConnect = kubeJDBCConnect;
        } else {
            jdbcConnect = execJDBCConnect;
        }

        try {
            conn = DriverManager.getConnection(jdbcConnect, userName, password);
        } catch (SQLException ex) {
            vendorError = ex.getErrorCode();
            System.out.println("dropDatabase(2) - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState() +
                    " " + vendorError);
            LOG.error("dropDatabase(2) - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState() + " " + vendorError);
        }

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(DROP_ACCESS_CONTROL_USER);

                stmt.execute(DROP_ACCESS_CONTROL_DB);
            } catch (SQLException sqlEx) {
                System.out.println("dropDatabase() - SQLException: " + sqlEx.getMessage() + " vendorError: " + sqlEx.getErrorCode());
                LOG.error("dropDatabase() - SQLException: " + sqlEx.getMessage() + " vendorError: " + sqlEx.getErrorCode());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("dropDatabase() - close - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        LOG.error("dropDatabase() - close - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database.
             */
            try {
                conn.close();
            } catch (SQLException sqlEx) {
                // handle any errors
                System.out.println("SQL conn close(2) SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("SQL conn close(2) SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
            }
        }

        LOG.info("dropDatabase() AccessControlDb database dropped");
    }


    /*
     ** This obtains a connection to communicate with the MySQL Access Control database.
     */
    public Connection getAccessControlDbConn() {
        Connection conn;
        String jdbcConnect;

        if (isDockerImage() || isKubernetesImage()) {
            jdbcConnect = kubeJDBCDatabaseConnect;
        } else {
            jdbcConnect = execJDBCDatabaseConnect;
        }

        try {
            conn = DriverManager.getConnection(jdbcConnect, ACCESS_DB_USER, ACCESS_DB_PASSWORD);
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("JDBC connect: " + jdbcConnect);
            System.out.println("getAccessControlDbConn() - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());
            LOG.error("getAccessControlDbConn() - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());
            return null;
        }

        return conn;
    }

    /*
     ** This closes the connection used to communicate with the MySql Access Control database.
     */
    public void closeAccessControlDbConn(final Connection conn) {

        /*
         ** Add safety check in case this was called when the Connection was not actually
         **   created in the first place.
         */
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException sqlEx) {
                // handle any errors
                System.out.println("closeAccessControlDbConn() - SQL conn close(2) SQLException: " + sqlEx.getMessage() + "  SQLState: " + sqlEx.getSQLState());
                LOG.error("closeAccessControlDbConn() - SQL conn close(2) SQLException: " + sqlEx.getMessage() + "  SQLState: " + sqlEx.getSQLState());
            }
        }
    }

    /*
     ** This creates the database used to hold the Tenancy Information and the User Accounts if the database does
     **   not already exist.
     */
    private boolean createAccessControlDb() {
        Connection conn;
        int vendorError;
        String jdbcConnect;

        if (isDockerImage() || isKubernetesImage()) {
            jdbcConnect = kubeJDBCDatabaseConnect;
        } else {
            jdbcConnect = execJDBCDatabaseConnect;
        }

        try {
            conn = DriverManager.getConnection(jdbcConnect, userName, password);

            /*
             ** Not going to use this connection, it is just to check if the database exists and can be
             **   connected to.
             */
            try {
                conn.close();
            } catch (SQLException sqlEx) {
                // handle any errors
                System.out.println("createAccessControlDb() - SQL conn close(1) SQLException: " + sqlEx.getMessage());
                LOG.error("createAccessControlDb() - SQL conn close(1) SQLException: " + sqlEx.getMessage());
            }

            System.out.println("createAccessControlDb() database found");

            return false;
        } catch (SQLException ex) {
            // handle any errors

            vendorError = ex.getErrorCode();
            if (vendorError != MysqlErrorNumbers.ER_BAD_DB_ERROR) {
                System.out.println("createAccessControlDb(1) - JDBC connect: " + jdbcConnect);
                System.out.println("createAccessControlDb(1) - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());
                LOG.error("createAccessControlDb(1) - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());

                return false;
            }
        }

        /*
         ** If the code reaches here, the AccessControlDb database needs to be created
         */
        System.out.println("Create AccessControlDb");

        if (isDockerImage() || isKubernetesImage()) {
            jdbcConnect = kubeJDBCConnect;
        } else {
            jdbcConnect = execJDBCConnect;
        }

        try {
            conn = DriverManager.getConnection(jdbcConnect, userName, password);
        } catch (SQLException ex) {
            vendorError = ex.getErrorCode();
            System.out.println("createAccessControlDb(2) - JDBC connect: " + jdbcConnect);
            System.out.println("createAccessControlDb(2) - creating database - SQLException: " + ex.getMessage() +
                    " SQLState: " + ex.getSQLState() + " " + vendorError);
            LOG.error("createAccessControlDb(2) - creating database - SQLException: " + ex.getMessage() +
                    " SQLState: " + ex.getSQLState() + " " + vendorError);
            return false;
        }

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute("CREATE DATABASE " + ACCESS_CONTROL_DB_NAME);

                stmt.execute(CREATE_ACCESS_DB_USER);
                stmt.execute(PRIVILEGE_SERVICE_SERVERS_USER);
            } catch (SQLException sqlEx) {
                System.out.println("createAccessControlDb() - create database - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("createAccessControlDb() - create database - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("createAccessControlDb() - close - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        LOG.error("createAccessControlDb() - close - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database.
             */
            try {
                conn.close();
            } catch (SQLException sqlEx) {
                // handle any errors
                System.out.println("SQL conn close(2) SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("SQL conn close(2) SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
            }
        }

        LOG.info("AccessControlDb database created");
        return true;
    }

    /*
     ** This performs an initial setup of the tables associated with the Servers if they do not already
     **   exist (meaning they are created if the database did not exist).
     */
    private void createAccessControlTables() {

        LOG.info("createAccessControlTables()");

        /*
        ** Build a list of tables to create so it can be done within a loop
         */
        List<String> tableCreates = new LinkedList<>();
        tableCreates.add(CREATE_TENANCY_TABLE);
        tableCreates.add(CREATE_TENANCY_USER_TABLE);

        Connection conn = getAccessControlDbConn();

        if (conn != null) {
            Statement stmt = null;

            for (String createTableStr : tableCreates) {

                try {
                    stmt = conn.createStatement();
                    stmt.execute(createTableStr);
                } catch (SQLException sqlEx) {
                    LOG.error("createAccessControlTables() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                    LOG.error("Bad SQL command: " + createTableStr);
                    System.out.println("SQLException: " + sqlEx.getMessage());
                } finally {
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException sqlEx) {
                            LOG.error("createAccessControlTables() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                            System.out.println("SQLException: " + sqlEx.getMessage());
                        }

                        stmt = null;
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeAccessControlDbConn(conn);
        }

        tableCreates.clear();
    }



    /*
     ** This obtains the UID for a particular table entry. It will return NULL if the entry does not exist.
     */
    public String getUID(final String uidQueryStr) {
        String uid = null;

        Connection conn = getAccessControlDbConn();

        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();
                if (stmt.execute(uidQueryStr)) {
                    rs = stmt.getResultSet();
                }
            } catch (SQLException sqlEx) {
                LOG.error("getUID() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + uidQueryStr);
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (rs != null) {
                    try {
                        int count = 0;
                        while (rs.next()) {
                            /*
                             ** The rs.getString(1) is the String format of the UID.
                             */
                            uid = rs.getString(1);
                            //LOG.info("Requested UID: " + uid);

                            count++;
                        }

                        /*
                        ** There should only be 0 or 1 responses from the getUID() query. There are 0 responses when
                        **   this is used to check for the presence of a record within a table.
                         */
                        if (count > 1) {
                            uid = null;
                            LOG.warn("getUID() incorrect response count: " + count);
                            LOG.warn(uidQueryStr);
                        }
                    } catch (SQLException sqlEx) {
                        System.out.println("getUID() SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("getUID() SQL conn rs.close() SQLException: " + sqlEx.getMessage());
                    }
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("getUID() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeAccessControlDbConn(conn);
        }

        return uid;
    }

    /*
     ** This obtains the AUTO_INCREMENT Id from a particular table entry. It will return -1 if the entry does not exist.
     */
    public int getId(final String idQueryStr) {
        int id = -1;

        Connection conn = getAccessControlDbConn();

        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();
                if (stmt.execute(idQueryStr)) {
                    rs = stmt.getResultSet();
                }
            } catch (SQLException sqlEx) {
                LOG.error("getId() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + idQueryStr);
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (rs != null) {
                    try {
                        int count = 0;
                        while (rs.next()) {
                            /*
                             ** The rs.getString(1) is the String format of the UID.
                             */
                            id = rs.getInt(1);
                            //LOG.info("Requested Id: " + id);

                            count++;
                        }

                        if (count != 1) {
                            id = -1;
                        }
                    } catch (SQLException sqlEx) {
                        System.out.println("getId() SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("getId() SQL conn rs.close() SQLException: " + sqlEx.getMessage());
                    }
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("getId() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeAccessControlDbConn(conn);
        }

        return id;
    }

    /*
     ** This obtains the a single String from a particular table entry. It will return null if the entry does not exist.
     */
    public String getSingleStr(final String createTimeQueryStr) {
        String requestedStr = null;

        Connection conn = getAccessControlDbConn();

        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();
                if (stmt.execute(createTimeQueryStr)) {
                    rs = stmt.getResultSet();
                }
            } catch (SQLException sqlEx) {
                LOG.error("getSingleStr() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + createTimeQueryStr);
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (rs != null) {
                    try {
                        int count = 0;
                        while (rs.next()) {
                            /*
                             ** The rs.getString(1) is the String format of the UID.
                             */
                            requestedStr = rs.getString(1);
                            //LOG.info("Return string: " + requestedStr);

                            /*
                             ** If it is "NULL", that means the field was not field in. Return null for
                             **   the string to indicate it is empty.
                             */
                            if (requestedStr.equals("NULL")) {
                                requestedStr = null;
                            }

                            count++;
                        }

                        if (count != 1) {
                            LOG.warn("getSingleStr() too many responses count: " + count);
                            LOG.warn(createTimeQueryStr);
                            requestedStr = null;
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
                        LOG.error("getSingleStr() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeAccessControlDbConn(conn);
        }

        return requestedStr;
    }

}
