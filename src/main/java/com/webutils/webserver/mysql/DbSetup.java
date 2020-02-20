package com.webutils.webserver.mysql;

import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class DbSetup {

    private static final Logger LOG = LoggerFactory.getLogger(DbSetup.class);

    public static final int storageServerTcpPort = 5010;

    private static final String userName = "root";
    private static final String password = "ktm300exc";

    private static final String storageServerDbName = "StorageServers";

    private static final String storageServerUser = "storageserveruser";
    private static final String storageServerPassword = "rwt25nX1";

    private static final String localStorageServerTable = "CREATE TABLE IF NOT EXISTS localServerIdentifier (" +
            " identifier INT AUTO_INCREMENT," +
            " serverName VARCHAR(255) NOT NULL," +
            " serverIpAddress VARCHAR(32) NOT NULL," +
            " serverPortNumber INT," +
            " PRIMARY KEY (identifier)" +
            ")";

    private static final String dockerStorageServerTable = "CREATE TABLE IF NOT EXISTS dockerServerIdentifier (" +
            " identifier INT AUTO_INCREMENT," +
            " serverName VARCHAR(255) NOT NULL," +
            " serverIpAddress VARCHAR(32) NOT NULL," +
            " serverPortNumber INT," +
            " PRIMARY KEY (identifier)" +
            ")";

    private static final String createStorageServerUser = "CREATE USER '" + storageServerUser + "'@'localhost'" +
            " IDENTIFIED BY '" + storageServerPassword + "'";

    private static final String privilegeStorageServerUser = "GRANT ALL PRIVILEGES ON " + storageServerDbName +
            ".* TO '" + storageServerUser + "'@'localhost'";

    private static final String addStorageServer_1 = "INSERT INTO localServerIdentifier(serverName, serverIpAddress, serverPortNumber) " +
            "VALUES('LocalStorageServer_";
    private static final String addStorageServer_2 = "', 'localhost', ";
    private static final String addStorageServer_3 = ")";

    private static final String addDockerStorageServer_1 = "INSERT INTO dockerServerIdentifier(serverName, serverIpAddress, serverPortNumber) " +
            "VALUES('DockerStorageServer_";
    private static final String addDockerStorageServer_2 = "', 'StorageServer', ";

    /*
    ** Is this running within a Docker Container?
     */
    private final WebServerFlavor flavor;

    public DbSetup(final WebServerFlavor flavor) {
        this.flavor = flavor;
    }

    public boolean isDockerImage() {
        return ((flavor == WebServerFlavor.DOCKER_OBJECT_SERVER_TEST) ||
                (flavor == WebServerFlavor.DOCKER_STORAGE_SERVER_TEST));
    }

    public boolean isKubernetesImage() {
        return ((flavor == WebServerFlavor.KUBERNETES_OBJECT_SERVER_TEST) ||
                (flavor == WebServerFlavor.KUBERNETES_STORAGE_SERVER_TEST));
    }

    /*
    ** This checks if the databases are created and creates them if needed. If the database is created, it will
    **   then create the tables.
     */
    public void checkAndSetupStorageServers() {
        if (createStorageServerDb()) {
            createStorageServerTables();
            populateStorageServers();
        }

        StorageServerDbOps ops = new StorageServerDbOps(this);

        List<ServerIdentifier> serverList = new LinkedList<>();

        ops.getStorageServers(serverList, 0);
    }

    /*
    ** This obtains a connection to communicate with the MySQL Storage Server database.
     */
    public Connection getStorageServerDbConn() {
        Connection conn = null;

        try {
            if (isDockerImage() || isKubernetesImage()) {
                conn = DriverManager.getConnection("jdbc:mysql://host.docker.internal/StorageServers", storageServerUser,
                        storageServerPassword);
            } else {
                conn = DriverManager.getConnection("jdbc:mysql://localhost/StorageServers", storageServerUser,
                        storageServerPassword);
            }
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
            return null;
        }

        return conn;
    }

    /*
    ** This closes the connection used to communicate with the MySql Storage Server database.
     */
    public void closeStorageServerDbConn(final Connection conn) {

        /*
        ** Add safety check in case this was called when the Connection was not actually
        **   created in the first place.
         */
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException sqlEx) {
                // handle any errors
                System.out.println("SQL conn close(2) SQLException: " + sqlEx.getMessage());
                System.out.println("SQL conn close(2) SQLState: " + sqlEx.getSQLState());
                System.out.println("SQL conn close(2) VendorError: " + sqlEx.getErrorCode());
            }
        }
    }

    /*
    ** This creates the database used to hold the Storage Server locations if the database does not already
    **   exist.
     */
    private boolean createStorageServerDb() {
        Connection conn = null;

        int vendorError = 0;

        try {
            System.out.println("Trying to connect to MySql");
            if (isDockerImage() || isKubernetesImage()) {
                conn = DriverManager.getConnection("jdbc:mysql://host.docker.internal/StorageServers", userName, password);
            } else {
                conn = DriverManager.getConnection("jdbc:mysql://localhost/StorageServers", userName, password);
            }

            /*
            ** Not going to use this connection, it is just to check if the database exists and can be
            **   connected to.
             */
            try {
                conn.close();
            } catch (SQLException sqlEx) {
                // handle any errors
                System.out.println("SQL conn close(1) SQLException: " + sqlEx.getMessage());
                System.out.println("SQL conn close(1) SQLState: " + sqlEx.getSQLState());
                System.out.println("SQL conn close(1) VendorError: " + sqlEx.getErrorCode());
            }

            System.out.println("createStorageServerDb() database found");

            return false;
        } catch (SQLException ex) {
            // handle any errors

            vendorError = ex.getErrorCode();
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + vendorError);

            if (vendorError != MysqlErrorNumbers.ER_BAD_DB_ERROR) {
                return false;
            }
        }

        /*
         ** Check if the Database needs to be created
         */
        if (vendorError == MysqlErrorNumbers.ER_BAD_DB_ERROR) {
            System.out.println("createStorageServerDb() create database");
            try {
                if (isDockerImage() || isKubernetesImage()) {
                    conn = DriverManager.getConnection("jdbc:mysql://host.docker.internal/", userName, password);
                } else {
                    conn = DriverManager.getConnection("jdbc:mysql://localhost/", userName, password);
                }
            } catch (SQLException ex) {
                vendorError = ex.getErrorCode();
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + vendorError);

                return false;
            }

            if (conn != null) {
                Statement stmt = null;

                try {
                    stmt = conn.createStatement();
                    stmt.execute("CREATE DATABASE StorageServers");

                    stmt.execute(createStorageServerUser);

                    stmt.execute(privilegeStorageServerUser);
                } catch (SQLException sqlEx) {
                    System.out.println("SQLException: " + sqlEx.getMessage());
                    System.out.println("SQLState: " + sqlEx.getSQLState());
                    System.out.println("VendorError: " + sqlEx.getErrorCode());

                    return false;
                }
                finally {
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException sqlEx) {
                            System.out.println("SQLException: " + sqlEx.getMessage());
                            System.out.println("SQLState: " + sqlEx.getSQLState());
                            System.out.println("VendorError: " + sqlEx.getErrorCode());
                        }

                        stmt = null;
                    }
                }

                /*
                ** Close out this connection as it was only used to create the database.
                 */
                try {
                    conn.close();
                } catch (SQLException sqlEx) {
                    // handle any errors
                    System.out.println("SQL conn close(2) SQLException: " + sqlEx.getMessage());
                    System.out.println("SQL conn close(2) SQLState: " + sqlEx.getSQLState());
                    System.out.println("SQL conn close(2) VendorError: " + sqlEx.getErrorCode());
                }
            }
        }

        System.out.println("createStorageServerDb() database created");
        return true;
    }

    /*
    ** This performs an initial setup of the tables associated with the Storage Servers if they do not already
    **   exist (meaning they are created if the database did not exist).
     */
    private boolean createStorageServerTables() {

        System.out.println("createStorageServerTables()");

        Connection conn = getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(localStorageServerTable);
            } catch (SQLException sqlEx) {
                System.out.println("SQLException: " + sqlEx.getMessage());
                System.out.println("SQLState: " + sqlEx.getSQLState());
                System.out.println("VendorError: " + sqlEx.getErrorCode());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("SQLException: " + sqlEx.getMessage());
                        System.out.println("SQLState: " + sqlEx.getSQLState());
                        System.out.println("VendorError: " + sqlEx.getErrorCode());
                    }

                    stmt = null;
                }
            }

            try {
                stmt = conn.createStatement();
                stmt.execute(dockerStorageServerTable);
            } catch (SQLException sqlEx) {
                System.out.println("SQLException: " + sqlEx.getMessage());
                System.out.println("SQLState: " + sqlEx.getSQLState());
                System.out.println("VendorError: " + sqlEx.getErrorCode());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("SQLException: " + sqlEx.getMessage());
                        System.out.println("SQLState: " + sqlEx.getSQLState());
                        System.out.println("VendorError: " + sqlEx.getErrorCode());
                    }

                    stmt = null;
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
            conn = null;
        }

        return true;
    }

    /*
    ** This populates three initial records for the Storage Servers
     */
    private void populateStorageServers() {
        Connection conn = getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                for (int i = 0; i < 3; i++) {
                    String tmp = addStorageServer_1 + i + addStorageServer_2 + (storageServerTcpPort + i) +
                            addStorageServer_3;
                    stmt.execute(tmp);
                }

                for (int i = 0; i < 3; i++) {
                    String tmp = addDockerStorageServer_1 + i + addDockerStorageServer_2 + (storageServerTcpPort + i) +
                            addStorageServer_3;
                    stmt.execute(tmp);
                }
            } catch (SQLException sqlEx) {
                System.out.println("SQLException: " + sqlEx.getMessage());
                System.out.println("SQLState: " + sqlEx.getSQLState());
                System.out.println("VendorError: " + sqlEx.getErrorCode());

                return;
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("SQLException: " + sqlEx.getMessage());
                        System.out.println("SQLState: " + sqlEx.getSQLState());
                        System.out.println("VendorError: " + sqlEx.getErrorCode());
                    }

                    stmt = null;
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
            conn = null;
        }
    }
}
