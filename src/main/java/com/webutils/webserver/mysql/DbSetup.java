package com.webutils.webserver.mysql;

import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.webutils.webserver.kubernetes.KubernetesInfo;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    private static final String kubernetesStorageServerTable = "CREATE TABLE IF NOT EXISTS kubernetesServerIdentifier (" +
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

    private static final String addKubernetesStorageServer_1 = "INSERT INTO kubernetesServerIdentifier(serverName, serverIpAddress, serverPortNumber) " +
            "VALUES('KubernetesStorageServer_";

    private static final String dropLocalServerIdentifierTable = "DROP TABLE IF EXISTS localServerIdentifier";
    private static final String dropDockerServerIdentifierTable = "DROP TABLE IF EXISTS dockerServerIdentifier" ;
    private static final String dropKubernetesServerIdentifierTable = "DROP TABLE IF EXISTS kubernetesServerIdentifier" ;

    private static final String kubeJDBCDatabaseConnect = "jdbc:mysql://host.docker.internal/StorageServers?serverTimeZone=US/Mountain";
    private static final String execJDBCDatabaseConnect = "jdbc:mysql://localhost/StorageServers?serverTimezone=US/Mountain";
    private static final String kubeJDBCConnect = "jdbc:mysql://host.docker.internal/?serverTimeZone=US/Mountain";
    private static final String execJDBCConnect = "jdbc:mysql://localhost/?serverTimezone=US/Mountain";

    /*
    ** Is this running within a Docker Container?
     */
    private final WebServerFlavor flavor;

    public DbSetup(final WebServerFlavor flavor) {
        LOG.info("DBSetup() WebServerFlavor: " + flavor.toString());
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

        /*
        ** Only have the Kubernetes Object Server setup the database to prevent conflicts with access to the database
         */
        if (flavor == WebServerFlavor.KUBERNETES_OBJECT_SERVER_TEST) {
            /*
             ** Always drop and repopulate the Kubernetes storage server tables since the IP address
             **   for the POD will likely change between startups.
             */
            String kubernetesIpAddr = null;

            KubernetesInfo kubeInfo = new KubernetesInfo(flavor);
            int retryCount = 0;
            final int maxRetryCount = 10;

            while ((kubernetesIpAddr == null) && (retryCount < maxRetryCount)) {
                try {
                    kubernetesIpAddr = kubeInfo.getInternalKubeIp();
                } catch (IOException io_ex) {
                    System.out.println("IOException: " + io_ex.getMessage());
                    LOG.error("checkAndSetupStorageServers() - IOException: " + io_ex.getMessage());
                    kubernetesIpAddr = null;
                }

                if (kubernetesIpAddr == null) {
                    try {
                        TimeUnit.SECONDS.sleep(10);
                    } catch (InterruptedException intEx) {
                        LOG.error("Trying to obtain internal Kubernetes IP " + intEx.getMessage());
                        break;
                    }

                    retryCount++;
                }
            }

            dropKubernetesStorageServerTables();
            createKubernetesStorageServerTables();
            populateKubernetesStorageServers(kubernetesIpAddr);
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
        String jdbcConnect;

        if (isDockerImage() || isKubernetesImage()) {
            jdbcConnect = kubeJDBCDatabaseConnect;
        } else {
            jdbcConnect = execJDBCDatabaseConnect;
        }

        try {
            conn = DriverManager.getConnection(jdbcConnect, storageServerUser, storageServerPassword);
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("JDBC connect: " + jdbcConnect);
            System.out.println("getStorageServerDbConn() - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());
            LOG.error("getStorageServerDbConn() - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());
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
                System.out.println("closeStorageServerDbConn() - SQL conn close(2) SQLException: " + sqlEx.getMessage() + "  SQLState: " + sqlEx.getSQLState());
                LOG.error("closeStorageServerDbConn() - SQL conn close(2) SQLException: " + sqlEx.getMessage() + "  SQLState: " + sqlEx.getSQLState());
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

        String jdbcConnect;

        if (isDockerImage() || isKubernetesImage()) {
            jdbcConnect = kubeJDBCDatabaseConnect;
        } else {
            jdbcConnect = execJDBCDatabaseConnect;
        }

        try {
            System.out.println("Trying to connect to MySql");
            conn = DriverManager.getConnection(jdbcConnect, userName, password);

            /*
            ** Not going to use this connection, it is just to check if the database exists and can be
            **   connected to.
             */
            try {
                conn.close();
            } catch (SQLException sqlEx) {
                // handle any errors
                System.out.println("createStorageServerDb() - SQL conn close(1) SQLException: " + sqlEx.getMessage());
                LOG.error("createStorageServerDb() - SQL conn close(1) SQLException: " + sqlEx.getMessage());
            }

            System.out.println("createStorageServerDb() database found");

            return false;
        } catch (SQLException ex) {
            // handle any errors

            vendorError = ex.getErrorCode();
            System.out.println("createStorageServerDb(1) - JDBC connect: " + jdbcConnect);
            System.out.println("createStorageServerDb(1) - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());
            LOG.error("createStorageServerDb(1) - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());

            if (vendorError != MysqlErrorNumbers.ER_BAD_DB_ERROR) {
                return false;
            }
        }

        /*
         ** Check if the Database needs to be created
         */
        if (vendorError == MysqlErrorNumbers.ER_BAD_DB_ERROR) {
            System.out.println("createStorageServerDb() create database");

            if (isDockerImage() || isKubernetesImage()) {
                jdbcConnect = kubeJDBCConnect;
            } else {
                jdbcConnect = execJDBCConnect;
            }

            try {
                conn = DriverManager.getConnection(jdbcConnect, userName, password);
            } catch (SQLException ex) {
                vendorError = ex.getErrorCode();
                System.out.println("createStorageServerDb(2) - JDBC connect: " + jdbcConnect);
                System.out.println("createStorageServerDb(2) - creating database - SQLException: " + ex.getMessage() +
                        " SQLState: " + ex.getSQLState() + " " + vendorError);
                LOG.error("createStorageServerDb(2) - creating database - SQLException: " + ex.getMessage() +
                        " SQLState: " + ex.getSQLState() + " " + vendorError);
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
                    System.out.println("createStorageServerDb() - create database - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                    LOG.error("createStorageServerDb() - create database - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                    return false;
                }
                finally {
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException sqlEx) {
                            System.out.println("createStorageServerDb() - close - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                            LOG.error("createStorageServerDb() - close - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
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
                    System.out.println("SQL conn close(2) SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                    LOG.error("SQL conn close(2) SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                }
            }
        }

        System.out.println("createStorageServerDb() database created");
        return true;
    }

    /*
    ** This will drop the docker and the local storage server tables. These are managed separately from the
    **   Kubernetes storage server tables since those can change with each startup
     */
    public void dropStorageServerTables() {
        System.out.println("dropStorageServerTables() WebServerFlavor: " + flavor.toString());
        LOG.info("dropStorageServerTables() WebServerFlavor: " + flavor.toString());

        Connection conn = getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(dropLocalServerIdentifierTable);
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
                stmt.execute(dropDockerServerIdentifierTable);
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
    }

    /*
     ** This will drop the Kubernetes storage server table. These are managed separately from the
     **   since those can change with each startup
     */
    public void dropKubernetesStorageServerTables() {
        LOG.info("dropKubernetesStorageServerTables()");

        Connection conn = getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(dropKubernetesServerIdentifierTable);
            } catch (SQLException sqlEx) {
                LOG.error("SQLException: " + sqlEx.getMessage());
                System.out.println("SQLState: " + sqlEx.getSQLState());
                System.out.println("VendorError: " + sqlEx.getErrorCode());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("SQLException: " + sqlEx.getMessage());
                        System.out.println("SQLState: " + sqlEx.getSQLState());
                        System.out.println("VendorError: " + sqlEx.getErrorCode());
                    }

                    stmt = null;
                }
            }

            /*
             ** Close out this connection as it was only used to drop the Kubernetes database table.
             */
            closeStorageServerDbConn(conn);
            conn = null;
        }
    }

    /*
    ** This performs an initial setup of the tables associated with the Storage Servers if they do not already
    **   exist (meaning they are created if the database did not exist).
     */
    private boolean createStorageServerTables() {

        LOG.info("createStorageServerTables()");

        Connection conn = getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(localStorageServerTable);
            } catch (SQLException sqlEx) {
                LOG.error("SQLException: " + sqlEx.getMessage());
                System.out.println("SQLState: " + sqlEx.getSQLState());
                System.out.println("VendorError: " + sqlEx.getErrorCode());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("SQLException: " + sqlEx.getMessage());
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
    ** This is used to create the Kubernetes storage server table.
     */
    private boolean createKubernetesStorageServerTables() {

        LOG.info("createKubernetesStorageServerTables()");

        Connection conn = getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(kubernetesStorageServerTable);
            } catch (SQLException sqlEx) {
                LOG.error("SQLException: " + sqlEx.getMessage());
                System.out.println("SQLState: " + sqlEx.getSQLState());
                System.out.println("VendorError: " + sqlEx.getErrorCode());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("SQLException: " + sqlEx.getMessage());
                        System.out.println("SQLState: " + sqlEx.getSQLState());
                        System.out.println("VendorError: " + sqlEx.getErrorCode());
                    }

                    stmt = null;
                }
            }

            /*
             ** Close out this connection as it was only used to create the Kubernetes database table.
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

    /*
     ** This populates three initial records for the Storage Servers
     */
    private void populateKubernetesStorageServers(final String kubernetesIpAddr) {
        if (kubernetesIpAddr != null) {

            LOG.info("populateKubernetesStorageServers() IP Address: " + kubernetesIpAddr);
            Connection conn = getStorageServerDbConn();

            if (conn != null) {
                Statement stmt = null;

                try {
                    stmt = conn.createStatement();
                    for (int i = 0; i < 3; i++) {
                        String tmp = addKubernetesStorageServer_1 + i + "', '" + kubernetesIpAddr + "', " + (storageServerTcpPort + i) +
                                addStorageServer_3;
                        stmt.execute(tmp);
                    }
                } catch (SQLException sqlEx) {
                    LOG.error("SQLException: " + sqlEx.getMessage());
                    System.out.println("SQLState: " + sqlEx.getSQLState());
                    System.out.println("VendorError: " + sqlEx.getErrorCode());

                    return;
                } finally {
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException sqlEx) {
                            LOG.error("SQLException: " + sqlEx.getMessage());
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
        } else {
            LOG.error("populateKubernetesStorageServers() IP Address is NULL");
        }
    }


}
