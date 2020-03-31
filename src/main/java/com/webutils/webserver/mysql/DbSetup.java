package com.webutils.webserver.mysql;

import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.webutils.webserver.kubernetes.KubernetesInfo;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class DbSetup {

    private static final Logger LOG = LoggerFactory.getLogger(DbSetup.class);

    public static final int storageServerTcpPort = 5010;

    private static final String userName = "root";
    private static final String password = "ktm300exc";

    private static final String storageServerDbName = "StorageServers";

    private static final String storageServerUser = "storageserveruser";
    private static final String storageServerPassword = "rwt25nX1";

    /*
    ** This table is used when running the test instance within the IntelliJ framework.
     */
    private static final String localStorageServerTable = "CREATE TABLE IF NOT EXISTS localServerIdentifier (" +
            " identifier INT AUTO_INCREMENT," +
            " serverName VARCHAR(255) NOT NULL," +
            " serverIpAddress VARCHAR(32) NOT NULL," +
            " serverPortNumber INT," +
            " PRIMARY KEY (identifier)" +
            ")";

    /*
    ** The k8PodServiceInfo is what provides the IP address and Port number for the Kubernetes POD (or PODs) that
    **   contain the Object Server and the mock Storage Servers.
    ** This table is used by the Client Test executable to obtain information when it is running in a Docker
    **   container. This is when the Client Test is started with the "docker-test" flag. This will set the
    **   WebServerFlavor to INTEGRATION_KUBERNETES_TESTS.
     */
    private static final String k8PodServiceInfoTable = "CREATE TABLE IF NOT EXISTS k8PodServiceInfo (" +
            " identifier INT AUTO_INCREMENT," +
            " serverName VARCHAR(255) NOT NULL," +
            " serverIpAddress VARCHAR(32) NOT NULL," +
            " serverPortNumber INT," +
            " PRIMARY KEY (identifier)" +
            ")";

    /*
    ** The k8LocalStorageServerInfo is what provides the IP address and Port number for the internal communications
    **   between the Object Server and the Storage Servers running in the same POD.
    ** This table is reconfigured every time the Object and Storage Server POD is restarted. This table only holds the
    **   IP Address/Port information for the Storage Servers.
    **
    ** NOTE: In theory, the Object Server should be able to use localhost to communicate with the mock Storage Servers,
    **   but that did not seem to work. Instead, the IP address for the mock Storage Servers is the endpoint address.
    **   This is the:
    **      V1Endpoints endpoint -> ️
    **      List<V1EndpointSubset> subsets = endpoint.getSubsets(); ->
    **      List<V1EndpointAddress> endpointAddrList = subset.getAddresses(); ->
    **      V1EndpointAddress addr = endpointAddrIter.next(); ->
    **      internalPodIp = addr.getIp();
     */
    private static final String k8LocalStorageServerInfoTable = "CREATE TABLE IF NOT EXISTS k8LocalStorageServerInfo (" +
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

    private static final String addK8Server = "INSERT INTO k8PodServiceInfo(serverName, serverIpAddress, serverPortNumber) " +
            "VALUES('";

    private static final String addK8InternalStorageServer = "INSERT INTO k8LocalStorageServerInfo(serverName, serverIpAddress, serverPortNumber) " +
            "VALUES('";

    private static final String dropLocalServerIdentifierTable = "DROP TABLE IF EXISTS localServerIdentifier";
    private static final String dropK8ServiceInfoTable = "DROP TABLE IF EXISTS k8PodServiceInfo" ;
    private static final String dropK8StorageServerInfoTable = "DROP TABLE IF EXISTS k8LocalStorageServerInfo" ;

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
    ** This checks if the databases are created and creates them if needed. If the database is created, it will
    **   then create the tables. The tables that are created are dependent upon who the caller is and the application
    **   type that is running (test code within IntelliJ, Docker image or Kubernetes POD).
     */
    public void checkAndSetupStorageServers() {

        /*
        ** When the database is created, the local mock Storage Server IP addresses and Ports are populated no
        **   matter which version of the code is running. These IP addresses and Ports are fixed and will not
        **   change (at least without changing a constant).
         */
        if (createStorageServerDb()) {
            createLocalStorageServerTable();
            populateLocalStorageServers();
        }

        /*
        ** This simply waits until the IP address can be obtained to insure that the POD is up and running.
        **
        ** NOTE: There must be a better way to determine if the POD has been started...
         */
        String k8IpAddr = null;

        KubernetesInfo kubeInfo = new KubernetesInfo(flavor);
        int retryCount = 0;
        int maxRetryCount;
        if (flavor == WebServerFlavor.KUBERNETES_OBJECT_SERVER_TEST) {
            maxRetryCount = 10;
        } else {
            maxRetryCount = 3;
        }

        while ((k8IpAddr == null) && (retryCount < maxRetryCount)) {
            try {
                k8IpAddr = kubeInfo.getInternalKubeIp();
            } catch (IOException io_ex) {
                System.out.println("IOException: " + io_ex.getMessage());
                LOG.error("checkAndSetupStorageServers() - IOException: " + io_ex.getMessage());
                k8IpAddr = null;
            }

            if (k8IpAddr == null) {
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException intEx) {
                    LOG.error("Trying to obtain internal Kubernetes IP " + intEx.getMessage());
                    break;
                }

                retryCount++;
            }
        }

        /*
        ** Only have the Kubernetes Object Server setup the k8 internal table to prevent conflicts with access
        **   to the database. Also, since the Object Server running within the POD is the only executable that
        **   needs access to that table, it is implicity the owner as well.
         */
        if (flavor == WebServerFlavor.KUBERNETES_OBJECT_SERVER_TEST) {
            /*
            ** Always drop and repopulate the Kubernetes storage server tables since the IP address
            **   for the POD will likely change between startups.
            ** Recreate the tables that provide the IP address and Port mappings for the Storage Servers when accessed
            **   from within the POD.
             */
            dropK8InternalStorageServerTables();

            /*
            ** Obtain the list of Storage Servers and their NodePorts (this is the port number that is exposed outside the
            **   POD).
             */
            Map<String, Integer> storageServersInfo = new Hashtable<>();
            try {
                int count = kubeInfo.getStorageServerPorts(storageServersInfo);
                if (count != 0) {
                    createK8InternalStorageServerTables();
                    populateK8InternalStorageServers(k8IpAddr, storageServersInfo);
                } else {
                    System.out.println("No Storage Server NodePorts - checkAndSetupStorageServers()");
                    LOG.error("No Storage Server NodePorts - checkAndSetupStorageServers()");
                }
            } catch (IOException io_ex) {
                System.out.println("Unable to obtain Storage Server NodePorts - IOException: " + io_ex.getMessage());
                LOG.error("Unable to obtain Storage Server NodePorts - checkAndSetupStorageServers() - IOException: " + io_ex.getMessage());
            }
        } else if (flavor == WebServerFlavor.INTEGRATION_KUBERNETES_TESTS) {
            /*
            ** Drop the table since the IP addresses and Port number may change between startups.
             */
            dropK8ServiceInfoTable();

            /*
             ** Obtain the list of Object Server and Storage Servers and their NodePorts (this is the port number that is exposed outside the
             **   POD).
             */
            Map<String, Integer> serversInfo = new Hashtable<>();
            try {
                int count = kubeInfo.getNodePorts(serversInfo);
                if (count != 0) {
                    createK8ServerInfoTable();
                    populateK8ServersInfo(k8IpAddr, serversInfo);
                } else {
                    System.out.println("No Storage Server NodePorts - checkAndSetupStorageServers()");
                    LOG.error("No Storage Server NodePorts - checkAndSetupStorageServers()");
                }
            } catch (IOException io_ex) {
                System.out.println("Unable to obtain Storage Server NodePorts - IOException: " + io_ex.getMessage());
                LOG.error("Unable to obtain Storage Server NodePorts - checkAndSetupStorageServers() - IOException: " + io_ex.getMessage());
            }
        }
    }

    /*
    ** This obtains a connection to communicate with the MySQL Storage Server database.
     */
    public Connection getStorageServerDbConn() {
        Connection conn;
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
        Connection conn;
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
    ** This will drop the local storage server table. This are managed separately from the
    **   Kubernetes storage server tables since those can change with each startup
     */
    public void dropLocalStorageServerTables() {
        System.out.println("dropLocalStorageServerTables() WebServerFlavor: " + flavor.toString());
        LOG.info("dropLocalStorageServerTables() WebServerFlavor: " + flavor.toString());

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

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
            conn = null;
        }
    }

    /*
     ** This will drop the Kubernetes Service Info table. This table is used to figure out the IP adddresses and Ports of the
     **   resources (Object Server, mock Storage Servers) running within a POD. This table provides the way that the
     **   resources can be accessed by an external entity.
     */
    public void dropK8ServiceInfoTable() {
        System.out.println("dropK8ServiceInfoTable() WebServerFlavor: " + flavor.toString());
        LOG.info("dropK8ServiceInfoTable() WebServerFlavor: " + flavor.toString());

        Connection conn = getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(dropK8ServiceInfoTable);
            } catch (SQLException sqlEx) {
                LOG.error("statement SQLException: " + sqlEx.getMessage() + " VendorError: " + sqlEx.getErrorCode());
                System.out.println("SQLException: " + sqlEx.getMessage());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("close SQLException: " + sqlEx.getMessage() + " VendorError: " + sqlEx.getErrorCode());
                        System.out.println("SQLException: " + sqlEx.getMessage());
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
    public void dropK8InternalStorageServerTables() {
        LOG.info("dropKubernetesStorageServerTables()");

        Connection conn = getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(dropK8StorageServerInfoTable);
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
    private void createLocalStorageServerTable() {

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

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
            conn = null;
        }
    }

    /*
    ** Create the table used to hold the IP address and Port information for the Object Server and mock Storage Servers
    **   running within a POD.
     */
    private void createK8ServerInfoTable() {

        LOG.info("createK8ServerInfoTable()");

        Connection conn = getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(k8PodServiceInfoTable);
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
    ** This is used to create the Kubernetes storage server table. This table is used by the Object Server running within
    **   the Kubernetes POD to access the mock Storage Servers that are also running within the same POD.
     */
    private void createK8InternalStorageServerTables() {

        LOG.info("createKubernetesStorageServerTables()");

        Connection conn = getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(k8LocalStorageServerInfoTable);
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
    }

    /*
    ** This populates three initial records for the Storage Servers. These records are used to access the mock
    **   Storage Servers when running the test framework from within IntelliJ.
     */
    private void populateLocalStorageServers() {
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
     ** This populates three initial records for the Storage Servers for access from within the Kubernetes POD. These
     **   records are used by the Object Server to access the mock Storage Servers running within the same POD.
     */
    private void populateK8InternalStorageServers(final String kubernetesIpAddr, final Map<String, Integer> storageServersInfo) {
        if (kubernetesIpAddr != null) {

            LOG.info("populateKubernetesStorageServers() IP Address: " + kubernetesIpAddr);
            Connection conn = getStorageServerDbConn();

            if (conn != null) {
                Statement stmt = null;

                try {
                    stmt = conn.createStatement();

                    Set< Map.Entry< String,Integer> > servers = storageServersInfo.entrySet();
                    for (Map.Entry< String,Integer> server:servers)
                    {
                        String tmp = addK8InternalStorageServer + server.getKey() + "', '" + kubernetesIpAddr + "', " + (server.getValue()) +
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
            LOG.error("populateK8InternalStorageServers() IP Address is NULL");
        }
    }

    /*
     ** This populates three initial records for the Storage Servers for access from within the Kubernetes POD. These
     **   records are used by the Object Server to access the mock Storage Servers running within the same POD.
     */
    private void populateK8ServersInfo(final String k8PodIpAddr, final Map<String, Integer> storageServersInfo) {
        if (k8PodIpAddr != null) {

            LOG.info("populateK8ServersInfo() IP Address: " + k8PodIpAddr);
            Connection conn = getStorageServerDbConn();

            if (conn != null) {
                Statement stmt = null;

                try {
                    stmt = conn.createStatement();

                    Set< Map.Entry< String,Integer> > servers = storageServersInfo.entrySet();
                    for (Map.Entry< String,Integer> server:servers)
                    {
                        String tmp = addK8Server + server.getKey() + "', '" + k8PodIpAddr + "', " + (server.getValue()) +
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
            LOG.error("populateK8ServersInfo() IP Address is NULL");
        }
    }

    public boolean retrieveStorageServers(final String sqlQuery, final List<ServerIdentifier> servers, final int chunkNumber) {
        boolean success = false;

        LOG.info("DbSetup getStorageServers() dockerImage: " + isDockerImage() + " k8Image: " +
                isKubernetesImage());

        Connection conn = getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();

                if (stmt.execute(sqlQuery)) {
                    rs = stmt.getResultSet();
                }
            } catch (SQLException sqlEx) {
                // handle any errors
                System.out.println("SQL conn close(2) SQLException: " + sqlEx.getMessage());
                System.out.println("SQL conn close(2) SQLState: " + sqlEx.getSQLState());
                System.out.println("SQL conn close(2) VendorError: " + sqlEx.getErrorCode());
            }

            finally {
                if (rs != null) {

                    try {
                        while (rs.next()) {
                            try {
                                /*
                                 ** The rs.getString(3) is the String format of the IP Address.
                                 */
                                InetAddress inetAddress = InetAddress.getByName(rs.getString(3));

                                ServerIdentifier server = new ServerIdentifier(rs.getString(2), inetAddress, rs.getInt(4), chunkNumber);
                                servers.add(server);

                                LOG.info("StorageServer host: " + rs.getString(2) + " " + inetAddress.toString() + " port: " +
                                        rs.getInt(4));
                            } catch (UnknownHostException ex) {
                                LOG.warn("Unknown host: " + rs.getString(2) + " " + ex.getMessage());
                                System.out.println("Unknown host: " + rs.getString(2) + " " + ex.getMessage());
                            }
                        }
                    } catch (SQLException sqlEx) {
                        System.out.println("SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    success = true;
                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("SQL conn rs close() SQLException: " + sqlEx.getMessage());
                        System.out.println("SQL conn rs close() SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQL conn rs close() VendorError: " + sqlEx.getErrorCode());
                    }

                    rs = null;
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("SQL conn stmt close() SQLException: " + sqlEx.getMessage());
                        System.out.println("SQL conn stmt close() SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQL conn stmt close() VendorError: " + sqlEx.getErrorCode());
                    }

                    stmt =  null;
                }
            }

            closeStorageServerDbConn(conn);
            conn = null;
        }

        return success;
    }

    /*
     ** This will return a specific server (if one exists) based upon the passed in serverName. Currently, the only
     **   server names that are valid are:
     **     "object-server"
     **     "storage-server-1"
     **     "storage-server-2"
     **     "storage-server-3"
     */
    protected boolean getServer(final String sqlQuery, final List<ServerIdentifier> serverList) {
        boolean success = false;

        Connection conn = getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();

                if (stmt.execute(sqlQuery)) {
                    rs = stmt.getResultSet();
                }
            } catch (SQLException sqlEx) {
                // handle any errors
                System.out.println("SQL conn close(2) SQLException: " + sqlEx.getMessage());
                System.out.println("SQL conn close(2) SQLState: " + sqlEx.getSQLState());
                System.out.println("SQL conn close(2) VendorError: " + sqlEx.getErrorCode());
            }

            finally {
                if (rs != null) {

                    try {
                        while (rs.next()) {
                            try {
                                /*
                                 ** The rs.getString(3) is the String format of the IP Address.
                                 */
                                InetAddress inetAddress = InetAddress.getByName(rs.getString(3));

                                ServerIdentifier server = new ServerIdentifier(rs.getString(2), inetAddress, rs.getInt(4), 0);
                                serverList.add(server);

                                LOG.info("StorageServer host: " + rs.getString(2) + " " + inetAddress.toString());
                            } catch (UnknownHostException ex) {
                                LOG.warn("Unknown host: " + rs.getString(2) + " " + ex.getMessage());
                                System.out.println("Unknown host: " + rs.getString(2) + " " + ex.getMessage());
                            }
                        }
                    } catch (SQLException sqlEx) {
                        System.out.println("SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    success = true;
                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("SQL conn rs close() SQLException: " + sqlEx.getMessage());
                        System.out.println("SQL conn rs close() SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQL conn rs close() VendorError: " + sqlEx.getErrorCode());
                    }

                    rs = null;
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("SQL conn stmt close() SQLException: " + sqlEx.getMessage());
                        System.out.println("SQL conn stmt close() SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQL conn stmt close() VendorError: " + sqlEx.getErrorCode());
                    }

                    stmt =  null;
                }
            }

            closeStorageServerDbConn(conn);
            conn = null;
        }

        return success;
    }

    public abstract boolean getStorageServers(final List<ServerIdentifier> servers, final int chunkNumber);

}
