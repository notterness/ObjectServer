package com.webutils.webserver.mysql;

import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.webutils.webserver.http.StorageTierEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public abstract class ServersDb {

    private static final Logger LOG = LoggerFactory.getLogger(ServersDb.class);

    public static final int STORAGE_SERVER_TCP_PORT = 5010;
    protected static final int CHUNKS_TO_ALLOCATE = 10;


    private static final String userName = "root";
    private static final String password = "ktm300exc";

    private static final String SERVICE_SERVERS_DB_NAME = "ServiceServersDb";

    private static final String SERVICE_SERVER_USER = "serviceserveruser";
    private static final String SERVICE_SERVER_PASSWORD = "rwt25nX1";

    /*
    ** This localServerIpAddr and localServerPort are used when running the test instance within the IntelliJ framework.
    **
     ** The k8Pod* are what what provides the IP address and Port number for the Kubernetes POD (or PODs) that
     **   contain the Object Server and the mock Storage Servers.
     **   These are used by the Client Test executable to obtain information when it is running in a Docker
     **   container. This is when the Client Test is started with the "docker-test" flag. This will set the
     **   WebServerFlavor to INTEGRATION_KUBERNETES_TESTS.
     **
     ** The k8* fields are what provides the IP address and Port number for the internal communications
     **   between the Object Server and the Storage Servers running in the same POD.
     ** This table is reconfigured every time the Object and Storage Server POD is restarted. This table only holds the
     **   IP Address/Port information for the Storage Servers.
     **
     ** The serverType is used to differentiate between the different types of service servers this table manages. The
     **   possible services are:
     **     OBJECT_SERVER - 1
     **     STORAGE_SERVER - 2
     **     CHUNK_MGR_SERVER - 3
     **
     ** NOTE: It may make more sense at some point to have different tables for different service servers, but for
     **   simplicity, it is a single combined table at the moment.
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
    protected static final int OBJECT_SERVER = 1;
    protected static final int STORAGE_SERVER = 2;
    protected static final int CHUNK_MGR_SERVER = 3;

    private static final String CREATE_SERVER_TABLE = "CREATE TABLE IF NOT EXISTS ServerIdentifier (" +
            " serverId INT AUTO_INCREMENT," +
            " serverName VARCHAR(255) NOT NULL," +
            " serverType INT NOT NULL," +
            " localServerIpAddr VARCHAR(32) NOT NULL," +
            " localServerPort INT," +
            " k8PodServerIpAddr VARCHAR(32)," +
            " k8PodServerPort INT," +
            " k8ServerIpAddr VARCHAR(32)," +
            " k8ServerPort INT," +
            " storageTier INT," +
            " allocatedChunks INT," +
            " usedChunks INT," +
            " createTime TIMESTAMP NOT NULL," +
            " lastAllocationTime TIMESTAMP," +
            " totalAllocations INT," +
            " disabled INT NOT NULL," +
            " disabledTime TIMESTAMP,"+
            " serverUID BINARY(16) NOT NULL," +
            " PRIMARY KEY (serverId)" +
            ")";

    /*
    ** The chunk state entry is used to keep track of the current state of the chunk on the storage server. The possible
    **   states are:
    **      Allocated - This chunk has client's data stored in it.
    **      Deleted - The object that referenced this chunk has been deleted, but the Storage Server has not been told
    **        to remove any references to it.
    **      Available - Once the Storage Server has released the chunk and over written the data so it cannot be read
    **        it is moved to the Available state. This means it can be used by another object to store client data.
     */
    private static final String CREATE_CHUNK_TABLE = "CREATE TABLE IF NOT EXISTS StorageServerChunk (" +
            " chunkId INT AUTO_INCREMENT," +
            " chunkNumber INT NOT NULL," +
            " startLba INT NOT NULL," +
            " size INT NOT NULL," +
            " state INT NOT NULL," +
            " lastAllocateTime TIMESTAMP," +
            " lastDeleteTime TIMESTAMP," +
            " chunkUID BINARY(16) NOT NULL," +
            " serverId INT NOT NULL," +
            " FOREIGN KEY(serverId)" +
            "   REFERENCES serverIdentifier(serverId)" +
            "   ON DELETE CASCADE," +
            " PRIMARY KEY(chunkId)" +
            ")";

    private static final String CREATE_SERVICE_SERVERS_USER = "CREATE USER '" + SERVICE_SERVER_USER + "'@'localhost'" +
            " IDENTIFIED BY '" + SERVICE_SERVER_PASSWORD + "'";

    private static final String PRIVILEDGE_SERVICE_SERVERS_USER = "GRANT ALL PRIVILEGES ON " + SERVICE_SERVERS_DB_NAME +
            ".* TO '" + SERVICE_SERVER_USER + "'@'localhost'";

    /*
    ** The storageTier field is a representation of the StorageTierEnum. The default is STANDARD_TIER (1).
     */
    protected static final String ADD_STORAGE_SERVER = "INSERT INTO serverIdentifier(serverName, serverType, localServerIpAddr, localServerPort," +
            " storageTier, allocatedChunks, usedChunks, createTime, lastAllocationTime, totalAllocations, disabled, serverUID) " +
            "VALUES(?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 0, 0, UUID_TO_BIN(UUID()) )";

    private static final String DROP_SERVER_IDENTIFIER_TABLE = "DROP TABLE IF EXISTS serverIdentifier";

    private static final String DROP_SERVICE_SERVERS_USER = "DROP USER '" + SERVICE_SERVER_USER + "'@'localhost'";
    private static final String DROP_SERVICE_SERVERS_DB = "DROP DATABASE " + SERVICE_SERVERS_DB_NAME;


    private static final String kubeJDBCDatabaseConnect = "jdbc:mysql://host.docker.internal/ServiceServersDb?serverTimeZone=US/Mountain";
    private static final String execJDBCDatabaseConnect = "jdbc:mysql://localhost/ServiceServersDb?serverTimezone=US/Mountain";
    private static final String kubeJDBCConnect = "jdbc:mysql://host.docker.internal/?serverTimeZone=US/Mountain";
    private static final String execJDBCConnect = "jdbc:mysql://localhost/?serverTimezone=US/Mountain";

    /*
    ** Is this running within a Docker Container?
     */
    protected final WebServerFlavor flavor;

    public ServersDb(final WebServerFlavor flavor) {
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
    public void checkAndSetupServices() {

        /*
        ** When the database is created, the local mock Storage Server IP addresses and Ports are populated no
        **   matter which version of the code is running. These IP addresses and Ports are fixed and will not
        **   change (at least without changing a constant).
         */
        if (createServerDb()) {
            createServerTable();
            createChunkTable();
            populateStorageServers(10, StorageTierEnum.STANDARD_TIER);

            ServerChunkMgr chunkMgr = new ServerChunkMgr(flavor);
            chunkMgr.addStoredProcedures();
        }
    }

    /*
     ** This is used to delete the ServiceServersDb and its users
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
                stmt.execute(DROP_SERVICE_SERVERS_USER);

                stmt.execute(DROP_SERVICE_SERVERS_DB);
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

        LOG.info("dropDatabase() ServiceServersDb database dropped");
    }


    /*
    ** This obtains a connection to communicate with the MySQL Storage Server database.
     */
    public Connection getServersDbConn() {
        Connection conn;
        String jdbcConnect;

        if (isDockerImage() || isKubernetesImage()) {
            jdbcConnect = kubeJDBCDatabaseConnect;
        } else {
            jdbcConnect = execJDBCDatabaseConnect;
        }

        try {
            conn = DriverManager.getConnection(jdbcConnect, SERVICE_SERVER_USER, SERVICE_SERVER_PASSWORD);
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
    private boolean createServerDb() {
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
                System.out.println("createServerDb() - SQL conn close(1) SQLException: " + sqlEx.getMessage());
                LOG.error("createServerDb() - SQL conn close(1) SQLException: " + sqlEx.getMessage());
            }

            return false;
        } catch (SQLException ex) {
            // handle any errors

            vendorError = ex.getErrorCode();
            if (vendorError != MysqlErrorNumbers.ER_BAD_DB_ERROR) {
                System.out.println("createServerDb(1) - JDBC connect: " + jdbcConnect);
                System.out.println("createServerDb(1) - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());
                LOG.error("createServerDb(1) - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());

                return false;
            }
        }

        /*
         ** If the code reaches here, the ServiceServersDb database needs to be created
         */
        System.out.println("Create ServiceServersDb");

        if (isDockerImage() || isKubernetesImage()) {
            jdbcConnect = kubeJDBCConnect;
        } else {
            jdbcConnect = execJDBCConnect;
        }

        try {
            conn = DriverManager.getConnection(jdbcConnect, userName, password);
        } catch (SQLException ex) {
            vendorError = ex.getErrorCode();
            System.out.println("createServerDb(2) - JDBC connect: " + jdbcConnect);
            System.out.println("createServerDb(2) - creating database - SQLException: " + ex.getMessage() +
                    " SQLState: " + ex.getSQLState() + " " + vendorError);
            LOG.error("createServerDb(2) - creating database - SQLException: " + ex.getMessage() +
                    " SQLState: " + ex.getSQLState() + " " + vendorError);
            return false;
        }

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute("CREATE DATABASE " + SERVICE_SERVERS_DB_NAME);

                stmt.execute(CREATE_SERVICE_SERVERS_USER);
                stmt.execute(PRIVILEDGE_SERVICE_SERVERS_USER);
            } catch (SQLException sqlEx) {
                System.out.println("createServerDb() - create database - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("createServerDb() - create database - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("createServerDb() - close - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        LOG.error("createServerDb() - close - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
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

        LOG.info("ServiceServersDb database created");
        return true;
    }

    /*
    ** This will drop the local storage server table. This are managed separately from the
    **   Kubernetes storage server tables since those can change with each startup
     */
    public void dropStorageServerTables() {
        System.out.println("dropLocalStorageServerTables() WebServerFlavor: " + flavor.toString());
        LOG.info("dropLocalStorageServerTables() WebServerFlavor: " + flavor.toString());

        Connection conn = getServersDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(DROP_SERVER_IDENTIFIER_TABLE);
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
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
        }
    }

    /*
    ** This performs an initial setup of the tables associated with the Servers if they do not already
    **   exist (meaning they are created if the database did not exist).
     */
    private void createServerTable() {

        Connection conn = getServersDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(CREATE_SERVER_TABLE);
            } catch (SQLException sqlEx) {
                LOG.error("createServerTable() SQLException: " + sqlEx.getMessage());
                System.out.println("createServerTable() SQLState: " + sqlEx.getSQLState());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("createServerTable() stmt.close() SQLException: " + sqlEx.getMessage());
                        System.out.println("createServerTable stmt.close() SQLState: " + sqlEx.getSQLState());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
        }
    }


    /*
     ** This performs an initial setup of the tables associated with the chunks that are used to store data on a
     **   Storage Server if they do not already exist (meaning they are created if the database did not exist).
     */
    private void createChunkTable() {

        Connection conn = getServersDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(CREATE_CHUNK_TABLE);
            } catch (SQLException sqlEx) {
                LOG.error("createChunkTable() SQLException: " + sqlEx.getMessage());
                System.out.println("createChunkTable() SQLState: " + sqlEx.getSQLState());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("createChunkTable() stmt.closer() SQLException: " + sqlEx.getMessage());
                        System.out.println("createChunkTable() stmt.close() SQLState: " + sqlEx.getSQLState());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
        }
    }


    /*
    ** This populates three initial records for the Storage Servers. These records are used to access the mock
    **   Storage Servers when running the test framework from within IntelliJ.
     */
    private void populateStorageServers(final int chunksPerStorageServer, final StorageTierEnum storageTier) {
        Connection conn = getServersDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(ADD_STORAGE_SERVER, Statement.RETURN_GENERATED_KEYS);
                for (int i = 0; i < 3; i++) {
                    stmt.setString(1, "storage-server-" + i);
                    stmt.setInt(2, STORAGE_SERVER);
                    stmt.setString(3, "localhost");
                    stmt.setInt(4, STORAGE_SERVER_TCP_PORT + i);
                    stmt.setInt(5, storageTier.toInt());
                    stmt.setInt(6, chunksPerStorageServer);
                    stmt.executeUpdate();

                    ResultSet rs = stmt.getGeneratedKeys();
                    if (rs.next()){
                        int serverId = rs.getInt(1);

                        ServerChunkMgr chunkMgr = new ServerChunkMgr(flavor);

                        chunkMgr.addServerChunks(serverId, CHUNKS_TO_ALLOCATE, RequestContext.getChunkSize());
                    }
                    rs.close();
                }
            } catch (SQLException sqlEx) {
                LOG.error("populateStorageServers() SQLException: " + sqlEx.getMessage());
                System.out.println("populateStorageServers() SQLException: " + sqlEx.getMessage());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("populateStorageServers() stmt.close() SQLException: " + sqlEx.getMessage());
                        System.out.println("populateStorageServers() stmt.close()  SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
        }
    }

    /*
     ** This obtains the UID for a particular table entry. It will return NULL if the entry does not exist.
     */
    public String getUID(final String uidQueryStr) {
        String uid = null;

        Connection conn = getServersDbConn();

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

                        if (count != 1) {
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
            closeStorageServerDbConn(conn);
        }

        return uid;
    }

}
