package com.webutils.webserver.mysql;

import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

/*
** This class is responsible for creating the tables used to store information about the Objects
 */
public class CreateObjectStorageTables extends ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(CreateObjectStorageTables.class);

    /*
    ** This is the Tenancy table. A tenancy is unique per customer, but the tenancyName does not have to be unique
    **   across all customers.
     */
    private static final String createTenancyTable = "CREATE TABLE IF NOT EXISTS customerTenancy (" +
            " tenancyId INT AUTO_INCREMENT, " +
            " tenancyName VARCHAR(255) NOT NULL, " +
            " customerName VARCHAR(255) NOT NULL, " +
            " tenancyUID BINARY(16) NOT NULL, " +
            " PRIMARY KEY (tenancyName)," +
            " KEY (tenancyId)" +
            ")";

    /*
    ** There is one namespace per tenancy per region. The namespace spans all of the Compartments within a particular
    **    region.
     */
    private static final String createNamespaceTable = "CREATE TABLE IF NOT EXISTS customerNamespace (" +
            " namespaceId INT AUTO_INCREMENT, " +
            " name VARCHAR(256) NOT NULL," +
            " region VARCHAR(128) NOT NULL," +
            " namespaceUID BINARY(16) NOT NULL," +
            " tenancyId INT NOT NULL, " +
            " FOREIGN KEY (tenancyId)" +
            "   REFERENCES customerTenancy(tenancyId)" +
            "   ON DELETE CASCADE," +
            " PRIMARY KEY (namespaceId)" +
            ")";

    /*
    **
     */
    private static final String createBucketTable = "CREATE TABLE IF NOT EXISTS bucket (" +
            " bucketId INT AUTO_INCREMENT, " +
            " bucketName VARCHAR(255) NOT NULL, " +
            " compartmentId VARCHAR(255) NOT NULL, " +
            " objectEventsEnabled INT NOT NULL," +
            " bucketUID BINARY(16) NOT NULL," +
            " namespaceId INT NOT NULL," +
            " FOREIGN KEY (namespaceId)" +
            "   REFERENCES customerNamespace(namespaceId)" +
            "   ON DELETE CASCADE," +
            " PRIMARY KEY (bucketId)" +
            ")";

    private static final String createBucketTagTable = "CREATE TABLE IF NOT EXISTS bucketTags (" +
            " tagName VARCHAR(64) NOT NULL," +
            " subTagName VARCHAR(64)," +
            " tagKey VARCHAR(255) NOT NULL," +
            " tagValue VARCHAR(255) NOT NULL," +
            " bucketId INT NOT NULL," +
            " FOREIGN KEY (bucketId)" +
            "   REFERENCES bucket(bucketId)" +
            "   ON DELETE CASCADE" +
            ")";

    private static final String createObjectTable = "CREATE TABLE IF NOT EXISTS object (" +
            " objectId INT AUTO_INCREMENT," +
            " objectName VARCHAR(255) NOT NULL," +
            " opcClientRequestId VARCHAR(256)," +
            " contentLength INT NOT NULL," +
            " chunkRedundancy INT NOT NULL," +
            " objectUID BINARY(16) NOT NULL," +
            " bucketId INT NOT NULL," +
            " namespaceId INT NOT NULL," +
            " FOREIGN KEY (bucketId)" +
            "   REFERENCES bucket(bucketId)" +
            "   ON DELETE CASCADE," +
            " FOREIGN KEY (namespaceId)" +
            "   REFERENCES customerNamespace(namespaceId)" +
            "   ON DELETE CASCADE," +
            " PRIMARY KEY (objectId)" +
            ")";

    private static final String createChunkTable = "CREATE TABLE IF NOT EXISTS storageChunk (" +
            " offset INT NOT NULL," +
            " length INT NOT NULL," +
            " chunkIndex INT NOT NULL," +
            " storageServerName VARCHAR(64) NOT NULL," +
            " serverIp VARCHAR(64) NOT NULL," +
            " storageLocation VARCHAR(128) NOT NULL," +
            " dataWritten INT NOT NULL," +
            " ownerObject INT NOT NULL," +
            " FOREIGN KEY (ownerObject)" +
            "   REFERENCES object(objectId)" +
            "   ON DELETE CASCADE" +
            ")";

    private static final String createObjectStorageDatabase = "CREATE DATABASE " + objectStorageDbName;

    protected static final String userName = "root";
    protected static final String password = "ktm300exc";

    private static final String createObjectStorageUser = "CREATE USER IF NOT EXISTS '" + objectStorageUser + "'@'localhost'" +
            " IDENTIFIED BY '" + objectStoragePassword + "'";

    private static final String privilegeObjectStorageUser = "GRANT ALL PRIVILEGES ON " + objectStorageDbName +
            ".* TO '" + objectStorageUser + "'@'localhost'";


    private final LinkedList<String> tableCreates;

    public CreateObjectStorageTables(final WebServerFlavor flavor) {
        super(flavor);

        tableCreates = new LinkedList<>();
        tableCreates.add(createTenancyTable);
        tableCreates.add(createNamespaceTable);
        tableCreates.add(createBucketTable);
        tableCreates.add(createBucketTagTable);
        tableCreates.add(createObjectTable);
        tableCreates.add(createChunkTable);
    }

    /*
    **
     */
    public void checkAndSetupObjectStorageDb() {
        if (createObjectStorageDb()) {
            createObjectStorageTables();
        }
    }


    /*
    ** Create the tables
     */
    private void createObjectStorageTables() {
        LOG.info("createObjectStorageTables() WebServerFlavor: " + flavor.toString());

        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            Statement stmt = null;

            for (String createTableStr : tableCreates) {

                try {
                    stmt = conn.createStatement();
                    stmt.execute(createTableStr);
                } catch (SQLException sqlEx) {
                    LOG.error("createObjectStorageTables() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                    LOG.error("Bad SQL command: " + createTableStr);
                    System.out.println("SQLException: " + sqlEx.getMessage());
                } finally {
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException sqlEx) {
                            LOG.error("createObjectStorageTables() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                            System.out.println("SQLException: " + sqlEx.getMessage());
                        }

                        stmt = null;
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeObjectStorageDbConn(conn);
        }
    }


    /*
     ** This creates the database used to hold the Storage Server locations if the database does not already
     **   exist.
     */
    private boolean createObjectStorageDb() {
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
                System.out.println("createObjectStorageDb() - SQL conn close(1) SQLException: " + sqlEx.getMessage());
                LOG.error("createObjectStorageDb() - SQL conn close(1) SQLException: " + sqlEx.getMessage());
            }

            LOG.info("createObjectStorageDb() ObjectStorageDb database found");

            return false;
        } catch (SQLException ex) {
            // handle any errors

            vendorError = ex.getErrorCode();
            System.out.println("createObjectStorageDb(1) - JDBC connect: " + jdbcConnect);
            System.out.println("createObjectStorageDb(1) - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());
            LOG.error("createObjectStorageDb(1) - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());

            if (vendorError != MysqlErrorNumbers.ER_BAD_DB_ERROR) {
                return false;
            }
        }

        /*
         ** Check if the Database needs to be created
         */
        if (vendorError == MysqlErrorNumbers.ER_BAD_DB_ERROR) {
            LOG.info("createObjectStorageDb() create database");

            if (isDockerImage() || isKubernetesImage()) {
                jdbcConnect = kubeJDBCConnect;
            } else {
                jdbcConnect = execJDBCConnect;
            }

            try {
                conn = DriverManager.getConnection(jdbcConnect, userName, password);
            } catch (SQLException ex) {
                vendorError = ex.getErrorCode();
                System.out.println("createObjectStorageDb(2) - JDBC connect: " + jdbcConnect);
                System.out.println("createObjectStorageDb(2) - creating database - SQLException: " + ex.getMessage() +
                        " SQLState: " + ex.getSQLState() + " " + vendorError);
                LOG.error("createObjectStorageDb(2) - creating database - SQLException: " + ex.getMessage() +
                        " SQLState: " + ex.getSQLState() + " " + vendorError);
                return false;
            }

            if (conn != null) {
                Statement stmt = null;

                /*
                ** For clarity and better handling of errors, may want to separate out the different execute statements. For
                **   example, if the Object Storage user has laready been created, it will cause an exception and return
                **   a vendor error of 1396.
                 */
                try {
                    stmt = conn.createStatement();
                    stmt.execute(createObjectStorageDatabase);

                    stmt.execute(createObjectStorageUser);

                    stmt.execute(privilegeObjectStorageUser);
                } catch (SQLException sqlEx) {
                    System.out.println("createObjectStorageDb() - create database - SQLException: " + sqlEx.getMessage() + " vendorError: " + sqlEx.getErrorCode());
                    LOG.error("createObjectStorageDb() - create database - SQLException: " + sqlEx.getMessage() + " vendorError: " + sqlEx.getErrorCode());
                    return false;
                }
                finally {
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException sqlEx) {
                            System.out.println("createObjectStorageDb() - close - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                            LOG.error("createObjectStorageDb() - close - SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
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
        }

        LOG.info("createObjectStorageDb() ObjectStorageDb database created");
        return true;
    }

}
