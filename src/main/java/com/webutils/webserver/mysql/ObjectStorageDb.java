package com.webutils.webserver.mysql;

import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/*
** This is responsible for managing the database used to hold information about the Objects.
**
** There are the following tables as part of the ObjectStorageDb:
**
**    Tenancy - This is a way for customers to separate out their data and provide controls over access to the data
**
**    Namespace - This is a way for a customer to divide up their tenancy and provide another level of control over
**      access to data within the namespace. A customer can have multiple Namespaces within a Tenancy.
**
**    Bucket - A Bucket is a way to organize similar Objects in a group. There can be multiple Buckets within a Namespace.
**    Tag (the Tags are associated with a Bucket) - Tags are a way to provide specialized searches for Buckets within a
**      Namespace.
**
**    Object - Objects are stored within a Bucket
**    Chunk - Chunks describe where the data for an Object is stored on the Storage Servers. There may be multiple
**      Chunks per Object depending upon the redundancy and the size of the Object
 */
public class ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectStorageDb.class);

    protected static final String kubeJDBCDatabaseConnect = "jdbc:mysql://host.docker.internal/objectStorageDb?serverTimeZone=US/Mountain";
    protected static final String execJDBCDatabaseConnect = "jdbc:mysql://localhost/objectStorageDb?serverTimezone=US/Mountain";
    protected static final String kubeJDBCConnect = "jdbc:mysql://host.docker.internal/?serverTimeZone=US/Mountain";
    protected static final String execJDBCConnect = "jdbc:mysql://localhost/?serverTimezone=US/Mountain";

    protected static final String objectStorageDbName = "objectStorageDb";
    protected static final String objectStorageUser = "objectstorageuser";
    protected static final String objectStoragePassword = "rwt25nX1";


    /*
     ** Is this running within a Docker Container?
     */
    protected final WebServerFlavor flavor;

    ObjectStorageDb(final WebServerFlavor flavor) {
        LOG.info("ObjectStorageDb() WebServerFlavor: " + flavor.toString());
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
     ** This obtains a connection to communicate with the MySQL ObjectStorageDb database.
     */
    public Connection getObjectStorageDbConn() {
        Connection conn;
        String jdbcConnect;

        if (isDockerImage() || isKubernetesImage()) {
            jdbcConnect = kubeJDBCDatabaseConnect;
        } else {
            jdbcConnect = execJDBCDatabaseConnect;
        }

        try {
            conn = DriverManager.getConnection(jdbcConnect, objectStorageUser, objectStoragePassword);
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("JDBC connect: " + jdbcConnect);
            System.out.println("getObjectStorageDbConn() - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());
            LOG.error("getObjectStorageDbConn() - SQLException: " + ex.getMessage() + " SQLState: " + ex.getSQLState());
            return null;
        }

        return conn;
    }

    /*
     ** This closes the connection used to communicate with the MySQL ObjectStorageDb database.
     */
    public void closeObjectStorageDbConn(final Connection conn) {

        /*
         ** Add safety check in case this was called when the Connection was not actually
         **   created in the first place.
         */
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException sqlEx) {
                // handle any errors
                System.out.println("closeObjectStorageDbConn() - SQL conn close(2) SQLException: " + sqlEx.getMessage() + "  SQLState: " + sqlEx.getSQLState());
                LOG.error("closeObjectStorageDbConn() - SQL conn close(2) SQLException: " + sqlEx.getMessage() + "  SQLState: " + sqlEx.getSQLState());
            }
        }
    }

}
