package com.webutils.webserver.mysql;

import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

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

    /*
     ** This obtains the UID for a particular table entry. It will return NULL if the entry does not exist.
     */
    public String getUID(final String uidQueryStr) {
        String uid = null;

        Connection conn = getObjectStorageDbConn();

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
            closeObjectStorageDbConn(conn);
        }

        return uid;
    }

    /*
     ** This obtains the AUTO_INCREMENT Id from a particular table entry. It will return -1 if the entry does not exist.
     */
    public int getId(final String idQueryStr) {
        int id = -1;

        Connection conn = getObjectStorageDbConn();

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
                            LOG.warn("getId() too many responses count: " + count);
                            LOG.warn(idQueryStr);

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
            closeObjectStorageDbConn(conn);
        }

        return id;
    }

    /*
     ** This obtains the a single String from a particular table entry. It will return null if the entry does not exist.
     */
    public String getSingleStr(final String createTimeQueryStr) {
        String requestedStr = null;

        Connection conn = getObjectStorageDbConn();

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
            closeObjectStorageDbConn(conn);
        }

        return requestedStr;
    }

    public boolean executeSqlStatement(final String sqlQuery) {
        boolean success = true;
        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(sqlQuery);
            } catch (SQLException sqlEx) {
                success = false;
                LOG.error("executeSqlStatement() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + sqlQuery);
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("executeSqlStatement() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
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

}
