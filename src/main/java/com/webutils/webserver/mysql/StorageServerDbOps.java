package com.webutils.webserver.mysql;

import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/*
** This provides a common place to obtain information about the Storage Servers from the MySql database
 */
public class StorageServerDbOps {

    private static final Logger LOG = LoggerFactory.getLogger(StorageServerDbOps.class);

    private static final String getLocalStorageServers = "SELECT * FROM localServerIdentifier";
    private static final String getK8Servers = "SELECT * FROM k8PodServiceInfo";
    private static final String getK8LocalStorageServers = "SELECT * FROM k8LocalStorageServerInfo";

    private static final String getLocalStorageServer = "SELECT * FROM localServerIdentifier WHERE serverName='";
    private static final String getK8Server = "SELECT * FROM k8PodServiceInfo WHERE serverName='";
    private static final String getK8LocalStorageServer = "SELECT * FROM k8LocalStorageServerInfo WHERE serverName='";
    private static final String getServerQueryEnd = "';";

    private final DbSetup dbSetup;

    public StorageServerDbOps(final DbSetup dbSetup) {
        if (dbSetup == null) {
            LOG.error("StorageServerDbOps instantiated with (dbSetup == null)");
        }
        this.dbSetup = dbSetup;
    }

    public boolean getStorageServers(final List<ServerIdentifier> servers, final int chunkNumber) {
        boolean success = false;

        if (dbSetup == null) {
            LOG.error("StorageServerDbOps getStorageServers() (dbSetup == null)");
            return false;
        }

        LOG.info("StorageServerDbOps getStorageServers() dockerImage: " + dbSetup.isDockerImage() + " k8Image: " +
                dbSetup.isKubernetesImage());

        Connection conn = dbSetup.getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();

                String queryStr;
                if (dbSetup.isDockerImage()) {
                    queryStr = getK8Servers;
                } else if (dbSetup.isKubernetesImage()) {
                    queryStr = getK8LocalStorageServers;
                } else {
                    queryStr = getLocalStorageServers;
                }

                if (stmt.execute(queryStr)) {
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

            dbSetup.closeStorageServerDbConn(conn);
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
    public boolean getServer(final String serverName, final List<ServerIdentifier> serverList) {
        boolean success = false;

        if (dbSetup == null) {
            LOG.error("StorageServerDbOps getServer() (dbSetup == null)");
            return false;
        }

        LOG.info("StorageServerDbOps getServer() serverName: " + serverName + " dockerImage: " + dbSetup.isDockerImage() +
                " k8Image: " + dbSetup.isKubernetesImage());

        Connection conn = dbSetup.getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();

                String queryStr;
                if (dbSetup.isDockerImage()) {
                    queryStr = getK8Server + serverName + getServerQueryEnd;
                } else if (dbSetup.isKubernetesImage()) {
                    queryStr = getK8LocalStorageServer + serverName + getServerQueryEnd;
                } else {
                    queryStr = getLocalStorageServer + serverName + getServerQueryEnd;
                }

                if (stmt.execute(queryStr)) {
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

            dbSetup.closeStorageServerDbConn(conn);
            conn = null;
        }

        return success;
    }

}
