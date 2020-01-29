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

/*
** This provides a common place to obtain information about the Storage Servers from the MySql database
 */
public class StorageServerDbOps {

    private static final Logger LOG = LoggerFactory.getLogger(StorageServerDbOps.class);

    private static final String getLocalStorageServers = "SELECT * FROM localServerIdentifier";
    private static final String getDockerStorageServers = "SELECT * FROM dockerServerIdentifier";

    private final DbSetup dbSetup;

    public StorageServerDbOps(final DbSetup dbSetup) {
        this.dbSetup = dbSetup;
    }

    public boolean getStorageServers(final List<ServerIdentifier> servers, final int chunkNumber) {
        boolean success = false;
        Connection conn = dbSetup.getStorageServerDbConn();

        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();

                String queryStr;
                if (dbSetup.isDockerImage()) {
                    queryStr = getDockerStorageServers;
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

                                ServerIdentifier server = new ServerIdentifier(inetAddress, rs.getInt(4), chunkNumber);
                                servers.add(server);

                                LOG.info("StorageServer host: " + rs.getString(3) + " " + inetAddress.toString());
                            } catch (UnknownHostException ex) {
                                LOG.warn("Unknown host: " + rs.getString(3) + " " + ex.getMessage());
                                System.out.println("Unknown host: " + rs.getString(3) + " " + ex.getMessage());
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
