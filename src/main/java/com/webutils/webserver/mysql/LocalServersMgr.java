package com.webutils.webserver.mysql;

import com.webutils.webserver.http.StorageTierEnum;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/*
 ** This class is used to access information about resoures that are running in a local Java executable. This is used
 **   for testing purposes within the IntelliJ framework.
 */
public class LocalServersMgr extends ServerIdentifierTableMgr {

    private static final Logger LOG = LoggerFactory.getLogger(LocalServersMgr.class);

    private static final String getLocalStorageServers = "SELECT serverName, LocalServerIpAddr, LocalServerPort FROM ServerIdentifier";
    private static final String GET_LOCAL_STORAGE_SERVER = "SELECT serverName, LocalServerIpAddr, LocalServerPort FROM ServerIdentifier WHERE serverName='";
    private static final String getServerQueryEnd = "'";

    private static final String STORAGE_SERVER_REQUEST = "SELECT serverId, serverName, localServerIpAddr, localServerPort FROM ServerIdentifier " +
            "WHERE serverType = ? AND storageTier = ? AND disabled = 0 ORDER BY usedChunks ASC, totalAllocations ASC, lastAllocationTime ASC";

    public LocalServersMgr(final WebServerFlavor flavor) {
        super(flavor);

        LOG.info("LocalServersMgr() WebServerFlavor: " + flavor.toString());
    }

    /*
     ** This will return a specific server (if one exists) based upon the passed in serverName. Currently, the only
     **   server names that are valid are (this is based upon the tests, but the list can easily be extended):
     **     "storage-server-1"
     **     "storage-server-2"
     **     "storage-server-3"
     **     "storage-server-4"
     **     "chunk-mgr-service"
     **     "object-server"
     **     "account-mgr-service"
     **
     ** NOTE: For simplicity, these can be easily found by the following MySQL commands when using the MySQL CLI:
     **    USE ServiceServersDb;
     **    SELECT * FROM ServerIdentifier;
     */
    public int getServer(final String serverName, final List<ServerIdentifier> serverList) {
        LOG.info("LocalServersMgr getServer() serverName: " + serverName + " dockerImage: " + isDockerImage() +
                " k8Image: " + isKubernetesImage());

        String queryStr = GET_LOCAL_STORAGE_SERVER + serverName + getServerQueryEnd;

        return super.retrieveServers(queryStr, serverList, 0);
    }

    /*
     ** This returns a list of Storage Servers and their IP addresses and Ports they are visible on.
     */
    public int getStorageServers(final List<ServerIdentifier> servers, final int chunkNumber) {
        LOG.info("LocalServersMgr getStorageServers() dockerImage: " + isDockerImage() + " k8Image: " +
                isKubernetesImage());

        return retrieveServers(getLocalStorageServers, servers, chunkNumber);
    }

    public int getOrderedStorageServers(final List<ServerIdentifier> servers, final StorageTierEnum storageTier,
                                        final int chunkNumber) {
        int result;
        Connection conn = getServersDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(STORAGE_SERVER_REQUEST);
                stmt.setInt(1, STORAGE_SERVER);
                stmt.setInt(2, storageTier.toInt());

                result = executeGetOrderedStorageServers(stmt, servers, chunkNumber);
            } catch (SQLException sqlEx) {
                LOG.error("getOrderedStorageServers() SQLException: " + sqlEx.getMessage());
                System.out.println("getOrderedStorageServers() SQLException: " + sqlEx.getMessage());
                result = HttpStatus.INTERNAL_SERVER_ERROR_500;
            }

            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("getOrderedStorageServers() stmt.close() SQLException: " + sqlEx.getMessage());
                        System.out.println("getOrderedStorageServers() stmt.close()  SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
        } else {
            LOG.warn("getOrderedStorageServers() Unable to obtain connection");
            result = HttpStatus.INTERNAL_SERVER_ERROR_500;
        }

        return result;
    }

}
