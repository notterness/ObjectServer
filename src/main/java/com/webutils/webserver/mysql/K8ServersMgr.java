package com.webutils.webserver.mysql;

import com.webutils.webserver.http.StorageTierEnum;
import com.webutils.webserver.kubernetes.KubernetesInfo;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 ** This class is used to access POD information for Docker Images that are running within the Kubernetes POD.
 **
 ** The two possible values for WebServerFlavor are:
 **
 **   WebServerFlavor.DOCKER_OBJECT_SERVER_TEST - This is when everything is running in different Docker images. This
 **     is not currently used for anything.
 **   WebServerFlavor.KUBERNETES_OBJECT_SERVER_TEST - This is teh default configuration and is when the Object Server,
 **     ChunkMgr and Storage Servers are all running within a Kubernetes POD.
 **
 ** NOTE: This is only used by the Object Storage Docker Image running within the webutils-site Kubernetes POD deployment.
 */
public class K8ServersMgr extends ServerIdentifierTableMgr {

    private static final Logger LOG = LoggerFactory.getLogger(K8ServersMgr.class);

    private static final String GET_K8_STORAGE_SERVERS = "SELECT serverName, K8ServerIpAddr, K8ServerPort FROM ServerIdentifier";
    private static final String GET_K8_STORAGE_SERVER = "SELECT serverName, K8ServerIpAddr, K8ServerPort  FROM ServerIdentifier WHERE serverName = '";
    private static final String GET_SERVER_QUERY_END = "'";

    private static final String UPDATE_K8_SERVER = "INSERT INTO k8PodServiceInfo(k8ServerIpAddr, k8ServerPort) " +
            "VALUES(?, ?) WHERE serverName = ?";
    private static final String UPDATE_K8_SERVERS = "INSERT INTO k8PodServiceInfo(k8ServerIpAddr, k8ServerPort) " +
            "VALUES(?, ?)";

    private static final String STORAGE_SERVER_REQUEST = "SELECT serverId, serverName, k8ServerIpAddr, k8ServerPort FROM ServerIdentifier " +
            "WHERE serverType = ? storageTier = ? ORDER BY usedChunks ASC, totalAllocations ASC, lastAllocationTime DESC";

    public K8ServersMgr(final WebServerFlavor flavor) {
        super(flavor);

        LOG.info("K8ServersMgr() WebServerFlavor: " + flavor.toString());
    }

    /*
     ** This will return a specific server (if one exists) based upon the passed in serverName. Currently, the only
     **   server names that are valid are:
     **     "storage-server-1"
     **     "storage-server-2"
     **     "storage-server-3"
     */
    public boolean getServer(final String serverName, final List<ServerIdentifier> serverList) {
        LOG.info("K8ServersMgr getServer() serverName: " + serverName + " dockerImage: " + isDockerImage() +
                " k8Image: " + isKubernetesImage());

        String queryStr = GET_K8_STORAGE_SERVER + serverName + GET_SERVER_QUERY_END;

        return super.retrieveServers(queryStr, serverList, 0);
    }

    public boolean getStorageServers(final List<ServerIdentifier> servers, final int chunkNumber) {
        LOG.info("K8ServersMgr getStorageServers() dockerImage: " + isDockerImage() + " k8Image: " +
                isKubernetesImage());

        return retrieveServers(GET_K8_STORAGE_SERVERS, servers, chunkNumber);
    }

    /*
     ** This checks if the databases are created and creates them if needed. If the database is created, it will
     **   then create the tables. The tables that are created are dependent upon who the caller is and the application
     **   type that is running (test code within IntelliJ, Docker image or Kubernetes POD).
     */
    public void checkAndSetupStorageServers() {

        super.checkAndSetupStorageServers();

        KubernetesInfo k8Info = new KubernetesInfo(flavor);

        String k8IpAddr = k8Info.waitForInternalK8Ip();
        if (k8IpAddr != null) {
            /*
             ** Always drop and repopulate the Kubernetes storage server tables since the IP address
             **   for the POD will likely change between startups.
             ** Recreate the tables that provide the IP address and Port mappings for the Storage Servers when accessed
             **   from within the POD.
             */
            clearK8Servers();

            /*
             ** Obtain the list of Storage Servers and their NodePorts (this is the port number that is exposed outside the
             **   POD).
             */
            Map<String, Integer> storageServersInfo = new Hashtable<>();
            try {
                int count = k8Info.getStorageServerPorts(storageServersInfo);
                if (count != 0) {
                    populateK8Servers(k8IpAddr, storageServersInfo);
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
     ** This populates three initial records for the Storage Servers for access from within the Kubernetes POD. These
     **   records are used by the Object Server to access the mock Storage Servers running within the same POD.
     */
    private void populateK8Servers(final String k8IpAddr, final Map<String, Integer> storageServersInfo) {
        if (k8IpAddr != null) {

            LOG.info("populateK8Servers() IP Address: " + k8IpAddr);
            Connection conn = getServersDbConn();

            if (conn != null) {
                PreparedStatement stmt = null;

                try {
                    stmt = conn.prepareStatement(UPDATE_K8_SERVER);

                    Set< Map.Entry< String,Integer> > servers = storageServersInfo.entrySet();
                    for (Map.Entry< String,Integer> server:servers)
                    {
                        stmt.setString(1, k8IpAddr);
                        stmt.setInt(2, server.getValue());
                        stmt.setString(3, server.getKey());
                        stmt.executeUpdate();
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
                    }
                }

                /*
                 ** Close out this connection as it was only used to create the database tables.
                 */
                closeStorageServerDbConn(conn);
            }
        } else {
            LOG.error("populateK8Servers() IP Address is NULL");
        }
    }

    /*
     ** This will drop the Kubernetes storage server values. These are managed separately from the
     **   since those can change with each startup
     */
    public void clearK8Servers() {
        System.out.println("clearK8Servers() WebServerFlavor: " + flavor.toString());
        LOG.info("clearK8Servers() WebServerFlavor: " + flavor.toString());

        Connection conn = getServersDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(UPDATE_K8_SERVERS);
                stmt.setNull(1, Types.VARCHAR);
                stmt.setNull(2, Types.INTEGER);
                stmt.executeUpdate();
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
                }
            }

            /*
             ** Close out this connection as it was only used to clear the Kubernetes database table.
             */
            closeStorageServerDbConn(conn);
        }
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