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
** This class is used to access POD information for resources that are running outside the POD. These resources
**   can be running in a Docker Container or a Kubernetes POD.
 */
public class K8PodServersMgr extends ServerIdentifierTableMgr {

    private static final Logger LOG = LoggerFactory.getLogger(K8PodServersMgr.class);

    private static final String GET_K8_POD_SERVERS = "SELECT serverName, K8PodServerIpAddr, K8SPodServerPort FROM ServerIdentifier";
    private static final String GET_K8_POD_SERVER = "SELECT serverName, K8PodServerIpAddr, K8SPodServerPort FROM ServerIdentifier WHERE serverName='";
    private static final String GET_QUERY_END = "';";

    private static final String UPDATE_K8_POD_SERVERS = "INSERT INTO k8LocalStorageServerInfo(k8PodServerIpAddr, k8PosServerPort) " +
            "VALUES(?, ?)";
    private static final String UPDATE_K8_POD_SERVER = "INSERT INTO k8LocalStorageServerInfo(k8PodServerIpAddr, k8PosServerPort) " +
            "VALUES(?, ?) WHERE serverName = ?";

    private static final String STORAGE_SERVER_REQUEST = "SELECT serverId, serverName, k8PodServerIpAddr, k8PosServerPort FROM ServerIdentifier " +
            "WHERE serverType = ? AND storageTier = ? AND disabled = 0 ORDER BY usedChunks ASC, totalAllocations ASC, lastAllocationTime DESC";


    /*
    ** This class is only used by the Client Test code.
    **
    ** The two possible values for WebServerFlavor are:
    **
    **   WebServerFlavor.INTEGRATION_DOCKER_TESTS - This is when everything is running in different Docker images. This
    **     is not currently used for anything.
    **   WebServerFlavor.INTEGRATION_KUBERNETES_TESTS - These are the tests run against the Storage Server and
    **     Object Server running in a different Kubernetes POD. The test client is running in a Docker image outside the
    **     Kubernetes POD.
    */
    public K8PodServersMgr(final WebServerFlavor flavor) {
        super(flavor);

        LOG.info("K8PodServersMgr() WebServerFlavor: " + flavor.toString());
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
        LOG.info("K8PodServersMgr getServer() serverName: " + serverName + " dockerImage: " + isDockerImage() +
                " k8Image: " + isKubernetesImage());

        String queryStr = GET_K8_POD_SERVER + serverName + GET_QUERY_END;

        return super.retrieveServers(queryStr, serverList,0);
    }

    /*
    ** This returns a list of Storage Servers and their IP addresses and Ports they are visible on.
     */
    public boolean getStorageServers(final List<ServerIdentifier> servers, final int chunkNumber) {
        LOG.info("K8PodServersMgr getStorageServers() dockerImage: " + isDockerImage() + " k8Image: " +
                isKubernetesImage());

        return retrieveServers(GET_K8_POD_SERVERS, servers, chunkNumber);
    }

    /*
    ** This updates the IP addresses and Ports for the Servers that will be accessed by the Client Test code.
    **
    ** NOTE: Since this is only called by the Client Test code, the expectation is that the StorageServers database is
    **   already setup and the tables are created.
     */
    public void checkAndSetupStorageServers() {

        KubernetesInfo k8Info = new KubernetesInfo(flavor);

        String k8IpAddr = k8Info.waitForInternalK8Ip();
        if (k8IpAddr != null) {
            /*
             ** Always drop and repopulate the Kubernetes storage server tables since the IP address
             **   for the POD will likely change between startups.
             ** Recreate the tables that provide the IP address and Port mappings for the Storage Servers when accessed
             **   from within the POD.
             */
            clearK8PodServers();

            /*
             ** Obtain the list of Storage Servers and their NodePorts (this is the port number that is exposed outside the
             **   POD).
             */
            Map<String, Integer> storageServersInfo = new Hashtable<>();
            try {
                int count = k8Info.getStorageServerPorts(storageServersInfo);
                if (count != 0) {
                    populateK8PodServers(k8IpAddr, storageServersInfo);
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
     ** This populates three initial records for the Storage Servers for access from outside the Kubernetes POD. These
     **   records are used by the Client Test code to access the Object Server and the mock Storage Servers running
     **   within the same Kubernetes POD (that will be accessed by a Docker image that is running outside the POD).
     */
    private void populateK8PodServers(final String k8IpAddr, final Map<String, Integer> storageServersInfo) {
        if (k8IpAddr != null) {

            LOG.info("populateK8Servers() IP Address: " + k8IpAddr);
            Connection conn = getServersDbConn();

            if (conn != null) {
                PreparedStatement stmt = null;

                try {
                    stmt = conn.prepareStatement(UPDATE_K8_POD_SERVER);

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
     ** This will drop the Kubernetes server values that provide the external addresses. These are managed separately
     **   since those can change with each startup of the Kubernetes POD.
     */
    public void clearK8PodServers() {
        System.out.println("clearK8Servers() WebServerFlavor: " + flavor.toString());
        LOG.info("clearK8Servers() WebServerFlavor: " + flavor.toString());

        Connection conn = getServersDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(UPDATE_K8_POD_SERVERS);
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
