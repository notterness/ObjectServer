package com.webutils.webserver.mysql;

import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/*
 ** This class is used to access POD information for Docker Images that are running within the POD.
 **
 ** NOTE: This is only used by the Object Storage Docker Image running within the webutils-site POD deployment.
 */
public class K8LocalDbInfo extends DbSetup {

    private static final Logger LOG = LoggerFactory.getLogger(K8LocalDbInfo.class);

    private static final String getK8LocalStorageServers = "SELECT * FROM k8LocalStorageServerInfo";
    private static final String getK8LocalStorageServer = "SELECT * FROM k8LocalStorageServerInfo WHERE serverName='";
    private static final String getServerQueryEnd = "';";


    public K8LocalDbInfo(final WebServerFlavor flavor) {
        super(flavor);

        LOG.info("K8LocalDbInfo() WebServerFlavor: " + flavor.toString());
    }

    /*
     ** This will return a specific server (if one exists) based upon the passed in serverName. Currently, the only
     **   server names that are valid are:
     **     "storage-server-1"
     **     "storage-server-2"
     **     "storage-server-3"
     */
    public boolean getServer(final String serverName, final List<ServerIdentifier> serverList) {
        LOG.info("K8LocalDbInfo getServer() serverName: " + serverName + " dockerImage: " + isDockerImage() +
                " k8Image: " + isKubernetesImage());

        String queryStr = getK8LocalStorageServer + serverName + getServerQueryEnd;

        return getServer(queryStr, serverList);
    }

    public boolean getStorageServers(final List<ServerIdentifier> servers, final int chunkNumber) {
        LOG.info("K8LocalDbInfo getStorageServers() dockerImage: " + isDockerImage() + " k8Image: " +
                isKubernetesImage());

        return retrieveStorageServers(getK8LocalStorageServers, servers, chunkNumber);
    }
}
