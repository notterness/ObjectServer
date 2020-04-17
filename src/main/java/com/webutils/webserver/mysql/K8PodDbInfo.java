package com.webutils.webserver.mysql;

import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/*
** This class is used to access POD information for resources that are running outside the POD. These resources
**   can be running in a Docker Container or a Kubernetes POD.
 */
public class K8PodDbInfo extends DbSetup {

    private static final Logger LOG = LoggerFactory.getLogger(K8PodDbInfo.class);

    private static final String getK8Servers = "SELECT * FROM k8PodServiceInfo";
    private static final String getK8Server = "SELECT * FROM k8PodServiceInfo WHERE serverName='";
    private static final String getServerQueryEnd = "';";


    public K8PodDbInfo(final WebServerFlavor flavor) {
        super(flavor);

        LOG.info("K8PodDbInfo() WebServerFlavor: " + flavor.toString());
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
        LOG.info("K8PodDbInfo getServer() serverName: " + serverName + " dockerImage: " + isDockerImage() +
                " k8Image: " + isKubernetesImage());

        String queryStr = getK8Server + serverName + getServerQueryEnd;

        return super.getServer(queryStr, serverList);
    }

    /*
    ** This returns a list of Storage Servers and their IP addresses and Ports they are visible on.
     */
    public boolean getStorageServers(final List<ServerIdentifier> servers, final int chunkNumber) {
        LOG.info("K8PodDbInfo getStorageServers() dockerImage: " + isDockerImage() + " k8Image: " +
                isKubernetesImage());

        return retrieveStorageServers(getK8Servers, servers, chunkNumber);
    }
}
