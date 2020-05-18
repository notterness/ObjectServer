package com.webutils.webserver.mysql;

import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/*
 ** This class is used to access information about resoures that are running in a local Java executable. This is used
 **   for testing purposes within the IntelliJ framework.
 */
public class LocalServersMgr extends ServerIdentifierTableMgr {

    private static final Logger LOG = LoggerFactory.getLogger(LocalServersMgr.class);

    private static final String getLocalStorageServers = "SELECT serverName, LocalServerIpAddr, LocalServerPort FROM ServerIdentifier";
    private static final String getLocalStorageServer = "SELECT serverName, LocalServerIpAddr, LocalServerPort FROM ServerIdentifier WHERE serverName='";
    private static final String getServerQueryEnd = "';";


    public LocalServersMgr(final WebServerFlavor flavor) {
        super(flavor);

        LOG.info("LocalServersMgr() WebServerFlavor: " + flavor.toString());
    }

    /*
     ** This will return a specific server (if one exists) based upon the passed in serverName. Currently, the only
     **   server names that are valid are:
     **     "storage-server-1"
     **     "storage-server-2"
     **     "storage-server-3"
     */
    public boolean getServer(final String serverName, final List<ServerIdentifier> serverList) {
        LOG.info("LocalServersMgr getServer() serverName: " + serverName + " dockerImage: " + isDockerImage() +
                " k8Image: " + isKubernetesImage());

        String queryStr = getLocalStorageServer + serverName + getServerQueryEnd;

        return super.retrieveServers(queryStr, serverList, 0);
    }

    /*
     ** This returns a list of Storage Servers and their IP addresses and Ports they are visible on.
     */
    public boolean getStorageServers(final List<ServerIdentifier> servers, final int chunkNumber) {
        LOG.info("LocalServersMgr getStorageServers() dockerImage: " + isDockerImage() + " k8Image: " +
                isKubernetesImage());

        return retrieveServers(getLocalStorageServers, servers, chunkNumber);
    }
}
