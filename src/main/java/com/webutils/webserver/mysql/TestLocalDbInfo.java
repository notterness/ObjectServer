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
public class TestLocalDbInfo extends DbSetup {

    private static final Logger LOG = LoggerFactory.getLogger(TestLocalDbInfo.class);

    private static final String getLocalStorageServers = "SELECT * FROM localServerIdentifier";
    private static final String getLocalStorageServer = "SELECT * FROM localServerIdentifier WHERE serverName='";
    private static final String getServerQueryEnd = "';";


    public TestLocalDbInfo(final WebServerFlavor flavor) {
        super(flavor);

        LOG.info("TestLocalDbInfo() WebServerFlavor: " + flavor.toString());
    }

    /*
     ** This will return a specific server (if one exists) based upon the passed in serverName. Currently, the only
     **   server names that are valid are:
     **     "storage-server-1"
     **     "storage-server-2"
     **     "storage-server-3"
     */
    public boolean getServer(final String serverName, final List<ServerIdentifier> serverList) {
        LOG.info("TestLocalDbInfo getServer() serverName: " + serverName + " dockerImage: " + isDockerImage() +
                " k8Image: " + isKubernetesImage());

        String queryStr = getLocalStorageServer + serverName + getServerQueryEnd;

        return getServer(queryStr, serverList);
    }

    /*
     ** This returns a list of Storage Servers and their IP addresses and Ports they are visible on.
     */
    public boolean getStorageServers(final List<ServerIdentifier> servers, final int chunkNumber) {
        LOG.info("TestLocalDbInfo getStorageServers() dockerImage: " + isDockerImage() + " k8Image: " +
                isKubernetesImage());

        return retrieveStorageServers(getLocalStorageServers, servers, chunkNumber);
    }
}
