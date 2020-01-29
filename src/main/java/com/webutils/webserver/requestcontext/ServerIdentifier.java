package com.webutils.webserver.requestcontext;

import java.net.InetAddress;

/*
** This is used to uniquely identify a Storage Server and a chunk operation
 */
public class ServerIdentifier {

    private final InetAddress storageServerIpAddress;
    private final int storageServerTcpPort;
    private final int chunkNumber;

    public ServerIdentifier(final InetAddress ipAddress, final int tcpPort, final int chunkNumber) {
        this.storageServerIpAddress = ipAddress;
        this.storageServerTcpPort = tcpPort;
        this.chunkNumber = chunkNumber;
    }

    public InetAddress getStorageServerIpAddress() {
        return storageServerIpAddress;
    }

    public int getStorageServerTcpPort() {
        return storageServerTcpPort;
    }

    public int getChunkNumber() {
        return chunkNumber;
    }
}
