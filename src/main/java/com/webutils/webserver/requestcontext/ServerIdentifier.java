package com.webutils.webserver.requestcontext;

import java.net.InetAddress;

/*
** This is used to uniquely identify a Storage Server and a chunk operation
 */
public class ServerIdentifier {

    private final String serverName;
    private final InetAddress serverIpAddress;
    private final int serverTcpPort;
    private final int chunkNumber;

    public ServerIdentifier(final String serverName, final InetAddress ipAddress, final int tcpPort, final int chunkNumber) {
        this.serverName = serverName;
        this.serverIpAddress = ipAddress;
        this.serverTcpPort = tcpPort;
        this.chunkNumber = chunkNumber;
    }

    public String getServerName() { return serverName; }

    public InetAddress getServerIpAddress() {
        return serverIpAddress;
    }

    public int getServerTcpPort() {
        return serverTcpPort;
    }

    public int getChunkNumber() {
        return chunkNumber;
    }
}
