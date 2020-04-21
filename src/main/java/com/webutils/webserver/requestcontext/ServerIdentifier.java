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

    private int chunkOffset;
    private int chunkLength;

    /*
    ** The following is the unique ID that can be used to access a chunk's information that is written into the
    **   database.
     */
    private int chunkUniqueId;

    public ServerIdentifier(final String serverName, final InetAddress ipAddress, final int tcpPort, final int chunkNumber) {
        this.serverName = serverName;
        this.serverIpAddress = ipAddress;
        this.serverTcpPort = tcpPort;
        this.chunkNumber = chunkNumber;

        chunkUniqueId = -1;
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

    public void setOffset(final int offset) {
        chunkOffset = offset;
    }

    public int getOffset() { return chunkOffset; }

    public void setLength(final int length) {
        chunkLength = length;
    }

    public int getLength() { return chunkLength; }

    public void setChunkId(final int id) {
        chunkUniqueId = id;
    }

    public int getChunkId() { return chunkUniqueId; }

    public String getChunkLocation() { return "test"; }
}
