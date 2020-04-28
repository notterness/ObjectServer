package com.webutils.webserver.requestcontext;

import com.webutils.webserver.http.HttpResponseInfo;

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

    private String chunkLocation;

    private String md5Digest;

    /*
    ** This is used to handle the HTTP Information that is parsed out of the response from the request to this
    **   server.
     */
    private HttpResponseInfo httpInfo;

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

    public void setChunkLocation(final String location) { chunkLocation = location; }
    public String getChunkLocation() { return "test"; }

    public void setMd5Digest(final String digest) {
        md5Digest = digest;
    }

    public String getMd5Digest() {
        return md5Digest;
    }

    /*
    **
     */
    public void setHttpInfo(final HttpResponseInfo httpResponseInfo) { httpInfo = httpResponseInfo; }

    public HttpResponseInfo getHttpInfo() { return httpInfo; }
}
