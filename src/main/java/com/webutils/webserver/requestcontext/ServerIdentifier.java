package com.webutils.webserver.requestcontext;

import com.webutils.webserver.http.HttpResponseInfo;
import org.eclipse.jetty.http.HttpStatus;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;

/*
** This is used to uniquely identify a Storage Server and a chunk operation
 */
public class ServerIdentifier {

    private final String serverName;
    private final InetAddress serverIpAddress;
    private final int serverTcpPort;

    /*
    ** The objectChunkNumber is which chunk relative to the object is being written (starting at 0). An object can
    **   be made up of 1 or more chunks that are distributed amongst the Storage Servers. In addition to the linear
    **   chunks (i.e. LBA 0 to N), depending upon the storage tier, each chunk may have 0 to 2 replicas. So, an object
    **   that requires 3 chunks to store all of its data may have a layout that looks like the following:
    **
    **      objectChunkNUmber  StorageServerName chunkNumber  chunkStartingLBA
    **             0            storage-server-0      5         (5 * chunkSize)
    **                          storage-server-2      2         (2 * chunkSize)
    **                          storage-server-6      11        (11 * chunkSize)
    **             1            storage-server-3      21        (21 * chunkSize)
    **                          storage-server-1      7         (7 * chunkSize)
    **                          storage-server-6      35        (35 * chunkSize)
    **             2            storage-server-0      23        (23 * chunkSize)
    **                          storage-server-5      9         (9 * chunkSize)
    **                          storage-server-8      1         (1 * chunkSize)
    **
    ** The goal is to distribute the chunks around on different storage servers to improve reliability and throughput.
    **   The chunks used to comprise an objectChunkNumber must all be on different Storage Servers to prevent a single
    **   Storage Server failure from taking out multiple copies.
     */
    private final int objectChunkNumber;

    /*
    ** The following is the chunkNumber on the Storage Server
     */
    private int storageServerChunkNumber;

    private int chunkOffset;
    private int chunkLength;

    private int chunkLBA;

    private String md5Digest;

    /*
    ** The following are used to keep track of the status for this chunk operation
     */
    private final AtomicBoolean statusSet;
    private int status;

    /*
    ** The following is set to indicate that the write to the client completed successfully. If there was an error
    **   the RequestContext error will be set
     */
    private final AtomicBoolean clientChunkWriteDone;

    /*
    ** This is used to handle the HTTP Information that is parsed out of the response from the request to this
    **   server.
     */
    private HttpResponseInfo httpInfo;

    /*
    ** The following is the unique ID that can be used to access a chunk's information that is written into the
    **   ObjectStorageDb database.
     */
    private int chunkUniqueId;

    /*
    ** The following is the unique ID for the Storage Server. This comes from the serverIdentifier.serverId in the
    **   StorageServer database.
     */
    private int serverId;

    public ServerIdentifier(final String serverName, final InetAddress ipAddress, final int tcpPort, final int objectChunkNumber) {
        this.serverName = serverName;
        this.serverIpAddress = ipAddress;
        this.serverTcpPort = tcpPort;
        this.objectChunkNumber = objectChunkNumber;

        chunkUniqueId = -1;
        serverId = -1;

        statusSet = new AtomicBoolean(false);
        clientChunkWriteDone = new AtomicBoolean(false);
    }

    public String getServerName() { return serverName; }

    public InetAddress getServerIpAddress() {
        return serverIpAddress;
    }

    public int getServerTcpPort() {
        return serverTcpPort;
    }

    public int getChunkNumber() {
        return objectChunkNumber;
    }

    public void setOffset(final int offset) {
        chunkOffset = offset;
    }

    public int getOffset() { return chunkOffset; }

    public void setLength(final int length) {
        chunkLength = length;
    }

    public int getLength() { return chunkLength; }

    /*
    ** The following are for when the chunk is associated with an object table in the ObjectStorageDb
     */
    public void setChunkId(final int id) {
        chunkUniqueId = id;
    }
    public int getChunkId() { return chunkUniqueId; }

    /*
    **
     */
    public String getChunkLocation() { return "test"; }

    /*
    **
     */
    public void setChunkLBA(final int lba) { chunkLBA = lba; }
    public int getChunkLBA() { return chunkLBA; }

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

    /*
    **
     */
    public void setResponseStatus(final int responseStatus) {
        boolean alreadySet = statusSet.getAndSet(true);

        if (!alreadySet) {
            status = responseStatus;
        } else if (status == HttpStatus.OK_200) {
            /*
            ** If there was already a status update, do not overwrite a previous error
             */
            status = responseStatus;
        }
    }

    public int getResponseStatus() {
        if (statusSet.get()) {
            return status;
        }

        return -1;
    }

    /*
    **
     */
    public void setClientChunkWriteDone() { clientChunkWriteDone.set(true);}
    public boolean getClientChunkWriteDone() { return clientChunkWriteDone.get(); }

    /*
    ** These are used when obtaining a chunk from a Storage Server
     */
    public void setServerId(final int id) { serverId = id; }
    public int getServerId() { return serverId; }

    /*
    **
     */
    public void setStorageServerChunkNumber(final int number) { storageServerChunkNumber = number; }
    public int getStorageServerChunkNumber() { return storageServerChunkNumber; }
}
