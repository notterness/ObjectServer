package com.webutils.webserver.mysql;

import com.google.common.io.BaseEncoding;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.List;
import java.util.StringTokenizer;

public class StorageChunkTableMgr extends ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(StorageChunkTableMgr.class);

    private final static String CREATE_CHUNK_1 = "INSERT INTO storageChunk VALUES ( NULL, ";   // offset
    private final static String CREATE_CHUNK_2 = ", ";                                         // length
    private final static String CREATE_CHUNK_3 = ", ";                                         // chunkIndex
    private final static String CREATE_CHUNK_4 = ", '";                                        // storageServerName
    private final static String CREATE_CHUNK_5 = "', '";                                       // serverIp
    private final static String CREATE_CHUNK_6 = "', ";                                        // serverPort
    private final static String CREATE_CHUNK_7 = ", '";                                        // storageLocation
    private final static String CREATE_CHUCK_8 = "', 0, 0, 0, NULL, ";    // dataWritten, readFailureCount, chunkOffline, chunkMd5, ownerObject
    private final static String CREATE_CHUNK_9 = " )";

    private final static String CHECK_FOR_DUPLICATE_1 = "SELECT chunkId FROM storageChunk WHERE offset = ";
    private final static String CHECK_FOR_DUPLICATE_2 = " AND storageServerName = '";
    private final static String CHECK_FOR_DUPLICATE_3 = "' AND serverIp = '";
    private final static String CHECK_FOR_DUPLICATE_4 = "' AND serverPort = ";

    private final static String GET_CHUNK_ID = " AND ownerObject = ";

    private final static String SET_CHUNK_WRITTEN = "UPDATE storageChunk SET dataWritten = 1, chunkMd5 = ? WHERE chunkId = ?";

    private final static String GET_CHUNK_MD5_DIGEST = "SELECT chunkMd5 FROM storageChunk WHERE chunkId = ";

    private final static String DELETE_CHUNK = "DELETE FROM storageChunk WHERE chunkId = ";

    private final static String GET_CHUNKS_FOR_OBJECT = "SELECT * FROM storageChunk WHERE ownerObject = ";

    private final static String INCREMENT_READ_FAILURES = "UPDATE storageChunk SET readFailureCount = readFailureCount + 1 WHERE chunkId = ";

    private final static String SET_CHUNK_OFFLINE = "UPDATE storageChunk SET readFailureCount = readFailureCount + 1, chunkOffline = 1 WHERE chunkId = ";

    private final HttpRequestInfo objectCreateInfo;

    public StorageChunkTableMgr(final WebServerFlavor flavor, final HttpRequestInfo objectCreateInfo) {
        super(flavor);

        this.objectCreateInfo = objectCreateInfo;
    }

    public int createChunkEntry(final int objectId, final ServerIdentifier server) {
        int status = HttpStatus.OK_200;

        if (checkForDuplicateChunk(server)) {
            /*
            ** Log an error and cause this Object write to fail. In addition, there needs to be some cleanup of
            **   the chunks that are allocated and available.
             */
        }

        String createChunkEntry = CREATE_CHUNK_1 + server.getOffset() + CREATE_CHUNK_2 + server.getLength() +
                CREATE_CHUNK_3 + server.getChunkNumber() + CREATE_CHUNK_4 + server.getServerName() + CREATE_CHUNK_5 +
                server.getServerIpAddress() + CREATE_CHUNK_6 + server.getServerTcpPort() + CREATE_CHUNK_7 + "test" +
                CREATE_CHUCK_8 + objectId + CREATE_CHUNK_9;

        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(createChunkEntry);
            } catch (SQLException sqlEx) {
                LOG.error("createObjectEntry() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + createChunkEntry);
                System.out.println("SQLException: " + sqlEx.getMessage());

                objectCreateInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, "\"SQL Exception\"");
                status = HttpStatus.INTERNAL_SERVER_ERROR_500;
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("createObjectEntry() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeObjectStorageDbConn(conn);
        }

        if (status == HttpStatus.OK_200) {
            int chunkId = getChunkId(objectId, server);
            if (chunkId != -1) {
                server.setChunkId(chunkId);
            } else {
                objectCreateInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, "\"Unable to obtain chunkId\"");
                status = HttpStatus.INTERNAL_SERVER_ERROR_500;
            }
        }

        return status;
    }

    /*
    ** This checks to make sure that there is not already a chunk written with the following fields that match:
    **   offset
    **   storageServerName
    **   serverIp
    **   serverPort
     */
    private boolean checkForDuplicateChunk(final ServerIdentifier server) {

        String duplicateCheck = CHECK_FOR_DUPLICATE_1 + server.getOffset() + CHECK_FOR_DUPLICATE_2 + server.getServerName() +
                CHECK_FOR_DUPLICATE_3 + server.getServerIpAddress() + CHECK_FOR_DUPLICATE_4 + server.getServerTcpPort();

        return (getId(duplicateCheck) == -1);
    }

    /*
    ** Obtain the chunkId. This uses the following fields to obtain a unique chunk
    **
    **   offset
    **   storageServerName
    **   serverIp
    **   serverPort
    **   objectId
     */
    private int getChunkId(final int objectId, final ServerIdentifier server) {
        String getObjectIdStr = CHECK_FOR_DUPLICATE_1 + server.getOffset() + CHECK_FOR_DUPLICATE_2 + server.getServerName() +
                CHECK_FOR_DUPLICATE_3 + server.getServerIpAddress() + CHECK_FOR_DUPLICATE_4 + server.getServerTcpPort() +
                GET_CHUNK_ID + objectId;

        return getId(getObjectIdStr);
    }

    /*
    ** This updates the dataWritten field for the chunk. This is called after all the data for the chunk has been written
    **   by the Storage Server.
     */
    public boolean setChunkWritten(final int chunkId, final String md5Digest) {
        if (md5Digest == null) {
            LOG.warn("setChunkWritten() md5Digest is null");
            return false;
        }

        byte[] md5DigestBytes = BaseEncoding.base64().decode(md5Digest);
        if (md5DigestBytes.length != 16) {
            LOG.warn("The value of the digest '" + md5Digest + "' incorrect length after base-64 decoding");
            return false;
        }

        boolean success = true;

        Connection conn = getObjectStorageDbConn();
        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(SET_CHUNK_WRITTEN);
                stmt.setBytes(1, md5DigestBytes);
                stmt.setInt(2,chunkId);
                stmt.execute();
            } catch (SQLException sqlEx) {
                success = false;
                LOG.error("executeSqlStatement() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + SET_CHUNK_WRITTEN);
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("executeSqlStatement() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeObjectStorageDbConn(conn);
        }

        return success;
    }

    /*
    ** The chunkMd5 is stored in the storageChunk table after the chunk is successfully written to the Storage Server.
    **   The chunkMd5 is then used when reading data back from the Storage Server to insure that it matches what was
    **   sent to the Storage Server earlier.
     */
    public String getChunkMd5Digest(final int chunkId) {
        String chunkMd5Query = GET_CHUNK_MD5_DIGEST + chunkId;
        String md5DigestStr = null;

        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();
                if (stmt.execute(chunkMd5Query)) {
                    rs = stmt.getResultSet();
                }
            } catch (SQLException sqlEx) {
                LOG.error("getSingleStr() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + chunkMd5Query);
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (rs != null) {
                    byte[] md5Bytes;

                    try {
                        int count = 0;
                        while (rs.next()) {
                            /*
                             ** The rs.getBytes(1) is the BINARY() representation of the Md5 Digest.
                             */
                            md5Bytes = rs.getBytes(1);

                            count++;

                            /*
                            ** There should only be one storageChunk table entry based upon the chunkId, but add a
                            **   safety check to insure that is true.
                             */
                            if (count == 1) {
                                md5DigestStr = BaseEncoding.base64().encode(md5Bytes);

                                System.out.println("getChunkMd5Digest() encode: " + md5DigestStr);
                            }
                        }

                        if (count != 1) {
                            LOG.warn("getChunkMd5Digest() invalid response count: " + count);
                            LOG.warn(chunkMd5Query);
                        }
                    } catch (SQLException sqlEx) {
                        System.out.println("getSingleStr() SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("getSingleStr() SQL conn rs.close() SQLException: " + sqlEx.getMessage());
                    }
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("getSingleStr() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeObjectStorageDbConn(conn);
        }

        return md5DigestStr;
    }

    /*
     ** The getChunks() method is used to retrieve all of the chunks associated with an object. This pulls in the
     **   information about that chunk.
     */
    public void getChunks(final int objectId, final List<ServerIdentifier> chunkList) {
        String chunkQuery = GET_CHUNKS_FOR_OBJECT + objectId;
        String md5DigestStr = null;

        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();
                if (stmt.execute(chunkQuery)) {
                    rs = stmt.getResultSet();
                }
            } catch (SQLException sqlEx) {
                LOG.error("getSingleStr() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + chunkQuery);
                System.out.println("SQLException: " + sqlEx.getMessage());
            } finally {
                if (rs != null) {
                    try {
                        while (rs.next()) {
                            int dataWritten = rs.getInt(9);
                            if (dataWritten == 1) {
                                int chunkId = rs.getInt(1);
                                int offset = rs.getInt(2);
                                int length = rs.getInt(3);
                                int chunkIndex = rs.getInt(4);
                                String serverName = rs.getString(5);
                                String ip = rs.getString(6);
                                int port = rs.getInt(7);
                                String location = rs.getString(8);

                                /*
                                ** Pull out the name of the server
                                 */
                                String hostName = null;
                                StringTokenizer stk = new StringTokenizer(ip, " /");
                                if (stk.hasMoreTokens()) {
                                    hostName = stk.nextToken();
                                    LOG.info("hostName: " + hostName + " chunkIndex: " + chunkIndex + " length: " + length);
                                }

                                int readFailureCount = rs.getInt(10);
                                boolean chunkOffline = rs.getBoolean(11);

                                /*
                                 ** The rs.getBytes(10) is the BINARY() representation of the Md5 Digest.
                                 */
                                byte[] md5Bytes = rs.getBytes(12);
                                md5DigestStr = BaseEncoding.base64().encode(md5Bytes);

                                if (hostName != null) {
                                    try {
                                        InetAddress addr = InetAddress.getByName(hostName);


                                        ServerIdentifier server = new ServerIdentifier(serverName, addr, port, chunkIndex);
                                        server.setOffset(offset);
                                        server.setLength(length);
                                        server.setChunkLocation(location);
                                        server.setMd5Digest(md5DigestStr);
                                        server.setChunkId(chunkId);

                                        chunkList.add(server);
                                    } catch (UnknownHostException ex) {
                                        LOG.error("Bad IP: " + ip + " " + ex.getMessage());
                                    }
                                }
                            }
                        }
                    } catch (SQLException sqlEx) {
                        System.out.println("getSingleStr() SQL conn rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("getSingleStr() SQL conn rs.close() SQLException: " + sqlEx.getMessage());
                    }
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("getSingleStr() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to read the database tables.
             */
            closeObjectStorageDbConn(conn);
        }
    }

    public void deleteChunk(final int chunkId) {
        executeSqlStatement(DELETE_CHUNK + chunkId);
    }

    public void incrementChunkReadFailure(final int chunkId) { executeSqlStatement(INCREMENT_READ_FAILURES + chunkId); }

    public void setChunkOffline(final int chunkId) { executeSqlStatement(SET_CHUNK_OFFLINE + chunkId); }
}
