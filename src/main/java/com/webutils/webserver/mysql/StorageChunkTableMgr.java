package com.webutils.webserver.mysql;

import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class StorageChunkTableMgr extends ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(StorageChunkTableMgr.class);

    private final static String CREATE_CHUNK_1 = "INSERT INTO storageChunk VALUES ( NULL, ";   // offset
    private final static String CREATE_CHUNK_2 = ", ";                                         // length
    private final static String CREATE_CHUNK_3 = ", ";                                         // chunkIndex
    private final static String CREATE_CHUNK_4 = ", '";                                        // storageServerName
    private final static String CREATE_CHUNK_5 = "', '";                                       // serverIp
    private final static String CREATE_CHUNK_6 = "', ";                                        // serverPort
    private final static String CREATE_CHUNK_7 = ", '";                                        // storageLocation
    private final static String CREATE_CHUCK_8 = "', 0, ";                                     // dataWritten, ownerObject
    private final static String CREATE_CHUNK_9 = " )";

    private final static String CHECK_FOR_DUPLICATE_1 = "SELECT chunkId FROM storageChunk WHERE offset = ";
    private final static String CHECK_FOR_DUPLICATE_2 = " AND storageServerName = '";
    private final static String CHECK_FOR_DUPLICATE_3 = "' AND serverIp = '";
    private final static String CHECK_FOR_DUPLICATE_4 = "' AND serverPort = ";

    private final static String GET_CHUNK_ID = " AND ownerObject = ";

    private final static String SET_CHUNK_WRITTEN = "UPDATE storageChunk SET dataWritten = 1 WHERE chunkId = ";

    private final static String DELETE_CHUNK = "DELETE FROM storageChunk WHERE chunkId = ";


    private final HttpRequestInfo objectCreateInfo;

    public StorageChunkTableMgr(final WebServerFlavor flavor, final HttpRequestInfo objectCreateInfo) {
        super(flavor);

        //LOG.info("ObjectTableMgr() flavor: " + flavor);

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
    public boolean setChunkWritten(final int chunkId) {
        return executeSqlStatement(SET_CHUNK_WRITTEN + chunkId);
    }

    public void deleteChunk(final int chunkId) {
        executeSqlStatement(DELETE_CHUNK + chunkId);
    }
}
