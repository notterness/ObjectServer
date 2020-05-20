package com.webutils.webserver.mysql;

import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/*
** This is used to manage the storageServerChunk table in the StorageServers database.
 */
public class ServerChunkMgr extends ServersDb {

    private static final Logger LOG = LoggerFactory.getLogger(ServerChunkMgr.class);

    private static final String CREATE_CHUNK = "INSERT INTO storageServerChunk VALUES(NULL, ?, ?, ?, ?, NULL, NULL, ?)";

    private static final String FIND_CHUNK_PROCEDURE = "CREATE PROCEDURE allocate_chunk( IN server_id INT)" +
            " BEGIN " +
            "   DECLARE chunk_id_tmp INT;" +
            "   SELECT chunkId INTO chunk_id_tmp FROM storageServerChunk WHERE serverId = server_id AND state = 3 LIMIT 1; " +
            "   IF (chunk_id_tmp IS NOT NULL) THEN" +
            "     SELECT chunkId, chunkNumber, startLba, size AS chunk_info FROM storageServerChunk " +
            "           WHERE chunkId = chunk_id_tmp; " +
            "     UPDATE storageServerChunk SET state = 1 WHERE chunkId = chunk_id_tmp;" +
            "   END IF;" +
            " END";

    private static final String EXECUTE_ALLOCATE_CHUNK = "{CALL allocate_chunk(?)}";

    private static final int CHUNK_ALLOCATED = 1;
    private static final int CHUNK_DELETED = 2;
    private static final int CHUNK_AVAILABLE = 3;

    public ServerChunkMgr(final WebServerFlavor flavor) {
        super(flavor);

        LOG.info("ServerChunkMgr() WebServerFlavor: " + flavor.toString());
    }

    /*
    ** This adds the allotment of chunks to a Storage Server. It is only done once when the Storage Server is added.
     */
    public void addServerChunks(final int serverId, final int chunksToAllocate, final int chunkSize) {
        Connection conn = getServersDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;
            int lba = 0;

            try {
                stmt = conn.prepareStatement(CREATE_CHUNK);
                for (int i = 0; i < chunksToAllocate; i++) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, lba);
                    stmt.setInt(3, chunkSize);
                    stmt.setInt(4, CHUNK_AVAILABLE);
                    stmt.setInt(5, serverId);
                    stmt.executeUpdate();

                    lba+= chunkSize;
                }
            } catch (SQLException sqlEx) {
                LOG.error("allocateServerChunks() SQLException: " + sqlEx.getMessage());
                System.out.println("SQLException: " + sqlEx.getMessage());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("allocateServerChunks() close() SQLException: " + sqlEx.getMessage());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
        }
    }

    public int allocateChunk(final ServerIdentifier serverIdentifier) {
        int result = HttpStatus.OK_200;

        Connection conn = getServersDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            int lba = 0;

            try {
                stmt = conn.prepareStatement(EXECUTE_ALLOCATE_CHUNK);
                stmt.setInt(1, serverIdentifier.getServerId());
                stmt.executeQuery();

                rs = stmt.getResultSet();
                if (rs.next()) {
                    int chunkId = rs.getInt(1);
                    int chunkNumber = rs.getInt(2);
                    int chunkLba = rs.getInt(3);
                    int chunkSize = rs.getInt(4);

                    serverIdentifier.setChunkId(chunkId);
                    serverIdentifier.setStorageServerChunkNumber(chunkNumber);
                    serverIdentifier.setChunkLBA(chunkLba);
                    serverIdentifier.setLength(chunkSize);

                    LOG.info("allocateChunk() id: " + chunkId + " " + chunkNumber + " " + chunkLba + " " + chunkSize);
                }
            } catch (SQLException sqlEx) {
                LOG.error("allocateServerChunks() SQLException: " + sqlEx.getMessage());
                System.out.println("SQLException: " + sqlEx.getMessage());
                result = HttpStatus.INTERNAL_SERVER_ERROR_500;
            }
            finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException ex) {
                        LOG.error("allocateServerChunks() rs.close() SQLException: " + ex.getMessage());
                        System.out.println("allocateServerChunks() rs.close() SQLException: " + ex.getMessage());
                    }
                }
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("allocateServerChunks() stmt.close() SQLException: " + sqlEx.getMessage());
                        System.out.println("allocateServerChunks() stmt.close() SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
        } else {
            LOG.error("allocateChunk() unable to obtain conn");
            result = HttpStatus.BAD_GATEWAY_502;
        }

        return result;
    }

    /*
     ** This adds the allotment of chunks to a Storage Server. It is only done once when the Storage Server is added.
     */
    public void addStoredProcedure() {
        Connection conn = getServersDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(FIND_CHUNK_PROCEDURE);
            } catch (SQLException sqlEx) {
                LOG.error("addStoredProcedure() SQLException: " + sqlEx.getMessage());
                System.out.println("SQLException: " + sqlEx.getMessage());
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("addStoredProcedure() close() SQLException: " + sqlEx.getMessage());
                        System.out.println("SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
        }
    }

}
