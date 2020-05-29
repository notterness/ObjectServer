package com.webutils.webserver.mysql;

import com.webutils.webserver.http.ChunkStatusEnum;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.ListChunkInfo;
import com.webutils.webserver.http.StorageTierEnum;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/*
** This is used to manage the storageServerChunk table in the ServiceServersDb database.
** This is managed by the ChunkMgr service.
 */
public class ServerChunkMgr extends ServersDb {

    private static final Logger LOG = LoggerFactory.getLogger(ServerChunkMgr.class);

    private static final String CREATE_CHUNK = "INSERT INTO storageServerChunk VALUES(NULL, ?, ?, ?, ?, NULL, NULL, UUID_TO_BIN(UUID()), ?)";

    private static final String FIND_CHUNK_PROCEDURE = "CREATE PROCEDURE allocate_chunk( IN server_id INT)" +
            " BEGIN " +
            "   DECLARE chunk_id_tmp INT;" +
            "   SELECT chunkId INTO chunk_id_tmp FROM storageServerChunk WHERE serverId = server_id AND state = 3 LIMIT 1; " +
            "   IF (chunk_id_tmp IS NOT NULL) THEN" +
            "     SELECT chunkId, chunkNumber, startLba, size, BIN_TO_UUID(chunkUID) AS chunkUID FROM storageServerChunk " +
            "           WHERE chunkId = chunk_id_tmp; " +
            "     UPDATE storageServerChunk SET state = 1 WHERE chunkId = chunk_id_tmp;" +
            "     UPDATE ServerIdentifier SET usedChunks = usedChunks + 1, lastAllocationTime = CURRENT_TIME() WHERE serverId = server_id;" +
            "   END IF;" +
            " END";

    private static final String EXECUTE_ALLOCATE_CHUNK = "{CALL allocate_chunk(?)}";

    private static final String LIST_CHUNK_PROCEDURE = "CREATE PROCEDURE list_chunks(IN _serverName VARCHAR(128), IN _tier INT, IN _state INT)\n" +
            "    BEGIN" +
            "        IF ((_serverName IS NULL) AND (_tier = 0) AND (_state = 0)) THEN" +
            "             SELECT chunkId, chunkNumber, startLba, size, state, lastAllocateTime, lastDeleteTime, serverId, BIN_TO_UUID(chunkUID) AS chunkUID FROM storageServerChunk;" +
            "        ELSEIF (_serverName IS NULL) THEN" +
            "           IF ((_tier != 0) AND (_state = 0)) THEN" +
            "             SELECT chunkId, chunkNumber, startLba, size, state, lastAllocateTime, lastDeleteTime, serverId, BIN_TO_UUID(chunkUID) AS chunkUID FROM storageServerChunk chunk" +
            "                 INNER JOIN ServerIdentifier server USING (serverId) WHERE server.storageTier = _tier;" +
            "          ELSEIF ((_tier = 0) AND (_state != 0)) THEN" +
            "             SELECT chunkId, chunkNumber, startLba, size, state, lastAllocateTime, lastDeleteTime, serverId, BIN_TO_UUID(chunkUID) AS chunkUID FROM storageServerChunk chunk" +
            "                 INNER JOIN ServerIdentifier server USING (serverId) WHERE chunk.state = _state;" +
            "          ELSE" +
            "             SELECT chunkId, chunkNumber, startLba, size, state, lastAllocateTime, lastDeleteTime, serverId, BIN_TO_UUID(chunkUID) AS chunkUID FROM storageServerChunk chunk" +
            "                 INNER JOIN ServerIdentifier server USING (serverId) WHERE chunk.state = _state AND server.storageTier = _tier;" +
            "          END IF;" +
            "        ELSE" +
            "           IF ((_tier != 0) AND (_state = 0)) THEN" +
            "              SELECT chunkId, chunkNumber, startLba, size, state, lastAllocateTime, lastDeleteTime, serverId, BIN_TO_UUID(chunkUID) AS chunkUID FROM storageServerChunk chunk" +
            "                 INNER JOIN ServerIdentifier server USING (serverId) WHERE server.storageTier = _tier AND server.serverName LIKE _serverName;" +
            "            ELSEIF ((_tier = 0) AND (_state != 0)) THEN" +
            "             SELECT chunkId, chunkNumber, startLba, size, state, lastAllocateTime, lastDeleteTime, serverId, BIN_TO_UUID(chunkUID) AS chunkUID FROM storageServerChunk chunk" +
            "                 INNER JOIN ServerIdentifier server USING (serverId) WHERE chunk.state = _state AND server.serverName LIKE _serverName;" +
            "            ELSE" +
            "             SELECT chunkId, chunkNumber, startLba, size, state, lastAllocateTime, lastDeleteTime, serverId, BIN_TO_UUID(chunkUID) AS chunkUID FROM storageServerChunk chunk" +
            "                 INNER JOIN ServerIdentifier server USING (serverId) WHERE chunk.state = _state AND server.storageTier = _tier AND server.serverName LIKE _serverName;" +
            "            END IF;" +
            "         END IF;" +
            "     END";

    private static final String EXECUTE_LIST_CHUNKS = "{CALL list_chunks(?, ?, ?)}";

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
                    stmt.setInt(4, ChunkStatusEnum.CHUNK_AVAILABLE.toInt());
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

    public int allocateStorageChunk(final ServerIdentifier serverIdentifier) {
        int result = HttpStatus.OK_200;

        Connection conn = getServersDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.prepareStatement(EXECUTE_ALLOCATE_CHUNK);
                stmt.setInt(1, serverIdentifier.getServerId());
                stmt.executeQuery();

                rs = stmt.getResultSet();
                if (rs != null) {
                    if (rs.next()) {
                        int chunkId = rs.getInt(1);
                        int chunkNumber = rs.getInt(2);
                        int chunkLba = rs.getInt(3);
                        int chunkSize = rs.getInt(4);
                        String chunkUID = rs.getString(5);

                        serverIdentifier.setChunkId(chunkId);
                        serverIdentifier.setStorageServerChunkNumber(chunkNumber);
                        serverIdentifier.setChunkLBA(chunkLba);
                        serverIdentifier.setLength(chunkSize);
                        serverIdentifier.setChunkUID(chunkUID);

                        LOG.info("allocateChunk() id: " + chunkId + " " + chunkNumber + " " + chunkLba + " " + chunkSize +
                                " " + chunkUID);
                    }
                } else {
                    LOG.warn("No chunks available on server");
                    result = HttpStatus.NO_CONTENT_204;
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
    public void addStoredProcedures() {
        Connection conn = getServersDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(FIND_CHUNK_PROCEDURE);
                stmt.execute(LIST_CHUNK_PROCEDURE);
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

    /*
    ** The three potential search modifications are (they are all optional and may not be present):
    **   storageTier - List the chunks for a particular storage tier.
    **   storage-server-name - This may be a string to match against. For example "server-xyx-*". This will return all
    **     the chunks associated with the matching storage server(s).
    **   chunk-status - This can be AVAILABLE, ALLOCATED or DELETED.
    **
    ** Then there is a limit on the number of records to return that can be specified. If the "limit" is not
    **   specified, then a maximum of 50 records will be returned.
     */
    public int listChunks(final HttpRequestInfo requestInfo, final List<ListChunkInfo> chunks) {
        int result = HttpStatus.OK_200;

        StorageTierEnum tier = requestInfo.getStorageTier();
        String serverName = requestInfo.getServerName();
        ChunkStatusEnum chunkStatus = requestInfo.getChunkStatus();

        Connection conn = getServersDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.prepareStatement(EXECUTE_LIST_CHUNKS);
                if (serverName != null) {
                    stmt.setString(1, serverName);
                } else {
                    stmt.setNull(1, Types.VARCHAR);
                }
                stmt.setInt(2, tier.toInt());
                stmt.setInt(3, chunkStatus.toInt());
                stmt.executeQuery();

                rs = stmt.getResultSet();
                if (rs != null) {
                    while (rs.next()) {
                        int id = rs.getInt(1);
                        int index = rs.getInt(2);
                        int lba = rs.getInt(3);
                        int size = rs.getInt(4);
                        ChunkStatusEnum state = ChunkStatusEnum.fromInt(rs.getInt(5));
                        String lastAllocate = rs.getString(6);
                        String lastDelete = rs.getString(7);
                        int serverId = rs.getInt(8);
                        String chunkUID = rs.getString(9);

                        ListChunkInfo info = new ListChunkInfo(id, index, chunkUID, lba, size, state, lastAllocate, lastDelete, serverId);
                        chunks.add(info);

                        LOG.info("listChunks() id: " + id + " " + index + " " + lba + " " + size +
                                " " + chunkUID);
                    }
                }
            } catch (SQLException sqlEx) {
                LOG.error("listChunks() SQLException: " + sqlEx.getMessage());
                System.out.println("SQLException: " + sqlEx.getMessage());
                result = HttpStatus.INTERNAL_SERVER_ERROR_500;
            }
            finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException ex) {
                        LOG.error("listChunks() rs.close() SQLException: " + ex.getMessage());
                        System.out.println("listChunks() rs.close() SQLException: " + ex.getMessage());
                    }
                }
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("listChunks() stmt.close() SQLException: " + sqlEx.getMessage());
                        System.out.println("listChunks() stmt.close() SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
        } else {
            LOG.error("listChunks() unable to obtain conn");
            result = HttpStatus.BAD_GATEWAY_502;
        }

        return result;
    }
}
