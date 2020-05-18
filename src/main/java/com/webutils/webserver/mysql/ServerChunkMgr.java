package com.webutils.webserver.mysql;

import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/*
** This is used to manage the storageServerChunk table in the StorageServers database.
 */
public class ServerChunkMgr extends ServersDb {

    private static final Logger LOG = LoggerFactory.getLogger(ServerChunkMgr.class);

    private static final String CREATE_CHUNK = "INSERT INTO storageServerChunk VALUES(NULL, ?, ?, ?, ?, ?)";

    private static final int CHUNK_ALLOCATED = 1;
    private static final int CHUNK_DELETED = 2;
    private static final int CHUNK_AVAILABLE = 3;

    public ServerChunkMgr(final WebServerFlavor flavor) {
        super(flavor);

        LOG.info("ServerChunkMgr() WebServerFlavor: " + flavor.toString());
    }

    public void allocateServerChunks(final int serverId, final int chunksToAllocate, final int chunkSize) {
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
                return;
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
}
