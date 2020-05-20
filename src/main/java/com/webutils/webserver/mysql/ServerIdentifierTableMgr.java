package com.webutils.webserver.mysql;

import com.webutils.webserver.http.StorageTierEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.List;

public abstract class ServerIdentifierTableMgr extends ServersDb {

    private static final Logger LOG = LoggerFactory.getLogger(ServerIdentifierTableMgr.class);

    private final static String GET_SERVER_UID_USING_ID = "SELECT BIN_TO_UUID(serverUID) FROM serverIdentifier WHERE serverId = ";

    private int serverId;

    public ServerIdentifierTableMgr(final WebServerFlavor flavor) {
        super(flavor);

        serverId = -1;
    }

    /*
     ** This can be used to return a List<ServerIdentifiers> of all the server records.
     ** It can also return a specific server (if one exists) based upon the passed in serverName. Currently, the only
     **   server names that are valid are:
     **     "object-server"
     **     "storage-server-1"
     **     "storage-server-2"
     **     "storage-server-3"
     */
    public boolean retrieveServers(final String sqlQuery, final List<ServerIdentifier> servers, final int chunkNumber) {
        boolean success = true;

        LOG.info("ServersDb retrieveStorageServers() dockerImage: " + isDockerImage() + " k8Image: " +
                isKubernetesImage());

        Connection conn = getServersDbConn();

        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = conn.createStatement();

                if (stmt.execute(sqlQuery)) {
                    rs = stmt.getResultSet();
                }
            } catch (SQLException sqlEx) {
                // handle any errors
                LOG.error("retrieveServers() SQLException: " + sqlEx.getMessage());
                System.out.println("retrieveServers() SQLException: " + sqlEx.getMessage());
            }

            finally {
                if (rs != null) {

                    try {
                        while (rs.next()) {
                            try {
                                /*
                                 ** The rs.getString(3) is the String format of the IP Address.
                                 */
                                InetAddress inetAddress = InetAddress.getByName(rs.getString(2));

                                ServerIdentifier server = new ServerIdentifier(rs.getString(1), inetAddress, rs.getInt(3), chunkNumber);
                                servers.add(server);

                                LOG.info("StorageServer host: " + rs.getString(1) + " " + inetAddress.toString() + " port: " +
                                        rs.getInt(3));
                            } catch (UnknownHostException ex) {
                                success = false;
                                LOG.warn("retrieveServers() Unknown host: " + rs.getString(1) + " " + ex.getMessage());
                            }
                        }
                    } catch (SQLException sqlEx) {
                        success = false;
                        System.out.println("retrieveServers() rs.next() SQLException: " + sqlEx.getMessage());
                        LOG.warn("retrieveServers() rs.next() SQLException: " + sqlEx.getMessage());
                    }

                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("retrieveServers()  rs close() SQLException: " + sqlEx.getMessage());
                        LOG.warn("retrieveServers() rs close() SQLException: " + sqlEx.getMessage());
                    }
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        System.out.println("retrieveServers()  close() SQLException: " + sqlEx.getMessage());
                        LOG.warn("retrieveServers() close() SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            closeStorageServerDbConn(conn);
        }

        if (!success) {
            servers.clear();
        }

        return success;
    }

    public int createServer(final String serverName, final String serverIp, final int port, final int chunksPerStorageServer,
                            final StorageTierEnum storageTier) {
        int result = HttpStatus.OK_200;
        Connection conn = getServersDbConn();

        if (conn != null) {
            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(ADD_STORAGE_SERVER, Statement.RETURN_GENERATED_KEYS);
                stmt.setString(1, serverName);
                stmt.setString(2, serverIp);
                stmt.setInt(3, port);
                stmt.setInt(4, chunksPerStorageServer);
                stmt.setInt(5, storageTier.toInt());
                stmt.executeUpdate();

                ResultSet rs = stmt.getGeneratedKeys();
                if (rs.next()) {
                    serverId = rs.getInt(1);

                    ServerChunkMgr chunkMgr = new ServerChunkMgr(flavor);
                    chunkMgr.addServerChunks(serverId, CHUNKS_TO_ALLOCATE, RequestContext.getChunkSize());
                }
                rs.close();
            } catch (SQLException sqlEx) {
                LOG.error("createServer() SQLException: " + sqlEx.getMessage());
                System.out.println("createServer() SQLException: " + sqlEx.getMessage());

                result = HttpStatus.INTERNAL_SERVER_ERROR_500;
            }
            finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("createServer() stmt.close() SQLException: " + sqlEx.getMessage());
                        System.out.println("createServer() stmt.close()  SQLException: " + sqlEx.getMessage());
                    }
                }
            }

            /*
             ** Close out this connection as it was only used to create the database tables.
             */
            closeStorageServerDbConn(conn);
        } else {
            LOG.error("createServer() unable to obtain conn");
            result = HttpStatus.BAD_GATEWAY_502;
        }

        return result;
    }

    public int getLastInsertId() { return serverId; }

    public abstract boolean getServer(final String serverName, final List<ServerIdentifier> serverList);
    public abstract boolean getStorageServers(final List<ServerIdentifier> servers, final int chunkNumber);

    public String getServerUID(final int serverId) {
        String getServerUIDStr = GET_SERVER_UID_USING_ID + serverId;

        return getUID(getServerUIDStr);
    }

}