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
     **     "chunk-mgr-server"
     **     "storage-server-1"
     **     "storage-server-2"
     **     "storage-server-3"
     */
    public boolean retrieveServers(final String sqlQuery, final List<ServerIdentifier> servers, final int chunkNumber) {
        boolean success = false;

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

                                String serverName = rs.getString(1);
                                int tcpPort = rs.getInt(3);
                                ServerIdentifier server = new ServerIdentifier(serverName, inetAddress, tcpPort, chunkNumber);
                                servers.add(server);

                                LOG.info("StorageServer host: " + serverName + " " + inetAddress.toString() + " port: " +
                                        tcpPort);

                                success = true;
                            } catch (UnknownHostException ex) {
                                LOG.warn("retrieveServers() Unknown host: " + rs.getString(1) + " " + ex.getMessage());
                            }
                        }
                    } catch (SQLException sqlEx) {
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

                /*
                ** There are three different types of Services
                 */
                if ((chunksPerStorageServer == 0) && (serverName.contains("chunk-mgr-service"))) {
                    stmt.setInt(2, CHUNK_MGR_SERVER);
                } else if (chunksPerStorageServer > 0) {
                    stmt.setInt(2, STORAGE_SERVER);
                } else {
                    stmt.setInt(2, OBJECT_SERVER);
                }
                stmt.setString(3, serverIp);
                stmt.setInt(4, port);
                stmt.setInt(5, storageTier.toInt());
                stmt.setInt(6, chunksPerStorageServer);
                stmt.executeUpdate();

                /*
                ** NOTE: The call to getGeneratedKeys() needs to be made to obtain the serverId that was generated
                **   when the ServerIdentifier table entry was made.
                 */
                ResultSet rs = stmt.getGeneratedKeys();
                if (rs.next()) {
                    serverId = rs.getInt(1);

                    /*
                    ** Only "storage-server-" need to have chunks added.
                     */
                    if (chunksPerStorageServer > 0) {
                        ServerChunkMgr chunkMgr = new ServerChunkMgr(flavor);
                        chunkMgr.addServerChunks(serverId, chunksPerStorageServer, RequestContext.getChunkSize());
                    }
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

    public String getServerUID(final int serverId) {
        String getServerUIDStr = GET_SERVER_UID_USING_ID + serverId;

        return getUID(getServerUIDStr);
    }

    public abstract boolean getServer(final String serverName, final List<ServerIdentifier> serverList);
    public abstract boolean getStorageServers(final List<ServerIdentifier> servers, final int chunkNumber);
    public abstract int getOrderedStorageServers(final List<ServerIdentifier> servers, final StorageTierEnum storageTier, final int chunkNumber);


    protected int executeGetOrderedStorageServers(final PreparedStatement stmt, final List<ServerIdentifier> servers, final int chunkNumber) {
        int result = HttpStatus.OK_200;

        ResultSet rs = null;
        try {
            rs = stmt.executeQuery();

            try {
                while (rs.next()) {
                    try {
                        /*
                         ** The rs.getString(3) is the String format of the IP Address.
                         */
                        int serverId = rs.getInt(1);
                        String serverName = rs.getString(2);
                        InetAddress inetAddress = InetAddress.getByName(rs.getString(3));
                        int port = rs.getInt(4);

                        ServerIdentifier server = new ServerIdentifier(serverName, inetAddress, port, chunkNumber);
                        server.setServerId(serverId);
                        servers.add(server);

                        LOG.info("StorageServer host: " + serverName + " " + inetAddress.toString() + " port: " + port);
                    } catch (UnknownHostException ex) {
                        LOG.warn("retrieveServers() Unknown host: " + rs.getString(1) + " " + ex.getMessage());
                        result = HttpStatus.UNPROCESSABLE_ENTITY_422;
                        break;
                    }
                }
            } catch (SQLException ex) {
                System.out.println("retrieveServers() rs.next() SQLException: " + ex.getMessage());
                LOG.warn("retrieveServers() rs.next() SQLException: " + ex.getMessage());
                result = HttpStatus.INTERNAL_SERVER_ERROR_500;
            }
        } catch (SQLException sqlEx) {
            LOG.error("executeGetOrderedStorageServers() SQLException: " + sqlEx.getMessage());
            System.out.println("executeGetOrderedStorageServers() SQLException: " + sqlEx.getMessage());
            result = HttpStatus.INTERNAL_SERVER_ERROR_500;
        }

        finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) {
                    LOG.error("executeGetOrderedStorageServers() rs.close() SQLException: " + sqlEx.getMessage());
                    System.out.println("executeGetOrderedStorageServers() rs.close() SQLException: " + sqlEx.getMessage());
                }
            }
        }

        return result;
    }
}
