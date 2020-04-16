package com.webutils.webserver.mysql;

import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class NamespaceTableMgr extends ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(NamespaceTableMgr.class);

    private final static String CREATE_NAMESPACE_1 = "INSERT INTO customerNamespace VALUES ( NULL, '";
    private final static String CREATE_NAMESPACE_2 = "', '";
    private final static String CREATE_NAMESPACE_3 = "', UUID_TO_BIN(UUID()), (SELECT tenancyId FROM customerTenancy WHERE tenancyUID = UUID_TO_BIN('";
    private final static String CREATE_NAMESPACE_4 = "') ) )";

    private final static String GET_NAMESPACE_UID_1 = "SELECT BIN_TO_UUID(namespaceUID) namespaceUID FROM customerNamespace WHERE name = '";
    private final static String GET_NAMESPACE_UID_2 = "' AND tenancyId = ( SELECT tenancyId FROM customerTenancy WHERE tenancyUID = UUID_TO_BIN('";
    private final static String GET_NAMESPACE_UID_3 = "') )";

    public NamespaceTableMgr(final WebServerFlavor flavor) {
        super(flavor);
    }

    /*
     ** NOTE: A namespace is unique to a Tenancy. A customer can have multiple Tenancies and each of those Tenancies
     **   can use the same namespace name even though the namespaces are unique. For example:
     **
     **        (Tenancy_1, customer A) -> name(Namespace_A)
     **        (Tenancy_2, customer A) -> name(Namespace_A)
     **
     **   In the above example, both of the namespaces are unique and can have different characteristics. For this reason,
     **   the Tenancy UID is used to insure uniqueness.
     **
     **   Another feature of a Namespace is that it is used only by Object Storage and it serves as a container for all
     **   Buckets created within a region. There is one namespace per Tenancy and the namespace spans across all of the
     **   Compartments within a region. Just as an FYI, the Compartments are used to provide access controls to
     **   resources.
     */
    public boolean createNamespaceEntry(final String namespace, final String tenancyUID, final String region) {
        boolean success = true;

        String createNamespaceStr = CREATE_NAMESPACE_1 + namespace + CREATE_NAMESPACE_2 + region + CREATE_NAMESPACE_3 +
                tenancyUID + CREATE_NAMESPACE_4;

        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(createNamespaceStr);
            } catch (SQLException sqlEx) {
                LOG.error("createNamespaceEntry() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + createNamespaceStr);
                System.out.println("SQLException: " + sqlEx.getMessage());

                success = false;
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("createNamespaceEntry() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
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
    ** This obtains the Namespace UID. It will return NULL if the namespace does not exist.
     */
    public String getNamespaceUID(final String namespace, final String tenancyUID) {
        String getNamespaceUIDStr = GET_NAMESPACE_UID_1 + namespace + GET_NAMESPACE_UID_2 + tenancyUID + GET_NAMESPACE_UID_3;

        return getUID(getNamespaceUIDStr);
    }
}
