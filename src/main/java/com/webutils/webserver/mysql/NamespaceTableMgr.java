package com.webutils.webserver.mysql;

import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class NamespaceTableMgr extends ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(NamespaceTableMgr.class);

    private final static String CREATE_NAMESPACE_1 = "INSERT INTO customerNamespace VALUES ( NULL, '";
    private final static String CREATE_NAMESPACE_2 = "', '";
    private final static String CREATE_NAMESPACE_3 = "', UUID_TO_BIN(UUID()), ";
    private final static String CREATE_NAMESPACE_4 = " )";

    private final static String GET_NAMESPACE_UID_1 = "SELECT BIN_TO_UUID(namespaceUID) namespaceUID FROM customerNamespace WHERE name = '";
    private final static String GET_NAMESPACE_UID_2 = "' AND tenancyId = ";

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
    public boolean createNamespaceEntry(final String namespace, final int tenancyId, final String region) {
        boolean success = true;

        /*
        ** First check if there is already a namespace entry. If it exists, simply return true indicating it is present.
         */
        String namespaceUID = getNamespaceUID(namespace, tenancyId);
        if (namespaceUID != null) {
            LOG.warn("This namespace already exists. namespace: " + namespace + " tenancyId: " + tenancyId);
            return true;
        }

        String createNamespaceStr = CREATE_NAMESPACE_1 + namespace + CREATE_NAMESPACE_2 + region + CREATE_NAMESPACE_3 +
                tenancyId + CREATE_NAMESPACE_4;

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
    ** This obtains the Namespace UID. It will return NULL if the namespace does not exist. A namespace is unique to a
    **   Tenancy and to a region.
    **
    ** NOTE: This check is not using the region table field at this point. For completeness, the search for the
    **   namespace should use the following:
    **     tenancyId - This is how resources are owned by a customer and provides the top level of organization.
    **     region - The namespace is per region within a tenancy.
    **     namespaceName - The name of the namespace the Object Storage items are contained within.
     */
    public String getNamespaceUID(final String namespaceName, final int tenancyId) {
        String getNamespaceUIDStr = GET_NAMESPACE_UID_1 + namespaceName + GET_NAMESPACE_UID_2 + tenancyId;

        return getUID(getNamespaceUIDStr);
    }
}
