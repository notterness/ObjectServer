package com.webutils.webserver.mysql;

import com.webutils.webserver.http.CreateBucketPostContent;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class BucketTagTableMgr  extends ObjectStorageDb {

    private static final Logger LOG = LoggerFactory.getLogger(BucketTagTableMgr.class);

    private final static String CREATE_TAG_1 = "INSERT INTO BucketTags VALUES ( '";
    private final static String CREATE_TAG_2 = "', '";
    private final static String CREATE_TAG_2_1 = "', NULL, '";
    private final static String CREATE_TAG_3 = "', '";
    private final static String CREATE_TAG_4 = "', '";
    private final static String CREATE_TAG_5 = "', ";
    private final static String CREATE_TAG_6 = " )";

    private final static String DELETE_TAGS = "DELETE FROM BucketTags WHERE bucketId = ";

    public BucketTagTableMgr(final WebServerFlavor flavor) {
        super(flavor);
    }

    public boolean createBucketTags(final CreateBucketPostContent createBucketPostContent, final int bucketId) {
        boolean success = true;

        /*
        ** First walk through all of the Free Form Tags and add those
         */
        Set<Map.Entry<String, String>> freeFormTags = createBucketPostContent.getFreeFormTags();

        Iterator<Map.Entry<String, String>> iter = freeFormTags.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();

            success = createBucketTagEntry("freeFormTags", null, entry.getKey(), entry.getValue(), bucketId);
            if (!success) {
                LOG.error("Unable to create tag entry");
                break;
            }
        }

        /*
        ** See if there are any Defined Tags and if so, add them
         */
        if (success) {
            Set<String> definedTagsSubTags = createBucketPostContent.getDefinedTagsSubTagKeys();
            Iterator<String> subTagIter = definedTagsSubTags.iterator();
            while (subTagIter.hasNext()) {
                String subTag = subTagIter.next();

                Set<Map.Entry<String, String>> subTagPairs = createBucketPostContent.getDefinedTags(subTag);

                Iterator<Map.Entry<String, String>> keyValuePairIter = subTagPairs.iterator();
                while (keyValuePairIter.hasNext()) {
                    Map.Entry<String, String> entry = keyValuePairIter.next();

                    success = createBucketTagEntry("definedTags", subTag, entry.getKey(), entry.getValue(), bucketId);
                    if (!success) {
                        LOG.error("Unable to create tag entry");
                        break;
                    }
                }
            }
        }

        return success;
    }

    private boolean createBucketTagEntry(final String tagName, final String subTagName, final String key, final String value, final int bucketId) {
        boolean success = true;

        String createTagStr;
        if (subTagName != null) {
            createTagStr = CREATE_TAG_1 + tagName + CREATE_TAG_2 + subTagName + CREATE_TAG_3 + key + CREATE_TAG_4 +
                    value + CREATE_TAG_5 + bucketId + CREATE_TAG_6;
        } else {
            createTagStr = CREATE_TAG_1 + tagName + CREATE_TAG_2_1 + key + CREATE_TAG_4 +
                    value + CREATE_TAG_5 + bucketId + CREATE_TAG_6;
        }

        Connection conn = getObjectStorageDbConn();

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.execute(createTagStr);
            } catch (SQLException sqlEx) {
                LOG.error("createBucketTagEntry() SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
                LOG.error("Bad SQL command: " + createTagStr);
                System.out.println("SQLException: " + sqlEx.getMessage());

                success = false;
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                        LOG.error("createBucketTagEntry() close SQLException: " + sqlEx.getMessage() + " SQLState: " + sqlEx.getSQLState());
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
    ** This is used to delete all the tags that are associated with a Bucket
     */
    public void deleteBucketTags(final int bucketId) {
        String deleteTagsStr = DELETE_TAGS + bucketId;

        executeSqlStatement(deleteTagsStr);
    }
}
