package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.mds.object.ObjectConstants;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Random;

/**
 * Recycle Bin feature is to provide some sort of undo/un-delete functionality for Oracle Data Cloud(ODC) as they
 * migrate from AWS S3 to OCI Object Storage. Full undo functionality (e.g. recovering objects that were overwritten)
 * will require versioning and it requires more time to complete. In the interest of time, it is decided to provide
 * recycle bin feature as an interim approach for ODC.  This feature is not intended to be publicly available. The
 * recycle bin would be a Limited Availability hack that we would only commit to supporting until versioning is
 * implemented.
 *
 * High level details of the feature:
 * - Recycle bin feature can be enabled by setting "RecycleBin" option at the bucket level
 * - Whenever an object is deleted, the object will be renamed to .trash/objectname.deletiontime.randomnumber
 * - Customer deletion of objects starting with ".trash/" will not be allowed. Only OLM will be allowed to delete
 *   objects from the recycle bin.
 * - Creation of objects, rename of objects, multipart upload and metadata updation of objects starting with ".trash/"
 *   are not allowed.
 *
 * <p>
 *     * Recycle Bin helper class
 * <p>
 */
@SuppressFBWarnings(value = "PREDICTABLE_RANDOM")
public final class RecycleBinHelper {
    // Prefix to be used to distinguish recycle bin objects
    private static final String PREFIX = ".trash/";
    private static final String OPTION_NAME = "recycleBin";
    private static final String DATE_PATTERN = "yyyyMMdd'T'HHmmss";

    // Separator to be used while constructing object names in the recycle bin
    // Eg: objectname.deletiontime.sequence
    private static final String SEPARATOR = ".";
    private static final boolean ENABLED = true;
    private static final int MAX_RANDOM = 999;

    // In order to handle maximum length object names (1024), it is decided to truncate the object name to add
    // '.trash/' and the new suffix.
    // Calculate the length of "./trash" + "." + ""yyyyMMddTHHmmss"" + "." + "size of random integer"
    // Subtract 2 (-2) to remove quotes length for 'T' from DATE_PATTERN
    private static final int ADDITIONAL_LEN = PREFIX.length() + 2 * SEPARATOR.length() + DATE_PATTERN.length() - 2  +
            String.valueOf(MAX_RANDOM).length();

    // Allowed original object name length in the recycle bin object name
    // {@link ObjectConstants#OBJECT_NAME_SIZE_LIMIT} contains the maximum length limit for object names
    private static final int ALLOWED_OBJECT_LEN = ObjectConstants.OBJECT_NAME_SIZE_LIMIT - ADDITIONAL_LEN;

    private RecycleBinHelper() {
    }

    /**
     * Get the option name to be used to enable the recycle bin feature for a bucket
     *
     * @return "recycleBin"
     */
    public static String getOptionName() {

        return OPTION_NAME;
    }

    /**
     * Get the prefix to be used to distinguish recycle bin objects
     *
     * @return ".trash/"
     */
    public static String getPrefix() {

        return PREFIX;
    }

    /**
     * Construct suffix to be used while moving the objects to recycle bin
     *
     * @return "currenttime.randomnumber"
     */
    private static String constructSuffix() {
        DateTimeFormatter pattern = DateTimeFormatter.ofPattern(DATE_PATTERN);
        String dateString = pattern.format(ZonedDateTime.now());
        Random random = new Random();
        int randomNumber = random.nextInt(MAX_RANDOM);
        return dateString + SEPARATOR + randomNumber;
    }

    /**
     * Construct name to be created in the recycle bin for a given object name.
     *
     * @param objectName name of the object
     * @return name to be used for the given object while moving to the recycle bin
     */
    public static String constructName(String objectName) {
        String suffix = constructSuffix();
        String truncObjectName = objectName;
        // Truncate object name as there is a size limit for object names in MDS
        if (objectName.length() > ALLOWED_OBJECT_LEN) {
            truncObjectName = objectName.substring(0, ALLOWED_OBJECT_LEN);
        }
        return PREFIX + truncObjectName + SEPARATOR + suffix;
    }

    /**
     * Determines if the RecycleBin option is enabled for the bucket.
     *
     * @param {@link WSTenantBucketInfo} of bucket
     * @return true if the RecycleBin option is enabled for the bucket else false
     */
    public static boolean isEnabled(WSTenantBucketInfo bucket) {
        Map<String, Object> options = bucket.getOptions();
        if (options != null && options.containsKey(OPTION_NAME) && options.get(OPTION_NAME).equals(ENABLED)) {
            return true;
        }
        return false;
    }

    /**
     * Checks if the object is in the recycle bin.
     *
     * @param {@link WSTenantBucketInfo} of bucket where the object resides
     * @param objectName name of the object
     * @return true if the object name starts with the recycle bin prefix else false
     */
    public static boolean isBinObject(WSTenantBucketInfo bucket, String objectName) {
        if (RecycleBinHelper.isEnabled(bucket)) {
            return objectName.startsWith(PREFIX);
        }
        return false;
    }
}
