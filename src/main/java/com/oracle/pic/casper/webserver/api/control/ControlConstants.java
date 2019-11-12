package com.oracle.pic.casper.webserver.api.control;

/**
 * A collection of constants used by the web server control plane.
 */
public interface ControlConstants {
    String HEALTHCHECK_ROUTE = "/healthcheck";
    String ROOT_PREFIX = "/webserver";
    String REFRESH_VOLUME_METADATA = ROOT_PREFIX + "/refreshVolumeMetadataCache";
    String GET_VMD_CHANGE_SEQ = ROOT_PREFIX + "/vmdChangeSequence";
    String REQUEST_SHUTDOWN = ROOT_PREFIX + "/shutdown";
    String EMBARGO_URI = ROOT_PREFIX + "/embargo";
    String JMX_URI = "/jmx";
    String CHECK_OBJECT_URI = "/check/n/([^/]+)/b/([^/]+)/o/(.+)";

    /**
     * The following constants are the URI for the unrestricted list APIs for the V2 objects
     */
    String UNRESTRICTED_NAMESPACE_COLLECTION_URI = "/walk/n/?";
    String UNRESTRICTED_BUCKET_COLLECTION_URI = "/walk/n/([^/]+)/b/?";
    String UNRESTRICTED_OBJECT_COLLECTION_URI = "/walk/n/([^/]+)/b/([^/]+)/o/?";

    String UNRESTRICTED_ARCHIVE_OBJECT_URI = "/archiveObject/n/([^/]+)/b/([^/]+)/o/(.+)";

    String CHECK_OBJECT_ROUTE = "/checkObject/n/([^/]+)/b/([^/]+)/o/(.+)";
}
