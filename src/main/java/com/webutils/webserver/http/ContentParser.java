package com.webutils.webserver.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public abstract class ContentParser {
    private static final Logger LOG = LoggerFactory.getLogger(ContentParser.class);

    public static final String FREE_FORM_TAG = "freeformTags";
    public static final String DEFINED_TAGS = "definedTags";

    private static final String CHUNK_ALLOC_RESPONSE = "chunk-";

    public static final String COMPARTMENT_ID_ATTRIBUTE = "compartmentId";
    protected static final String STORAGE_TIER_ATTRIBUTE = "storageTier";

    /*
    ** These are used by multiple content parsers, so they are kept in the base class
     */
    public static final String SERVICE_NAME = "service-name";
    public static final String STORAGE_ID = "storage-id";
    public static final String SERVER_IP = "server-ip";
    public static final String SERVER_PORT = "server-port";
    public static final String CHUNK_UID = "chunk-etag";


    private static final int DEFINED_TAGS_SUB_CATEGORY_DEPTH = 3;
    private static final int CHUNK_ALLOCATION_CATEGORY_DEPTH = 2;

    protected final Map<String, String> params;

    protected final Map<String, String> freeformTags;

    protected final Map<String, Map<String, String>> definedTags;

    protected final Map<String, Map<String, String>> chunkAllocations;

    protected final Map<String, Map<String, String>> keyValuePairPlacement;

    protected final Stack<String> keyStringStack;

    protected final LinkedList<String> requiredAttributes;

    protected int bracketDepth;
    private boolean keyStrObtained;

    private String keyStr;

    /*
    ** This is set in the validateContentData() method
     */
    protected boolean contentValid;

    public ContentParser() {
        params = new HashMap<>(10);
        freeformTags = new HashMap<>(10);
        definedTags = new HashMap<>(10);
        chunkAllocations = new HashMap<>(5);

        keyValuePairPlacement = new HashMap<>(10);
        keyValuePairPlacement.put(FREE_FORM_TAG, freeformTags);

        keyStringStack = new Stack<>();

        /*
         ** Fill in the list of required attributes so they are easy to check
         */
        requiredAttributes = new LinkedList<>();

        bracketDepth = 0;
        keyStrObtained = false;

        contentValid = false;
    }

    public boolean isValid() {
        if (!contentValid) {
            LOG.warn("content data failed validation");
        }
        return contentValid;
    }

    public abstract boolean validateContentData();

    public boolean addData(final String str1) {
        boolean parsingError = false;

        /*
         ** First check if the String is a open or closing bracket
         */
        switch (str1) {
            case "{":
                bracketDepth++;

                /*
                 ** The first time this is called (bracketDepth == 1 at this point), keyStrObtained will be false.
                 */
                if (keyStrObtained) {
                    //LOG.info("Open bracket " + bracketDepth + " - " + keyStr);

                    /*
                     ** Special case checking for "definedTags" and "chunk-". This must take place prior to pushing the
                     **   new keyStr onto the stack since it looks at the value at the top of the stack.
                     */
                    if (checkForDefinedTags(keyStr)) {

                        /*
                         ** Validate that the keyStr is not null
                         */
                        keyStringStack.push(keyStr);

                        keyStr = null;
                        keyStrObtained = false;
                    } else {
                        parsingError = true;
                    }
                } else {
                    //LOG.info("Open bracket " + bracketDepth);
                }
                break;
            case "}":
                bracketDepth--;
                if (bracketDepth < 0) {
                    /*
                     ** Need to register a parser error and give up
                     */
                    LOG.error("Closing bracket, bracketDepth went negative");
                    parsingError = true;
                }

                if (bracketDepth > 0) {
                    try {
                        String removedStr = keyStringStack.pop();
                        //LOG.info("Closing bracket " + bracketDepth + " - " + removedStr);
                    } catch (EmptyStackException ex) {
                        LOG.error("Closing bracket, should be something on stack: " + ex.getMessage());
                        parsingError = true;
                    }
                } else {
                    //LOG.info("Closing bracket " + bracketDepth);
                }
                break;
            case ":":
                if (!keyStrObtained) {
                    LOG.error(" POST parsing found ':' without first finding the key");
                    parsingError = true;
                }
                break;
            case ",":
                /*
                 ** Nothing to do here
                 */
                break;
            default:
                /*
                 ** This is is either a key string or an identifier
                 */
                if (!keyStrObtained) {
                    keyStr = str1;
                    keyStrObtained = true;
                    //LOG.info("keyStr: " + keyStr);
                } else {
                    /*
                     ** Check if there is anything in the stack, the top element is the type of Map to add to
                     */
                    if (keyStringStack.empty()) {
                        /*
                         ** Simple case, just add to the normal bucketParams Map. To output what is being added to the
                         **   attribute map, uncomment the following LOG statement.
                         */
                        //LOG.info("empty stack - " + keyStr + " : " + str1);
                        params.put(keyStr, str1);
                    } else {
                        try {
                            String mapSelector = keyStringStack.peek();

                            //LOG.info("mapSelector (" + mapSelector + ") - " + keyStr + " : " + str1);

                            /*
                             ** The first check is to see if there is a map that covers this selector
                             */
                            Map<String, String> workingMap = keyValuePairPlacement.get(mapSelector);
                            if (workingMap != null) {
                                workingMap.put(keyStr, str1);
                            } else {
                                LOG.error("Unable to find Map: " + mapSelector);
                                parsingError = true;
                            }
                        } catch (EmptyStackException ex) {
                            LOG.error("Expected to find stack entry: " + bracketDepth + " " + ex.getMessage());
                            parsingError = true;
                        }
                    }

                    keyStr = null;
                    keyStrObtained = false;
                }
                break;
        }

        /*
         ** When a parsing error is hit, clean up the resources
         */
        if (parsingError) {
            LOG.error("Parsing error - clearing all data");
            clearAllMaps();
        }

        return (!parsingError);
    }

    /*
     ** Debug method to display the information that has been stored in the various maps from the information in the
     **   Create Bucket POST operation.
     */
    public void dumpMaps() {
        LOG.info("params");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            LOG.info("    " + entry.getKey() + " : " + entry.getValue());
        }

        LOG.info("freeFormTags");
        for (Map.Entry<String, String> entry : freeformTags.entrySet()) {
            LOG.info("    " + entry.getKey() + " : " + entry.getValue());
        }

        LOG.info("definedTags");
        for (Map.Entry<String, Map<String, String>> entry : definedTags.entrySet()) {
            LOG.info("    sub-category - " + entry.getKey());

            Map<String, String> subCategory = entry.getValue();
            for (Map.Entry<String, String> subCategoryEntry : subCategory.entrySet()) {
                LOG.info("        " + subCategoryEntry.getKey() + " : " + subCategoryEntry.getValue());
            }
        }
    }


    /*
     ** This is used to create (if needed) additional maps to hold sub-category information under the "definedTags"
     **    category.
     */
    private boolean checkForDefinedTags(final String keyStr) {
        boolean parsingError = false;

        /*
         ** First make sure the depth is set to 3, otherwise there is no point checking.
         **   The "definedTags" listing must be at level 3. The input data follows the following pattern:
         **
         **   {  - Bracket Depth is 1
         **      "definedTags"
         **      {  - Bracket Depth is 2, "definedTags is pushed onto the stack
         **          "MyTags" - keyStrObtained is set to true
         **          {  - Bracket Depth is 3, "definedTags" is at the top of the stack
         */
        if (bracketDepth == DEFINED_TAGS_SUB_CATEGORY_DEPTH) {
            /*
             ** First check if the top of the stack contains "definedTags"
             */
            try {
                String topOfStackStr = keyStringStack.peek();
                if (topOfStackStr.equals(DEFINED_TAGS)) {
                    /*
                     ** This means a new Map is needed to hold the values for the sub-tags (these are key value pairs
                     **   that are under a new sub-category within the "definedTags" grouping).
                     */
                    if (!definedTags.containsKey(keyStr)) {
                        //LOG.info("Creating new map under definedTags: " + keyStr);

                        Map<String, String> subCategoryMap = new HashMap<> (5);

                        definedTags.put(keyStr, subCategoryMap);

                        /*
                         ** Also add this to the keyValuePairPlacement map so the items can easily be added
                         */
                        keyValuePairPlacement.put(keyStr, subCategoryMap);
                    }
                }
            } catch (EmptyStackException ex) {
                LOG.error("bracketDepth == " + bracketDepth + ", but stack is empty");
                parsingError = true;
            }
        } else if (bracketDepth == CHUNK_ALLOCATION_CATEGORY_DEPTH) {
            /*
             ** First check if the top of the stack contains "chunk-"
             */
            if (keyStr.contains(CHUNK_ALLOC_RESPONSE)) {
                /*
                ** This means a new Map is needed to hold the values for the chunk results (these are key value pairs
                **   that are under a new sub-category within the "chunkAllocations" grouping).
                 */
                if (!chunkAllocations.containsKey(keyStr)) {
                    LOG.info("Creating new map under chunkAllocations: " + keyStr);
                    Map<String, String> subCategoryMap = new HashMap<>(5);
                    chunkAllocations.put(keyStr, subCategoryMap);

                    /*
                    ** Also add this to the keyValuePairPlacement map so the items can easily be added
                    */
                    keyValuePairPlacement.put(keyStr, subCategoryMap);
                }
            }
        }

        return (!parsingError);
    }

    /*
     ** This is called either when there is a parsing error or after the data has all been processed (i.e. used to create
     **   the Bucket). It is the cleanup step to insure that all resources have been released.
     */
    protected void clearAllMaps() {
        for (Map.Entry<String, Map<String, String>> entry : definedTags.entrySet()) {
            entry.getValue().clear();
        }
        definedTags.clear();

        params.clear();
        freeformTags.clear();

        keyValuePairPlacement.clear();

        for (Map.Entry<String, Map<String, String>> entry : chunkAllocations.entrySet()) {
            Map<String, String> subCategory = entry.getValue();
            subCategory.clear();
        }
        chunkAllocations.clear();

        /*
         ** Clear out the stack just in case
         */
        keyStringStack.clear();
        bracketDepth = 0;
        keyStr = null;
        keyStrObtained = false;
    }

    /*
     ** The possible Storage Tiers are:
     **
     **   Standard - 3 copies of the data within the same data center
     **   Intelligent-Tiering - Moves data between fast and slow disk depending on access patterns. Always uses 3
     **     copies of the data.
     **   Standard-IA (Infrequent Access) - 3 copies on slow disk
     **   OneZone (Another form of Infrequent Access with less redundancy) - 2 copies of the data on slow disk.
     **   Archive (slower access than Standard-IA) -
     **   DeepArchive (slowest access of all, data may be kept on offline storage) -
     **
     ** NOTE: If the "storageTier" attribute is not set, then the default is "Standard"
     */
    public StorageTierEnum getStorageTier() {
        String storageTierStr = params.get(STORAGE_TIER_ATTRIBUTE);

        StorageTierEnum storageTier;
        if (storageTierStr == null) {
            storageTier = StorageTierEnum.STANDARD_TIER;
        } else {
            storageTier = StorageTierEnum.fromString(storageTierStr);
        }

        return storageTier;
    }

    /*
     ** The following is used to extract the following Strings from the response:
     **   service-name
     **   chunk-etag
     **   chunk-location
     */
    protected String getStr(final Map<String, String> subCategory, final String requestedStr) {
        String str = subCategory.get(requestedStr);

        if (str == null) {
            LOG.warn(requestedStr + " attribute is missing");
        }
        return str;
    }

    /*
     ** The following is used to pull out the "storage-server-ip". This needs to handle the case where the hostname
     **   string is something like:
     **     "localhost/127.0.0.1"
     **    This is the purpose of the StringTokenizer() used below.
     */
    protected InetAddress getServerIp(final Map<String, String> subCategory) {
        String str = subCategory.get(SERVER_IP);

        //LOG.info("serverIp: " + str);

        InetAddress inetAddress = null;
        if (str != null) {
            String hostName;

            StringTokenizer stk = new StringTokenizer(str, " /");
            if (stk.hasMoreTokens()) {
                hostName = stk.nextToken();
            } else {
                hostName = str;
            }

            try {
                inetAddress = InetAddress.getByName(hostName);
            } catch (UnknownHostException ex) {
                LOG.warn("IP address results in unknown host: " + str + " ex: " + ex.getMessage());
            }
        } else {
            LOG.warn(SERVER_IP + " attribute is missing");
        }

        return inetAddress;
    }

    /*
     ** The following is used to obtain the "server-port" for the allocated chunk
     */
    protected int getServerPort(final Map<String, String> subCategory) {
        String portStr = subCategory.get(SERVER_PORT);
        int port = -1;

        if (portStr != null) {
            try {
                port = Integer.parseInt(portStr);

                /*
                 ** Make sure it is a positive integer
                 */
                if (port < 0) {
                    LOG.warn("storage-server-port must be a positive integer - " + port);
                    port = -1;
                }
            } catch (NumberFormatException ex) {
                LOG.warn(SERVER_PORT + " is invalid: " + portStr);
            }
        } else {
            LOG.warn("storage-server-port attribute is missing");
        }
        return port;
    }


}
