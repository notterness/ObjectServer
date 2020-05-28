package com.webutils.webserver.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class ParseRequestContent {
    private static final Logger LOG = LoggerFactory.getLogger(ParseRequestContent.class);

    private static final String FREE_FORM_TAG = "freeformTags";
    private static final String DEFINED_TAGS = "definedTags";

    protected static final String COMPARTMENT_ID_ATTRIBUTE = "compartmentId";
    protected static final String STORAGE_TIER_ATTRIBUTE = "storageTier";

    private static final int DEFINED_TAGS_SUB_CATEGORY_DEPTH = 3;

    protected final Map<String, String> params;

    protected final Map<String, String> freeformTags;

    protected final Map<String, Map<String, String>> definedTags;

    protected final Map<String, Map<String, String>> keyValuePairPlacement;

    protected final Stack<String> keyStringStack;

    protected final LinkedList<String> requiredAttributes;

    protected int bracketDepth;
    private boolean keyStrObtained;

    private String keyStr;

    public ParseRequestContent() {
        params = new HashMap<>(10);
        freeformTags = new HashMap<>(10);
        definedTags = new HashMap<>(10);

        keyValuePairPlacement = new HashMap<>(10);
        keyValuePairPlacement.put(FREE_FORM_TAG, freeformTags);

        keyStringStack = new Stack<>();

        /*
         ** Fill in the list of required attributes so they are easy to check
         */
        requiredAttributes = new LinkedList<>();

        bracketDepth = 0;
        keyStrObtained = false;
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
                     ** Special case checking for "definedTags". This must take place prior to pushing the
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
                    LOG.info("Closing bracket " + bracketDepth);
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
                         ** Simple case, just add to the normal bucketParams Map
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
        LOG.info("bucketParams");
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
                LOG.error("bracketDepth == 2, but stack is empty");
                parsingError = true;
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
     **   OneZone (Another form of Infrequent Access with less redundacy) - 2 copies of the data on slow disk.
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


}
