package com.webutils.webserver.http;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PostContentData {
    private static final Logger LOG = LoggerFactory.getLogger(PostContentData.class);

    private static final String FREE_FORM_TAG = "freeformTags";
    private static final String DEFINED_TAGS = "definedTags";

    private static final int DEFINED_TAGS_SUB_CATEGORY_DEPTH = 3;

    private static final String NAME_ATTRIBUTE = "name";
    private static final String COMPARTMENT_ID_ATTRIBUTE = "compartmentId";

    private final Map<String, String> bucketParams;

    private final Map<String, String> freeformTags;

    private final Map<String, Map<String, String>> definedTags;

    private final Map<String, Map<String, String>> keyValuePairPlacement;

    private final Stack<String> keyStringStack;

    private final LinkedList<String> requiredAttributes;

    private int bracketDepth;
    private boolean keyStrObtained;

    private String keyStr;

    public PostContentData() {

        bucketParams = new HashMap<>(10);
        freeformTags = new HashMap<>(10);
        definedTags = new HashMap<>(10);

        keyValuePairPlacement = new HashMap<>(10);
        keyValuePairPlacement.put(FREE_FORM_TAG, freeformTags);

        keyStringStack = new Stack<>();

        /*
        ** Fill in the list of required attributes so they are easy to check
         */
        requiredAttributes = new LinkedList<>();
        requiredAttributes.add(NAME_ATTRIBUTE);
        requiredAttributes.add(COMPARTMENT_ID_ATTRIBUTE);

        bracketDepth = 0;
        keyStrObtained = false;
    }

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
                    LOG.info("Open bracket " + bracketDepth + " - " + keyStr);

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
                    LOG.info("Open bracket " + bracketDepth);
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
                        LOG.info("Closing bracket " + bracketDepth + " - " + removedStr);
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
                    LOG.info("keyStr: " + keyStr);
                } else {
                    /*
                     ** Check if there is anything in the stack, the top element is the type of Map to add to
                     */
                    if (keyStringStack.empty()) {
                        /*
                         ** Simple case, just add to the normal bucketParams Map
                         */
                        LOG.info("empty stack - " + keyStr + " : " + str1);
                        bucketParams.put(keyStr, str1);
                    } else {
                        try {
                            String mapSelector = keyStringStack.peek();

                            LOG.info("mapSelector (" + mapSelector + ") - " + keyStr + " : " + str1);

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
    ** This is used to validate that the required fields are present for the Create Bucket operation.
    **   The required fields are:
    **     "name" - This is the Bucket name
    **     "compartmentId" - THe ID of the compartment in which to create the bucket.
    **
    **   Optional fields are:
    **     "metadata" - A String up to 4kB in length
    **     "publicAccessType" -
    **     "storageTier" - default is "Standard", allowed values are "Standard" and "Archive"
    **     "objectEventsEnabled" - boolean
    **     "freeformTags"
    **     "definedTags"
    **     "kmsKeyId" - UID to access the master encryption key.
    **
    ** NOTE: At some point it might be worth validating that there are no unexpected attributes passed in. The other
    **   thing to validate would be the contents of the attributes to make sure garbage data is not provided.
     */
    public boolean validatePostContentData() {
        boolean valid = true;

        /*
        ** First make sure that the bracketDepth is 0 to insure the brackets are properly paired.
         */
        if (bracketDepth == 0) {
            /*
            ** Make sure the required information is present
             */
            for (String attribute : requiredAttributes) {
                if (!bucketParams.containsKey(attribute)) {
                    valid = false;

                    LOG.error("Missing required attribute: " + attribute);
                    break;
                }
            }

        } else {
            LOG.error("Invalid bracketDepth: " + bracketDepth);
            valid = false;
        }
        if (!valid) {
            clearAllMaps();
        }

        return valid;
    }

    /*
    ** Debug method to display the information that has been stored in the various maps from the information in the
    **   Create Bucket POST operation.
     */
    public void dumpMaps() {
        LOG.info("bucketParams");
        for (Map.Entry<String, String> entry : bucketParams.entrySet()) {
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
                        LOG.info("Creating new map under definedTags: " + keyStr);

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
    private void clearAllMaps() {
        for (Map.Entry<String, Map<String, String>> entry : definedTags.entrySet()) {
            entry.getValue().clear();
        }
        definedTags.clear();

        bucketParams.clear();
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
}
