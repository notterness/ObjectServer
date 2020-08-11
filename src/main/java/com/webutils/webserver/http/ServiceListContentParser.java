package com.webutils.webserver.http;

/*
** The content data for the ListServers call takes the following form:
*
** {
**  "data": [
**   "service-name": "account-mgr-service"
**     {
**        "storage-id": "-1"
**        "server-ip": "localhost/127.0.0.1"
**        "server-port": "5003"
**        "storage-server-read-errors": "0"
**     }
**   ]
** }
*/

import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.*;

public class ServiceListContentParser extends ContentParser {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceListContentParser.class);

    private static final String DATA_STRING = "data";
    private static final int SQUARE_BRACKET_DEPTH = 1;
    private static final int SERVICE_TAG_SUB_CATEGORY_DEPTH = 1;

    private final Map<String, Map<String, String>> definedServicesTags;

    private final Stack<String> keyStringStack;

    private final LinkedList<String> requiredAttributes;
    private final LinkedList<String> requiredSubAttributes;

    private int bracketDepth;
    private boolean squareBracketFound;
    private boolean keyStrObtained;

    private String keyStr;

    /*
     ** This is set in the validateContentData() method
     */
    protected boolean contentValid;

    public ServiceListContentParser() {

        super();

        definedServicesTags = new HashMap<>(10);
        keyStringStack = new Stack<>();

        /*
         ** Fill in the list of required attributes so they are easy to check
         */
        requiredAttributes = new LinkedList<>();
        requiredAttributes.add(SERVICE_NAME);

        requiredSubAttributes = new LinkedList<>();
        requiredSubAttributes.add(STORAGE_ID);
        requiredSubAttributes.add(SERVER_IP);
        requiredSubAttributes.add(SERVER_PORT);

        bracketDepth = 0;
        squareBracketFound = false;
        keyStrObtained = false;

        contentValid = false;
    }

    public boolean isValid() {
        if (!contentValid) {
            LOG.warn("content data failed validation");
        }
        return contentValid;
    }

    public boolean validateContentData() {
        contentValid = true;
        /*
        ** First make sure that the bracketDepth is 0 to insure the brackets are properly paired after the parsing
        **   and extraction of the information was completed.
        **
        ** NOTE: This does not check for the "service-name" required attribute since that was already checked when the
        **   sub-attributes maps where created. The definedServiceTags map only holds the name of the service and the
        **   pointer to the sub-attributes.
         */
        if (bracketDepth != 0) {
            LOG.error("Invalid bracketDepth: " + bracketDepth);
            contentValid = false;
        }

        for (Map.Entry<String, Map<String, String>> entry : definedServicesTags.entrySet()) {
            Map<String, String> subCategory = entry.getValue();

            for (String subAttribute: requiredSubAttributes) {
                if (subCategory.get(subAttribute) == null) {
                    LOG.warn("ServiceList missing required sub-attribute: " + subAttribute);
                    contentValid = false;
                }
            }
        }

        if (!contentValid) {
            LOG.warn("ServiceListContentParser validateContentData() contentValid is false");
            clearAllMaps();
        }

        return contentValid;
    }

    /*
    ** This is what parses the tokenized Strings that are passed in from the PostContentParser class. The content
    **   buffers are actually extracted in the PostContentBuffers class and passed into the PostContentParser where
    **   they are tokenized and then passed into the addData() method.
     */
    public boolean addData(final String str1) {
        boolean parsingError = false;

        //LOG.info("addData - str1: " + str1);

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

                    parsingError = true;
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

            case "[":
                /*
                ** This must follow the keyStr of "data"
                 */
                if (keyStrObtained && keyStr.equalsIgnoreCase(DATA_STRING)) {
                    keyStr = null;
                    keyStrObtained = false;
                    squareBracketFound = true;
                } else {
                    if (keyStrObtained) {
                        LOG.warn("Expected keyStr to be \"data\" was: " + keyStr);
                    } else {
                        LOG.warn("Missing keyStr \"data\" - bracketDepth: " + bracketDepth);
                    }
                    parsingError = true;
                }
                break;

            case "]":
                if (bracketDepth == SQUARE_BRACKET_DEPTH) {
                    if (squareBracketFound) {
                        squareBracketFound = false;
                    } else {
                        LOG.warn("Missing opening square bracket");
                        parsingError = true;
                    }
                } else {
                    LOG.warn("Square bracket at invalid bracketDepth: " + bracketDepth);
                    parsingError = true;
                }
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
                        ** This is what is expected after the closing "}" and there needs to be a new map
                        **   created to hold the information about the service.
                         */
                        //LOG.info("empty stack - " + keyStr + " : " + str1);
                        checkForDefinedTags(keyStr, str1);
                    } else {
                        try {
                            String mapSelector = keyStringStack.peek();

                            //LOG.info("mapSelector (" + mapSelector + ") - " + keyStr + " : " + str1);

                            /*
                             ** The first check is to see if there is a map that covers this selector
                             */
                            Map<String, String> workingMap = definedServicesTags.get(mapSelector);
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
        LOG.info("Service Definition");
        for (Map.Entry<String, Map<String, String>> entry : definedServicesTags.entrySet()) {
            LOG.info("    service-name - " + entry.getKey());

            Map<String, String> subCategory = entry.getValue();
            for (Map.Entry<String, String> subCategoryEntry : subCategory.entrySet()) {
                LOG.info("        " + subCategoryEntry.getKey() + " : " + subCategoryEntry.getValue());
            }
        }
    }


    /*
     ** This is used to create (if needed) additional maps to hold sub-category information under the "service-name"
     **    category.
     */
    private boolean checkForDefinedTags(final String keyStr, final String keyStrValue) {
        boolean parsingError = false;

        /*
         ** First make sure the depth is set to 1, otherwise there is no point checking.
         **   The "service-name" listing must be at level 1. The input data follows the following pattern:
         **
         **   {  - Bracket Depth is 1
         **      "data": [   - squareBracketFound is set to true
         **      "service-name": "account-mgr-service"
         **      {  - Bracket Depth is 2, "account-mgr-service" is pushed onto the stack
         **          "MyTags" - keyStrObtained is set to true
         */
        if ((bracketDepth == SERVICE_TAG_SUB_CATEGORY_DEPTH) && squareBracketFound) {
            /*
             ** First check if the keyStr equals "service-name"
             */
            if (keyStr.equalsIgnoreCase(ContentParser.SERVICE_NAME)) {
                if (!definedServicesTags.containsKey(keyStrValue)) {
                    //LOG.info("Creating new map under definedServicesTags: " + keyStrValue);

                    Map<String, String> subCategoryMap = new HashMap<> (4);

                    definedServicesTags.put(keyStrValue, subCategoryMap);

                    keyStringStack.push(keyStrValue);
                } else {
                    LOG.warn("Multiple entries for \"" + ContentParser.SERVICE_NAME + "\" - " + keyStrValue);
                    parsingError = true;
                }

            } else {
                LOG.warn("checkForDefinedTags() expected keyStr to equal \"" + ContentParser.SERVICE_NAME + "\" instead was: " +
                        keyStr);
                parsingError = true;
            }
        }

        return (!parsingError);
    }

    /*
     ** This is called either when there is a parsing error or after the data has all been processed (i.e. used to
     **   obtain the service information). It is the cleanup step to insure that all resources have been released.
     */
    protected void clearAllMaps() {
        for (Map.Entry<String, Map<String, String>> entry : definedServicesTags.entrySet()) {
            Map<String, String> subCategory = entry.getValue();
            subCategory.clear();
        }
        definedServicesTags.clear();

        /*
         ** Clear out the stack just in case
         */
        keyStringStack.clear();
        bracketDepth = 0;
        keyStr = null;
        keyStrObtained = false;
    }

    /*
     ** This pulls the information passed back in the response content from the AllocateChunks method and puts it
     **   into the ServerIdentifier class (actually a list of ServerIdentifiers).
     */
    public void extractServers(final List<ServerIdentifier> servers) {

        LOG.info("extractServers()");
        for (Map.Entry<String, Map<String, String>> entry : definedServicesTags.entrySet()) {

            Map<String, String> subCategory = entry.getValue();

            String serviceName = entry.getKey();
            LOG.info("extractServers() serviceName: " + serviceName);
            InetAddress inetAddress = getServerIp(subCategory);
            LOG.info("extractServers() serviceName: " + serviceName + " IP: " + inetAddress);
            if (inetAddress != null) {
                int port = getServerPort(subCategory);
                ServerIdentifier server = new ServerIdentifier(serviceName, inetAddress, port, 0);

                LOG.info("extractContent() serverName: " + serviceName + " IP: " + inetAddress + " Port: " + port);

                /*
                 ** Need to add the rest of the fields
                 */
                servers.add(server);
            }
        }

        LOG.info("extractContent() servers found: " + servers.size());
    }

}
