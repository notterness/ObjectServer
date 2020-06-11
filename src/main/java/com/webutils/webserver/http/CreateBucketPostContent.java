package com.webutils.webserver.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
** This parses the content for the CreateBucket POST method. An example of the content is:
**   {
**       "compartmentId": "clienttest.compartment.12345.abcde",
**       "namespace": "testnamespace",
**       "name": "CreateBucket_Simple",
**       "objectEventsEnabled": false,
**       "freeformTags": {"Test_1": "Test_2"},
**       "definedTags":
**       {
**           "MyTags":
**           {
**               "TestTag_1": "ABC",
**               "TestTag_2": "123",
**           }
**       }
**  }
**
 */
public class CreateBucketPostContent extends ContentParser {
    private static final Logger LOG = LoggerFactory.getLogger(CreateBucketPostContent.class);

    public static final String NAME_ATTRIBUTE = "name";
    public static final String NAMESPACE_ATTRIBUTE = "namespace";
    public static final String EVENTS_ENABLED_ATTRIBUTE = "objectEventsEnabled";

    public CreateBucketPostContent() {
        super();

        /*
        ** Fill in the list of required attributes so they are easy to check
         */
        requiredAttributes.add(NAME_ATTRIBUTE);
        requiredAttributes.add(NAMESPACE_ATTRIBUTE);
        requiredAttributes.add(COMPARTMENT_ID_ATTRIBUTE);
    }

    /*
    ** This is used to validate that the required fields are present for the Create Bucket operation.
    **   The required fields are:
    **     "name" - This is the Bucket name
    **     "compartmentId" - The ID of the compartment in which to create the bucket.
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
    public boolean validateContentData() {
        contentValid = true;

        /*
        ** First make sure that the bracketDepth is 0 to insure the brackets are properly paired.
         */
        if (bracketDepth == 0) {
            /*
            ** Make sure the required information is present
             */
            for (String attribute : requiredAttributes) {
                if (!params.containsKey(attribute)) {
                    contentValid = false;

                    LOG.error("Missing required attribute: " + attribute);
                    break;
                }
            }

            if (getStorageTier() == StorageTierEnum.INVALID_TIER) {
                contentValid = false;
            }
        } else {
            LOG.error("Invalid bracketDepth: " + bracketDepth);
            contentValid = false;
        }
        if (!contentValid) {
            clearAllMaps();
        }

        return contentValid;
    }

    public String getBucketName() {
        return params.get(NAME_ATTRIBUTE);
    }

    public String getNamespace() { return params.get(NAMESPACE_ATTRIBUTE); }

    public String getCompartmentId() {
        return params.get(COMPARTMENT_ID_ATTRIBUTE);
    }

    public int getObjectEventsEnabled() {
        int enabled;

        String eventsEnabled = params.get(EVENTS_ENABLED_ATTRIBUTE);
        if (eventsEnabled == null) {
            enabled = 0;
        } else if (eventsEnabled.equals("true")) {
            enabled = 1;
        } else {
            enabled = 0;
        }

        return enabled;
    }

    public Set<Map.Entry<String, String>> getFreeFormTags() {
        return freeformTags.entrySet();
    }

    public Set<Map.Entry<String, String>> getDefinedTags(final String subTagName) {
        Map<String, String> subCategory = definedTags.get(subTagName);

        if (subCategory != null) {
            return subCategory.entrySet();
        } else {
            return null;
        }
    }

    public Set<String> getDefinedTagsSubTagKeys() {
        return definedTags.keySet();
    }

}
