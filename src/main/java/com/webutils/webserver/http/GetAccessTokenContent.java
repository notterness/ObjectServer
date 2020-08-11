package com.webutils.webserver.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class GetAccessTokenContent extends ContentParser {

    private static final Logger LOG = LoggerFactory.getLogger(GetAccessTokenContent.class);

    public GetAccessTokenContent() {
        super();

        /*
         ** Fill in the list of required attributes so they are easy to check
         */
        requiredAttributes.add(CreateTenancyPostContent.CUSTOMER_ATTRIBUTE);
        requiredAttributes.add(CreateUserPostContent.USER_NAME_ATTRIBUTE);
        requiredAttributes.add(CreateUserPostContent.USER_PASSWORD_ATTRIBUTE);
    }

    /*
     ** This is used to validate that the required fields are present for the Get Access Token operation.
     **   The required fields are:
     **     "tenancyName" - This is the Tenancy name
     **     "customer" - The owner of the Tenancy.
     **     "userName" - The name of the user the access token is being requested for.
     **     "password" - The password of the user the access token is being requested for.
     **
     **   Optional fields are:
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
        } else {
            LOG.error("Invalid bracketDepth: " + bracketDepth);
            contentValid = false;
        }
        if (!contentValid) {
            clearAllMaps();
        }

        return contentValid;
    }

    public String getTenancyName() {
        return params.get(CreateTenancyPostContent.TENANCY_NAME_ATTRIBUTE);
    }

    public String getCustomer() { return params.get(CreateTenancyPostContent.CUSTOMER_ATTRIBUTE); }

    public String getUser() {
        return params.get(CreateUserPostContent.USER_NAME_ATTRIBUTE);
    }

    public String getPassword() { return params.get(CreateUserPostContent.USER_PASSWORD_ATTRIBUTE); }

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
