package com.webutils.webserver.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class CreateUserPostContent extends ContentParser {

    private static final Logger LOG = LoggerFactory.getLogger(CreateBucketPostContent.class);

    public static final String USER_NAME_ATTRIBUTE = "userName";
    public static final String USER_PASSWORD_ATTRIBUTE = "password";
    public static final String USER_PERMISSION_ATTRIBUTE = "permissions";

    public CreateUserPostContent() {
        super();

        /*
         ** Fill in the list of required attributes so they are easy to check
         */
        requiredAttributes.add(CreateTenancyPostContent.TENANCY_NAME_ATTRIBUTE);
        requiredAttributes.add(CreateTenancyPostContent.CUSTOMER_ATTRIBUTE);
        requiredAttributes.add(USER_NAME_ATTRIBUTE);
        requiredAttributes.add(USER_PASSWORD_ATTRIBUTE);
        requiredAttributes.add(USER_PERMISSION_ATTRIBUTE);
    }

    /*
     ** This is used to validate that the required fields are present for the Create Bucket operation.
     **   The required fields are:
     **     "tenancyName" - This is the Tenancy name
     **     "customer" - The owner of the Tenancy.
     **     "userName" - The name of the user being added
     **     "password" - The password of the user being added
     **     "permissions" - An integer representation of the permissions for the user
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
        return params.get(USER_NAME_ATTRIBUTE);
    }

    public String getPassword() { return params.get(USER_PASSWORD_ATTRIBUTE); }

    public int getPermissions() {
        String permissionsStr = params.get(USER_PERMISSION_ATTRIBUTE);
        int permissions = -1;

        if (permissionsStr != null) {
            try {
                permissions = Integer.parseInt(permissionsStr);

                /*
                 ** TODO: Determine valid range for port numbers
                 */
                if (permissions < 0) {
                    LOG.warn(SERVER_PORT + " must be a positive integer - " + permissions);
                    permissions = -1;
                }
            } catch (NumberFormatException ex) {
                LOG.warn(USER_PERMISSION_ATTRIBUTE + " is invalid: " + permissionsStr);
            }
        } else {
            LOG.warn(USER_PERMISSION_ATTRIBUTE + " attribute is missing");
        }
        return permissions;
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
