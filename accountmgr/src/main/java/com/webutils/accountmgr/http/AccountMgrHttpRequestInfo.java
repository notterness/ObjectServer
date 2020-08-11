package com.webutils.accountmgr.http;

import com.webutils.webserver.http.HttpMethodEnum;
import com.webutils.webserver.http.HttpRequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountMgrHttpRequestInfo extends HttpRequestInfo {

    private static final Logger LOG = LoggerFactory.getLogger(AccountMgrHttpRequestInfo.class);

    public AccountMgrHttpRequestInfo() {
        super();
    }

    /*
     ** When the headers have been completely read in, that will be the time to insure it is valid
     **   and the field values make sense.
     */
    public void setHeaderComplete() {

        boolean tenancySet = getTenancySetInUri();
        switch (httpMethod) {
            case PUT_METHOD:
                break;

            case POST_METHOD:
                boolean userSet = getUserSetInUri();
                if (tenancySet) {
                    /*
                     ** This is the POST Tenancy request meaning the URI had "/t" in it.
                     */
                    LOG.info("Setting httpMethod to POST_TENANCY_METHOD");
                    httpMethod = HttpMethodEnum.POST_TENANCY_METHOD;
                } else if (userSet) {
                    /*
                     ** This is the POST User request meaning the URI had "/u" in it.
                     */
                    LOG.info("Setting httpMethod to POST_USER_METHOD");
                    httpMethod = HttpMethodEnum.POST_USER_METHOD;
                }
                break;

            case GET_METHOD:
                String tenancyName = getTenancyFromUri();
                if (isHealthCheck()) {
                    /*
                     ** This is the Health Check request meaning the URI had "/health" in it.
                     */
                    LOG.info("Setting httpMethod to HEALTH_CHECK");
                    httpMethod = HttpMethodEnum.HEALTH_CHECK;
                } else if (tenancySet) {
                    /*
                     ** This is the GetTenancyId from the AccessToken method. This is the case when just "/t" is
                     **   passed in the URI
                     */
                    LOG.info("Setting httpMethod to GET_TENANCY_ID");
                    httpMethod = HttpMethodEnum.GET_TENANCY_ID;
                } else if (tenancyName != null) {
                    /*
                     ** This is the GetAccessToken method.
                     */
                    LOG.info("Setting httpMethod to GET_ACCESS_TOKEN");
                    httpMethod = HttpMethodEnum.GET_ACCESS_TOKEN;
                }
                break;

            case DELETE_METHOD:
                break;

            default:
                LOG.warn("Illegal method: " + httpMethod.toString());
                break;
        }

        headerComplete = true;
    }

}
