package com.webutils.objectserver.http;

import com.webutils.webserver.http.HttpMethodEnum;
import com.webutils.webserver.http.HttpRequestInfo;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectServerHttpRequestInfo extends HttpRequestInfo {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectServerHttpRequestInfo.class);

    public ObjectServerHttpRequestInfo() {
        super();
    }

    /*
     ** When the headers have been completely read in, that will be the time to insure it is valid
     **   and the field values make sense.
     */
    public void setHeaderComplete() {
        /*
         ** Verify that the "Content-Length" header has been received. It is an error if it has not
         */
        if (!contentLengthReceived) {
            LOG.warn("No Content-Length [" + requestId +  "]");
            String failureMessage = "{\r\n  \"code\":" + HttpStatus.BAD_REQUEST_400 +
                    "\r\n  \"message\": \"Missing required attributes - Content-Length is missing\" + " +
                    "\r\n}";
            setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
            return;
        }

        /*
         ** Verify that there was an Object Name, Bucket Name and Tenant Name passed in
         */
        String bucketName = getBucket();
        String namespace = getNamespace();
        String objectName = getObject();

        switch (httpMethod) {
            case PUT_METHOD:
                if (objectName == null) {
                    LOG.warn("PUT Missing Critical Object Info [" + requestId + "] objectName is null");

                    String failureMessage = "{\r\n  \"code\":" + HttpStatus.BAD_REQUEST_400 +
                            "\r\n  \"message\": \"Missing required attributes - objectName is missing\" + " +
                            "\r\n}";
                    LOG.warn(failureMessage);
                    setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
                } else  {
                    /*
                     ** Special handling for the PUT command to write the chunk data to the Storage Servers
                     */
                    if (getBucket() == null) {
                        LOG.warn("PUT Missing Critical Object Info [" + requestId + "] bucketName is missing");

                        String failureMessage = "{\r\n  \"code\":" + HttpStatus.BAD_REQUEST_400 +
                                "\r\n  \"message\": \"Missing required attributes - bucketName is missing\" + " +
                                "\r\n}";
                        LOG.warn(failureMessage);
                        setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
                    } else if (getNamespace() == null) {
                        LOG.warn("PUT Missing Critical Object Info [" + requestId + "] namespace is missing");

                        String failureMessage = "{\r\n  \"code\":" + HttpStatus.BAD_REQUEST_400 +
                                "\r\n  \"message\": \"Missing required attributes - namespace is missing\" + " +
                                "\r\n}";
                        LOG.warn(failureMessage);
                        setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
                    }
                }
                break;

            case POST_METHOD:
                if (bucketName != null) {
                    LOG.warn("POST Critical Object Info [" + requestId + "] bucketName is missing");

                    String failureMessage = "{\r\n  \"code\":" + HttpStatus.BAD_REQUEST_400 +
                            "\r\n  \"message\": \"/b/ Attribute must not be set\" + " +
                            "\r\n}";
                    LOG.warn(failureMessage);
                    setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
                } else if (namespace == null) {
                    LOG.warn("POST Missing Critical Object Info [" + requestId + "] namespace is missing");

                    String failureMessage = "{\r\n  \"code\":" + HttpStatus.BAD_REQUEST_400 +
                            "\r\n  \"message\": \"Missing required attributes - namespace is missing\" + " +
                            "\r\n}";
                    LOG.warn(failureMessage);
                    setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
                }
                break;

            case GET_METHOD:
                /*
                 ** This needs to handle the differences between the Object GET (retrieve the data for the object),
                 **   the ListObjects (which provides a list of all the objects that exist under a bucket) and
                 **   the Health Check GET.
                 */
                if (isHealthCheck()) {
                    /*
                    ** This is the Health Check request meaning the URI had "/health" in it.
                     */
                    LOG.info("Setting httpMethod to HEALTH_CHECK");
                    httpMethod = HttpMethodEnum.HEALTH_CHECK;
                } else if (bucketName == null) {
                    LOG.warn("GET Missing Critical Object Info [" + requestId + "] bucketName is missing");

                    String failureMessage = "{\r\n  \"code\":" + HttpStatus.BAD_REQUEST_400 +
                            "\r\n  \"message\": \"Missing required attributes - bucketName is missing\" + " +
                            "\r\n}";
                    LOG.warn(failureMessage);
                    setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
                } else if (namespace == null) {
                    LOG.warn("GET Missing Critical Object Info [" + requestId + "] namespace is missing");

                    String failureMessage = "{\r\n  \"code\":" + HttpStatus.BAD_REQUEST_400 +
                            "\r\n  \"message\": \"Missing required attributes - namespace is missing\" + " +
                            "\r\n}";
                    LOG.warn(failureMessage);
                    setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
                } else if (objectName == null) {
                    /*
                     ** When the "/o" is empty, then this is the LIST_METHOD
                     */
                    LOG.info("Setting httpMethod to LIST_METHOD");
                    httpMethod = HttpMethodEnum.LIST_METHOD;
                }
                break;

            case DELETE_METHOD:
                if (bucketName == null) {
                    LOG.warn("DELETE Missing Critical Object Info [" + requestId + "] bucketName is missing");

                    String failureMessage = "{\r\n  \"code\":" + HttpStatus.BAD_REQUEST_400 +
                            "\r\n  \"message\": \"Missing required attributes - bucketName is missing\" + " +
                            "\r\n}";
                    LOG.warn(failureMessage);
                    setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
                } else if (namespace == null) {
                    LOG.warn("DELETE Missing Critical Object Info [" + requestId + "] namespace is missing");

                    String failureMessage = "{\r\n  \"code\":" + HttpStatus.BAD_REQUEST_400 +
                            "\r\n  \"message\": \"Missing required attributes - namespace is missing\" + " +
                            "\r\n}";
                    LOG.warn(failureMessage);
                    setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
                } else if (objectName == null) {
                    /*
                     ** When the "/o" is empty, then this is the DELETE Bucket method
                     */
                    LOG.info("Setting httpMethod to DELETE_BUCKET");
                    httpMethod = HttpMethodEnum.DELETE_BUCKET;
                }
                break;

            default:
                LOG.warn("Illegal method: " + httpMethod.toString());
                break;
        }

        headerComplete = true;
    }

}
