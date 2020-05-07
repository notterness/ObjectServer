package com.webutils.webserver.common;

/*
** These are the parameters needed to send the GET object command to the Object Server
 */
public class GetObjectParams extends ObjectParams {

    /*
    ** The following are optional headers
     */
    private String versionId;

    private String ifMatch;
    private String ifNoneMatch;

    private String opcClientRequestId;
    private String hostName;

    private String objectFilePath;

    public GetObjectParams(final String namespace, final String bucket, final String object, final String objectFilePath) {

        super(namespace, bucket, object, objectFilePath);
        this.opcClientRequestId = null;
    }

    public String constructRequest() {
        String initialContent = "GET /n/" + namespaceName +
                "/b/" + bucketName +
                "/o/" + objectName + " HTTP/1.1\n";

        String finalContent = "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: ClientRequest/0.0.1\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "Content-Length: 0\n\n";

        String request;
        if (hostName != null) {
            request = initialContent + "Host: " + hostName + "\n";
        } else {
            request = initialContent + "Host: ClientTest\n";
        }

        if (opcClientRequestId != null) {
            request += "opc-client-request-id: " + opcClientRequestId + "\n";
        }

        request += finalContent;

        return request;
    }

}

