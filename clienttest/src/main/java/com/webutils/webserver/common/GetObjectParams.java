package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpResponseInfo;
import org.eclipse.jetty.http.HttpStatus;

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

    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good, then display the following:
         **   opc-client-request-id
         **   opc-request-id
         **   ETag
         **   content-length
         **   content-md5
         **   last-modified
         **   version-id
         */
        if (httpInfo.getResponseStatus() == HttpStatus.OK_200) {
            System.out.println("Status: 200");
            String opcClientRequestId = httpInfo.getOpcClientRequestId();
            if (opcClientRequestId != null) {
                System.out.println("opc-clent-request-id: " + opcClientRequestId);
            }

            String opcRequestId = httpInfo.getOpcRequestId();
            if (opcRequestId != null) {
                System.out.println("opc-request-id: " + opcRequestId);
            }

            String etag = httpInfo.getResponseEtag();
            if (etag != null) {
                System.out.println("ETag: " + etag);
            }

            int contentLength = httpInfo.getContentLength();
            System.out.println("content-length: " + contentLength);

            String contentMd5 = httpInfo.getResponseContentMd5();
            if (contentMd5 != null) {
                System.out.println("opc-content-md5: " + contentMd5);
            }

            String lastModified = httpInfo.getResponseLastModified();
            if (lastModified != null) {
                System.out.println("last-modified: " + lastModified);
            }

            String versionId = httpInfo.getResponseVersionId();
            if (versionId != null) {
                System.out.println("version-id: " + versionId);
            }
        } else {
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

}

