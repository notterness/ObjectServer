package com.webutils.webserver.http;

import org.eclipse.jetty.http.HttpStatus;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public class BuildHttpResult {

    private final HttpRequestInfo httpRequestInfo;
    private final int opcRequestId;

    public BuildHttpResult(final HttpRequestInfo objectCreateInfo, final int opcRequestId) {

        this.httpRequestInfo = objectCreateInfo;
        this.opcRequestId = opcRequestId;
    }

    public void buildResponse(final ByteBuffer respBuffer, int resultCode, boolean addContext, boolean close) {

        String tmpStr;
        String content;
        int contentLength;

        HttpStatus.Code result = HttpStatus.getCode(resultCode);
        if (result != null) {
            /*
            ** Special Handling for METHOD_NOT_ALLOWED_$)% since it needs to respond with the supported
            **   methods
             */
            if (addContext && (resultCode != HttpStatus.METHOD_NOT_ALLOWED_405)) {
                String failureDescription = httpRequestInfo.getParseFailureReason();

                if (failureDescription != null) {
                    content = failureDescription;
                } else {
                    content = "{\r\n" +
                            "  \"Description\":\"" +
                            result.getMessage() +
                            "\"\r\n}";
                }

                // Assuming that the last two pairs of CR/LF do not count towards the content length
                contentLength = content.length();
            } else {
                contentLength = 0;
                content = "";
            }

            StringBuilder tmpBuiltStr;
            if (resultCode == HttpStatus.METHOD_NOT_ALLOWED_405) {
                tmpBuiltStr = new StringBuilder()
                        .append("HTTP/1.1 ")
                        .append(result.getCode())
                        .append(" Method Not Allowed\r\n")
                        .append("Content-Type: text/html\n")
                        .append("Allow: DELETE GET PUT POST\n")
                        .append("Connection: close\n")
                        .append(HttpInfo.CONTENT_LENGTH + ": 0\n\n");

            } else if (resultCode != HttpStatus.CONTINUE_100) {
                tmpBuiltStr = new StringBuilder()
                        .append("HTTP/1.1 ")
                        .append(result.getCode())
                        .append("\r\n");

                String clientOpcRequestId = httpRequestInfo.getOpcClientRequestId();
                if (clientOpcRequestId != null) {
                    tmpBuiltStr.append(HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + clientOpcRequestId + "\r\n");
                }

                tmpBuiltStr.append(HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId + "\r\n");

                tmpBuiltStr.append(HttpInfo.CONTENT_LENGTH + ": ")
                        .append(contentLength)
                        .append("\r\n");
                if (close) {
                    tmpBuiltStr
                        .append("Connection: close\r\n");
                }
                tmpBuiltStr
                        .append("\r\n")
                        .append(content);
            } else {
                tmpBuiltStr = new StringBuilder()
                        .append("HTTP/1.1 ")
                        .append(result.getCode())
                        .append("\r\n")
                        .append(HttpInfo.CONTENT_LENGTH + ": ")
                        .append(contentLength)
                        .append("\r\n")
                        .append(content);
            }
            tmpStr = tmpBuiltStr.toString();
        } else {
            /*
             ** For some reason the passed in code was not valid, need to respond with something
             */
            result = HttpStatus.getCode(HttpStatus.NON_AUTHORITATIVE_INFORMATION_203);
            if (result != null) {
                tmpStr = "HTTP/1.1 " +
                        result.getCode() +
                        "\r\n" +
                        HttpInfo.CONTENT_LENGTH + ": 0\n\n";
            } else {
                // TODO: This is a problem and need to crash.
                tmpStr = null;
            }
        }

        HttpInfo.str_to_bb(respBuffer, tmpStr);
    }

    public void buildPostOkResponse(final ByteBuffer respBuffer) {

        String respHeader = httpRequestInfo.getResponseHeaders();

        String tmpStr = "HTTP/1.1 200 OK" +
                "\r\n" +
                "Content-Type: text/html\n" +
                respHeader +
                HttpInfo.CONTENT_LENGTH + ": 0\n\n";

        HttpInfo.str_to_bb(respBuffer, tmpStr);
    }

    public void buildDeleteOkResponse(final ByteBuffer respBuffer) {

        String respHeader = httpRequestInfo.getResponseHeaders();

        String tmpStr = "HTTP/1.1 200 OK" +
                "\r\n" +
                "Content-Type: text/html\n" +
                respHeader +
                HttpInfo.CONTENT_LENGTH + ": 0\n\n";

        HttpInfo.str_to_bb(respBuffer, tmpStr);
    }


    /*
    ** The OK_200 PUT response wil return the following headers:
    *
    ** opc-client-request-id - Echoes back the value passed in the opc-client-request-id header, for use by clients
    **   when debugging.
    ** opc-request-id - Unique assigned identifier for the request. If you need to contact Oracle about a particular
    **    request, provide this request ID.
    ** opc-content-md5 - The base-64 encoded MD5 hash of the request body as computed by the server.
    ** ETag	- The entity tag (ETag) for the object. This is the UID created for the Object.
    ** last-modified - The time the object was modified, as described in RFC 2616.
    **
    ** NOTE: If the "Content-Length: 0" is replaced by "\n", then the HttpResponseListener() calls for contentComplete()
    **   and messageComplete() will not happen. The callback for the response being complete is done in the
    **   messageComplete() method. Need to figure out if the response is valid or has some other problem.
    **
    ** The responseHeaders for the PUT Object method are build in the ObjectTableMgr.buildSuccessHeader() method.
     */
    public void buildPutOkResponse(final ByteBuffer respBuffer) {
        String respHeader = httpRequestInfo.getResponseHeaders();

        String tmpStr = "HTTP/1.1 200 OK" +
                "\r\n" +
                respHeader +
                HttpInfo.CONTENT_LENGTH + ": 0\n\n";

        HttpInfo.str_to_bb(respBuffer, tmpStr);
    }

    public void buildListObjectsOkResponse(final ByteBuffer respBuffer) {
        String respHeader = httpRequestInfo.getResponseHeaders();
        String respContent = httpRequestInfo.getResponseContent();

        String tmpStr = "HTTP/1.1 200 OK" +
                "\r\n" +
                respHeader +
                HttpInfo.CONTENT_LENGTH + ": " + respContent.length()  + "\n\n" +
                respContent;

        HttpInfo.str_to_bb(respBuffer, tmpStr);
    }

    public void buildAllocateChunksOkResponse(final ByteBuffer respBuffer) {
        String respHeader = httpRequestInfo.getResponseHeaders();
        String respContent = httpRequestInfo.getResponseContent();

        String tmpStr = "HTTP/1.1 200 OK" +
                "\r\n" +
                respHeader +
                HttpInfo.CONTENT_LENGTH + ": " + respContent.length()  + "\n\n" +
                respContent;

        HttpInfo.str_to_bb(respBuffer, tmpStr);
    }

    public void buildHealthCheckOkResponse(final ByteBuffer respBuffer) {
        String respHeader = httpRequestInfo.getResponseHeaders();

        String tmpStr = "HTTP/1.1 200 OK" +
                "\r\n" +
                respHeader +
                HttpInfo.CONTENT_LENGTH + ": 0\n\n";

        HttpInfo.str_to_bb(respBuffer, tmpStr);
    }


}
