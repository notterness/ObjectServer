package com.oracle.athena.webserver.http;

import com.oracle.athena.webserver.connectionstate.BufferState;
import org.eclipse.jetty.http.HttpStatus;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public class BuildHttpResult {

    private HttpStatus httpStatus;

    public BuildHttpResult() {
        httpStatus = new HttpStatus();
    }

    public ByteBuffer buildResponse(final BufferState respBufferState, int resultCode, boolean addContext, boolean close) {

        ByteBuffer respBuffer = respBufferState.getBuffer();
        String tmpStr;
        String content;
        int contentLength;

        HttpStatus.Code result = HttpStatus.getCode(resultCode);
        if (result != null) {
            if (addContext) {
                StringBuilder builtContent = new StringBuilder()
                        .append("\r\n")
                        .append("{\r\n")
                        .append("  \"Description\":\"")
                        .append(result.getMessage())
                        .append("\"\r\n}");
                content = builtContent.toString();

                // Assuming that the last two pairs of CR/LF do not count towards the content length
                contentLength = content.length();
            } else {
                contentLength = 0;
                content = "";
            }

            StringBuilder tmpBuiltStr;
            if (resultCode != HttpStatus.CONTINUE_100) {
                tmpBuiltStr = new StringBuilder()
                        .append("HTTP/1.1 ")
                        .append(result.getCode())
                        .append("\r\n")
                        .append("Content-Length: ")
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
                        .append("Content-Length: ")
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
                StringBuilder tmpBuiltStr = new StringBuilder()
                        .append("HTTP/1.1 ")
                        .append(result.getCode())
                        .append("\r\n")
                        .append("Content-Length: 0\r\n")
                        .append("Connection: close\r\n")
                        .append("\r\n");
                tmpStr = tmpBuiltStr.toString();
            } else {
                // TODO: This is a problem and need to crash.
                tmpStr = null;
            }
        }

        str_to_bb(respBuffer, tmpStr);
        return respBuffer;
    }

    private void str_to_bb(ByteBuffer out, String in) {
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            boolean endOfInput = true;

            encoder.encode(CharBuffer.wrap(in), out, endOfInput);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
