package com.oracle.athena.webserver.http.parser;

import com.oracle.athena.webserver.connectionstate.CasperHttpInfo;
import org.eclipse.jetty.http.HttpParser;

import java.nio.ByteBuffer;

public class ByteBufferHttpParser {

    private HttpParser httpParser;

    public ByteBufferHttpParser(final CasperHttpInfo httpHeaderInfo) {
        HttpParserListener listener = new HttpParserListener(httpHeaderInfo);
        httpParser = new HttpParser(listener);
    }

    public void parseHttpData(ByteBuffer buffer, boolean initiallBuffer) {

        if (initiallBuffer) {
            if (httpParser.isState(HttpParser.State.END))
                httpParser.reset();
            if (!httpParser.isState(HttpParser.State.START))
                throw new IllegalStateException("!START");
        }

        // continue parsing
        int remaining = buffer.remaining();
        while (!httpParser.isState(HttpParser.State.END) && remaining > 0) {
            int was_remaining = remaining;
            httpParser.parseNext(buffer);
            remaining = buffer.remaining();
            if (remaining == was_remaining)
                break;
        }
    }

    public void resetHttpParser() {
        httpParser.reset();
    }
}
