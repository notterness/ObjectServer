package com.oracle.athena.webserver.http.parser;

import com.oracle.athena.webserver.connectionstate.CasperHttpInfo;
import org.eclipse.jetty.http.HttpParser;

import java.nio.ByteBuffer;

public class ByteBufferHttpParser {

    private HttpParser httpParser;
    private CasperHttpInfo casperHeaderInfo;

    public ByteBufferHttpParser(final CasperHttpInfo httpHeaderInfo) {
        casperHeaderInfo = httpHeaderInfo;

        HttpParserListener listener = new HttpParserListener(httpHeaderInfo);
        httpParser = new HttpParser(listener);
    }

    /*
    ** This is the method that does the actual feeding of the buffers into the Jetty HTTP Parser.
    **   The ByteBuffer is broken up in pieces delineated by CR/LF to allow determination of when
    **   the headers have actually been all parsed. This is due to the fact that a ByteBuffer may
    **   contain information for both the headers and the content and the processing of the
    **   content is handled differently.
    **   The content data is not feed through the HTTP Parser to avoid copies.
     */
    public ByteBuffer parseHttpData(ByteBuffer buffer, boolean initiallBuffer) {

        if (initiallBuffer) {
            if (httpParser.isState(HttpParser.State.END))
                httpParser.reset();
            if (!httpParser.isState(HttpParser.State.START))
                throw new IllegalStateException("!START");
        }

        StringChunk chunk = new StringChunk(buffer);
        ByteBuffer bufferToParse;
        ByteBuffer remainingBuffer = null;

        while ((bufferToParse = chunk.getBuffer()) != null) {
            int remaining = bufferToParse.remaining();

            while (!httpParser.isState(HttpParser.State.END) && remaining > 0) {
                int was_remaining = remaining;
                httpParser.parseNext(bufferToParse);
                remaining = bufferToParse.remaining();
                if (remaining == was_remaining)
                    break;
            }

            /*
            ** Check if the header has been parsed. If so, grab the remaining bytes from
            **   the passed in buffer and return it.
             */
            if (casperHeaderInfo.getHeaderComplete() == true) {
                System.out.println("parseHttpData() headerComplete");
                remainingBuffer = chunk.getRemainingBuffer();
                break;
            }
        }

        return remainingBuffer;
    }


    public void resetHttpParser() {
        httpParser.reset();
    }
}
