package com.webutils.webserver.http.parser;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpResponseCallback;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.http.HttpResponseListener;
import org.eclipse.jetty.http.HttpParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ResponseHttpParser {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseHttpParser.class);

    private final HttpParser httpParser;
    private final HttpResponseInfo httpInfo;

    public ResponseHttpParser(final HttpResponseInfo httpInfo, final HttpResponseCallback msgCompCb) {
        this.httpInfo = httpInfo;

        HttpResponseListener listener = new HttpResponseListener(httpInfo, msgCompCb);
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
    public boolean parseHttpData(final ByteBuffer buffer, final boolean initialBuffer) {

        if (initialBuffer) {
            if (httpParser.isState(HttpParser.State.END))
                httpParser.reset();
            if (!httpParser.isState(HttpParser.State.START))
                throw new IllegalStateException("!START");
        }

        StringChunk chunk = new StringChunk(buffer);
        ByteBuffer bufferToParse;
        boolean remainingBuffer = false;

        while ((bufferToParse = chunk.getBuffer()) != null) {
            int remaining = bufferToParse.remaining();

            //String tmpStr = HttpInfo.bb_to_str(bufferToParse);
            //LOG.info("ResponseHttpParser parseHttpData() " + tmpStr);

            while (!httpParser.isState(HttpParser.State.END) && (remaining > 0)) {
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
            if (httpInfo.getHeaderComplete()) {
                LOG.info("parseHttpData() headerComplete");
                remainingBuffer = chunk.isThereRemainingData();
                break;
            }
        }

        return remainingBuffer;
    }


    public void resetHttpParser() {
        httpParser.reset();
    }

}
