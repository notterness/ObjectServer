package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.server.ClientDataReadCallback;
import org.eclipse.jetty.http.HttpParser;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TestClientReadCallback extends ClientDataReadCallback {

    private HttpParser httpParser;

    public TestClientReadCallback(final HttpParser parser) {

        httpParser = parser;
    }

    public void dataBufferRead(final int result, final ByteBuffer readBuffer) {

        displayBuffer(readBuffer);

        readBuffer.flip();
        System.out.println("buffer " + readBuffer.position() + " " + readBuffer.limit());

        // continue parsing
        if (httpParser.isState(HttpParser.State.END))
            httpParser.reset();
        if (!httpParser.isState(HttpParser.State.START))
            throw new IllegalStateException("!START");

        int remaining = readBuffer.remaining();
        while (!httpParser.isState(HttpParser.State.END) && remaining > 0) {
            int was_remaining = remaining;
            httpParser.parseNext(readBuffer);

            System.out.println("buffer " + readBuffer.position() + " " + readBuffer.limit());

            remaining = readBuffer.remaining();
            if (remaining == was_remaining)
                break;
        }

    }

    private void displayBuffer(final ByteBuffer buffer) {
        System.out.println("buffer " + buffer.position() + " " + buffer.limit());

        String tmp = bb_to_str(buffer);

        System.out.println("TestClientReadCallback: client buffer " + tmp);
    }

    String bb_to_str(ByteBuffer buffer) {
        buffer.flip();
         /*
        CharBuffer cbuf = buffer.asCharBuffer();
        String tmp = cbuf.toString();
        */

        return StandardCharsets.UTF_8.decode(buffer).toString();
    }

}
