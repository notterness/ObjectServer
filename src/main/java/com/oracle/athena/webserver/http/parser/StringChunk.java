package com.oracle.athena.webserver.http.parser;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringChunk {
    private ByteBuffer initialBuffer;

    private int currentPosition;
    private int remaining;

    StringChunk(ByteBuffer buffer) {
        initialBuffer = buffer;

        currentPosition = buffer.position();
        remaining = buffer.remaining();
    }

    /*
    ** This walks the buffer to find the first occurrence of CR LF and returns a
    **   ByteBuffer that includes that.
     */
    ByteBuffer getBuffer() {
        byte ch;

        /*
        ** End of buffer checking
         */
        if (currentPosition == remaining) {
            return null;
        }
        //displayChar(initialBuffer);

        int i;
        int charCount = 0;
        for (i = currentPosition; i < remaining; i++) {
            try {
                ch = initialBuffer.get(i);
                charCount++;
            } catch (IndexOutOfBoundsException ex) {
                System.out.println("StringChunk i: " + i);
                break;
            }

            if ((ch == 13) || (ch == 10)) {
                /*
                ** Check if the following character is something besides a CR or LF
                 */
                boolean done = false;
                i++;
                while (i < remaining) {
                    byte ch_next = initialBuffer.get(i);
                    if ((ch_next == 13) || (ch_next == 10)) {
                        i++;
                        charCount++;
                    } else {
                        break;
                    }
                }
                break;
            }
        }

        /* Return a ByteBuffer that is a copy */
        ByteBuffer bb = initialBuffer.slice();
        bb.limit(charCount);

        //String str = bb_to_str(bb);
        //System.out.println("StringChunk: " + str);

        currentPosition = i;
        initialBuffer.position(i);

        return bb;
    }

    ByteBuffer getRemainingBuffer() {
        /*
         ** End of buffer checking
         */
        if (currentPosition == remaining) {
            System.out.println("StringChunk getRemainingBuffer() null");
            return null;
        }

        ByteBuffer bb = initialBuffer.slice();

        System.out.println("StringChunk position: " + bb.position() + " limit: " + bb.limit() +
                " remaining: " + bb.remaining());

        return bb;
    }

    private String bb_to_str(ByteBuffer buffer) {
        int position = buffer.position();
        String tmp = StandardCharsets.UTF_8.decode(buffer).toString();

        buffer.position(position);
        return tmp;
    }

    private void displayChar(ByteBuffer buffer) {
        int position = buffer.position();
        int limit = buffer.limit();

        byte b;
        String tmp = "";

        int i = position;
        int count = 0;
        while (i < limit) {
            b = buffer.get(i);

            tmp = tmp + " " + b;

            i++;
            count++;
            if (count == 16)
                break;
        }

        System.out.println(tmp);
    }

}
