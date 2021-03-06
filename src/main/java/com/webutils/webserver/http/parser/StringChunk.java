package com.webutils.webserver.http.parser;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.webutils.webserver.http.HttpInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringChunk {
    
    private static final Logger LOG = LoggerFactory.getLogger(StringChunk.class);

    private final ByteBuffer initialBuffer;

    private int currentPosition;
    private final int limit;

    StringChunk(final ByteBuffer buffer) {
        initialBuffer = buffer;

        currentPosition = buffer.position();
        limit = buffer.limit();
        
        //LOG.info(" currentPosition: " + currentPosition + " remaining: " + buffer.remaining() + " limit: " + buffer.limit());
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
        if (currentPosition == limit) {
            return null;
        }
        //displayChar(initialBuffer);

        int i;
        int charCount = 0;
        for (i = currentPosition; i < limit; i++) {
            try {
                ch = initialBuffer.get(i);
                charCount++;
            } catch (IndexOutOfBoundsException ex) {
                LOG.info("StringChunk i: " + i);
                break;
            }

            if ((ch == 13) || (ch == 10)) {
                /*
                ** Check if the following character is something besides a CR or LF
                 */
                i++;
                while (i < limit) {
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

        String str = HttpInfo.bb_to_str(bb);

        /*
        ** Uncomment out the following line to display the string that has been extracted from the
        **   ByteBuffer that is being processed.
         */
        //LOG.info("StringChunk: charCount: " + charCount + " - " + str);

        currentPosition = i;
        initialBuffer.position(i);

        return bb;
    }

    /*
    ** After the HTTP Parsing is done, this checks if there is more data remaining in the buffer
     */
    boolean isThereRemainingData() {
        /*
         ** End of buffer checking
         */
        if (currentPosition == limit) {
            //LOG.info("StringChunk getRemainingBuffer() null");
            return false;
        }

        //LOG.info("StringChunk position: " + bb.position() + " limit: " + bb.limit() +
        //        " remaining: " + bb.remaining());

        return true;
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

        LOG.info(tmp);
    }

}
