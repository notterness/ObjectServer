package com.webutils.webserver.http.parser;

/*
** This class is used to parse the content passed in through the POST REST method. An example of the content looks like
**  the following for the CreateBucket:
**
**   {
**       "compartmentId": "clienttest.compartment.12345.abcde",
**       "namespace": "testnamespace",
**       "name": "CreateBucket_Simple",
**       "objectEventsEnabled": false,
**       "freeformTags": {"Test_1": "Test_2"},
**       "definedTags":
**       {
**           "MyTags":
**           {
**               "TestTag_1": "ABC",
**               "TestTag_2": "123",
**           }
**           "Audit":
**           {
**               "DataSensitivity": "PII",
**               "CageSecurity": "High",
**               "Simplicity": "complex"
**           }
**       }
**   }
 */

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.ParseRequestContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.StringTokenizer;

public class PostContentParser {

    private static final Logger LOG = LoggerFactory.getLogger(PostContentParser.class);

    private final int contentLength;
    private final ParseRequestContent parseRequestContent;

    private int contentBytesParsed;

    private boolean parseError;

    public PostContentParser(final int contentLength, final ParseRequestContent parseRequestContent) {

        this.contentLength = contentLength;
        this.parseRequestContent = parseRequestContent;

        this.contentBytesParsed = 0;
        this.parseError = false;
    }

    public boolean parseBuffer(final ByteBuffer buffer) {

        StringChunk chunk = new StringChunk(buffer);
        ByteBuffer bufferToParse;

        while ((bufferToParse = chunk.getBuffer()) != null) {
            contentBytesParsed += bufferToParse.remaining();

            /*
            ** Convert to a string. Then tokenize if using space, tab, NewLine, CR, \f and Double Quote.
            */
            String tmpStr = HttpInfo.bb_to_str(bufferToParse);
            StringTokenizer stk = new StringTokenizer(tmpStr, " \t\n\r\f\"");
            while (stk.hasMoreTokens()) {
                String str1 = stk.nextToken();

                /*
                ** Uncomment out the following line to look at what the string is that has been parsed out by the
                **   StringTokenizer().
                 */
                //LOG.info(" token: " + str1);

                if (!parseRequestContent.addData(str1)) {
                    parseError = true;
                    return false;
                }
            }
        }

        return true;
    }

    public boolean allContentParsed() {
        return ((contentLength == contentBytesParsed) || parseError);
    }

    public boolean getParseError() {
        return parseError;
    }
}
