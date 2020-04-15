package com.webutils.webserver.http.parser;

/*
** This class is used to parse the content passed in through the POST REST method. An example of the content looks like:
**
**  {
**    "compartmentId": "ocid.compartment.test.exampleuniquecompartmentID",
**    "namespace": "ansh8lvru1zp",
**    "objectEventsEnabled": true,
**    "name": "my-test-1",
**    "freeformTags": {"Department": "Finance"},
**    "definedTags":
**    {
**      "MyTags":
**      {
**        "CostCenter": "42",
**        "Project": "Stealth",
**        "CreatedBy": "BillSmith",
**        "CreatedDate": "9/21/2017T14:00"
**      },
**      "Audit":
**      {
**        "DataSensitivity": "PII",
**        "CageSecurity": "High",
**        "Simplicity": "complex"
**      }
**    }
**  }
 */

import com.webutils.webserver.http.PostContentData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.StringTokenizer;

public class PostContentParser {
    private static final Logger LOG = LoggerFactory.getLogger(PostContentParser.class);

    private final int contentLength;
    private final PostContentData postContentData;

    private int contentBytesParsed;

    private boolean parseError;

    public PostContentParser(final int contentLength, final PostContentData postContentData) {

        this.contentLength = contentLength;
        this.postContentData = postContentData;

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
            String tmpStr = chunk.bb_to_str(bufferToParse);
            StringTokenizer stk = new StringTokenizer(tmpStr, " \t\n\r\f\"");
            while (stk.hasMoreTokens()) {
                String str1 = stk.nextToken();

                /*
                ** Uncomment out the following line to look at what the string is that has been parsed out by the
                **   StringTokenizer().
                 */
                //LOG.info(" token: " + str1);

                if (!postContentData.addData(str1)) {
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
