package com.oracle.pic.casper.webserver.api.s3.util;

import javax.annotation.Nullable;
import java.util.HashMap;

/**
 * Util class to convert unprintable characters (ASCII control characters 00–1F hex and 7F) to HTML entities
 * Characters not included:
 * \n (\u000A)- newline (not supported during PUT)
 * \t (\u0009) - horizontal tab (printable)
 * \r (\u000D) - carriage return (not supported during PUT)
 * \u007F - handled by the Xml mapper
 */
public final class S3XmlResponseUtil {

    private static final HashMap<Character, String> HTML_ENCODED_ENTITIES = new HashMap<>();
    static {
        HTML_ENCODED_ENTITIES.put('\u0001', "&#x1;");
        HTML_ENCODED_ENTITIES.put('\u0002', "&#x2;");
        HTML_ENCODED_ENTITIES.put('\u0003', "&#x3;");
        HTML_ENCODED_ENTITIES.put('\u0004', "&#x4;");
        HTML_ENCODED_ENTITIES.put('\u0005', "&#x5;");
        HTML_ENCODED_ENTITIES.put('\u0006', "&#x6;");
        HTML_ENCODED_ENTITIES.put('\u0007', "&#x7;");
        HTML_ENCODED_ENTITIES.put('\u0008', "&#x8;");
        HTML_ENCODED_ENTITIES.put('\u0010', "&#x10;");
        HTML_ENCODED_ENTITIES.put('\u000B', "&#xb;");
        HTML_ENCODED_ENTITIES.put('\u000C', "&#xc;");
        HTML_ENCODED_ENTITIES.put('\u000E', "&#xe;");
        HTML_ENCODED_ENTITIES.put('\u000F', "&#xf;");
        HTML_ENCODED_ENTITIES.put('\u0010', "&#x10;");

        HTML_ENCODED_ENTITIES.put('\u0011', "&#x11;");
        HTML_ENCODED_ENTITIES.put('\u0012', "&#x12;");
        HTML_ENCODED_ENTITIES.put('\u0013', "&#x13;");
        HTML_ENCODED_ENTITIES.put('\u0014', "&#x14;");
        HTML_ENCODED_ENTITIES.put('\u0015', "&#x15;");
        HTML_ENCODED_ENTITIES.put('\u0016', "&#x16;");
        HTML_ENCODED_ENTITIES.put('\u0017', "&#x17;");
        HTML_ENCODED_ENTITIES.put('\u0018', "&#x18;");
        HTML_ENCODED_ENTITIES.put('\u0019', "&#x19;");
        HTML_ENCODED_ENTITIES.put('\u001A', "&#x1a;");
        HTML_ENCODED_ENTITIES.put('\u001B', "&#x1b;");
        HTML_ENCODED_ENTITIES.put('\u001C', "&#x1c;");
        HTML_ENCODED_ENTITIES.put('\u001D', "&#x1d;");
        HTML_ENCODED_ENTITIES.put('\u001E', "&#x1e;");
        HTML_ENCODED_ENTITIES.put('\u001F', "&#x1f;");
    }

    private S3XmlResponseUtil() {

    }

    public static String sanitizeForXml(@Nullable String source) {
        if (source == null) return null;

        StringBuffer stringBuffer = new StringBuffer();
        source.chars().forEach(ch -> {
            char sourceChar = (char) ch;
            if (HTML_ENCODED_ENTITIES.containsKey(sourceChar)) {
                stringBuffer.append(HTML_ENCODED_ENTITIES.get(sourceChar));
            } else stringBuffer.append(sourceChar);
        });
        return stringBuffer.toString();
    }

 }
