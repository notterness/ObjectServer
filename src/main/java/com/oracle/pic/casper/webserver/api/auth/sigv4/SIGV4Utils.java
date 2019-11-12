package com.oracle.pic.casper.webserver.api.auth.sigv4;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.oracle.pic.casper.common.exceptions.AccessDeniedException;
import com.oracle.pic.casper.common.exceptions.AuthorizationHeaderMalformedException;
import com.oracle.pic.casper.common.exceptions.AuthorizationQueryParametersException;
import com.oracle.pic.casper.common.util.DateUtil;

import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

public final class SIGV4Utils {

    public static final String X_AMZ_DATE = "X-Amz-Date";

    private static final long MAX_VALID_SECONDS = 604800L;

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssX");

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * These headers must be signed when header authorization. Also, one of
     * Date or X-Amz-Date must be signed. Although AWS documentation implies
     * that x-amz-content-sha256 must also be signed, its behavior indicates
     * that this is not required.
     */
    public static final Set<String> REQUIRED_HEADERS = ImmutableSet.of("host");
    /**
     * A request will be rejected if its time (Date/X-Amz-Date) differs from
     * the system clock by more than this amount.
     *
     * We use 5 minutes to match the standard Oracle BMCS signing.
     */
    public static final Duration MAX_REQUEST_AGE = Duration.ofMinutes(5);
    /**
     * These headers must be signed when using query string authorization.
     */
    public static final Set<String> REQUIRED_HEADERS_QUERY_STRING = ImmutableSet.of("host");
    /**
     * Used with presigned URLs.
     */
    public static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

    private static final String DATE = "date";

    public static final String US_EAST_REGION_STRING = "us-east-1";


    private SIGV4Utils() {
    }

    //-------------------------------------------------- headers -------------------------------------------------------
    /**
     * Get the x-amz-date or date header from the request and parse it. We prefer x-amz-date.
     */
    public static Instant getRequestTimeFromHeaders(Map<String, List<String>> headers, List<String> signedHeaders) {
        String dateHeader = X_AMZ_DATE;
        String requestTimeString = getHeader(headers, dateHeader);
        if (requestTimeString == null) {
            dateHeader = DATE;
            requestTimeString = getHeader(headers, dateHeader);
        }

        // No date at all == error
        if (requestTimeString == null) {
            final String error = String.format("%s or %s is required", X_AMZ_DATE, DATE);
            throw new AccessDeniedException(error);
        }

        // The date we are using must be signed
        verifyHeaderIsSigned(dateHeader, signedHeaders);

        // Parse the time
        final Instant requestTime;
        try {
            requestTime = parseHeaderTime(requestTimeString);
        } catch (DateTimeParseException e) {
            final String error = String.format("Illegal date format %s", requestTimeString);
            throw new AccessDeniedException(error);
        }
        return requestTime;
    }

    /**
     * Verify all the headers are signed.
     */
    public static void verifyHeadersAreSigned(Iterable<String> headers, List<String> signedHeaders) {
        for (String h : headers) {
            verifyHeaderIsSigned(h, signedHeaders);
        }
    }

    /**
     * Throw an exception if a specific header isn't signed.
     */
    private static void verifyHeaderIsSigned(String header, List<String> signedHeaders) {
        if (!signedHeaders.contains(header.toLowerCase())) {
            final String error = String.format("Header %s must be signed", header);
            throw new AccessDeniedException(error);
        }
    }

    public static String getHeader(Map<String, List<String>> headers, String header) {
        String authorizationHeader = null;
        for (Map.Entry<String, List<String>> e : headers.entrySet()) {
            if (header.equalsIgnoreCase(e.getKey())) {
                if (e.getValue().size() != 1) {
                    final String error = String.format("Invalid %s header", header);
                    throw new AuthorizationHeaderMalformedException(error);
                }
                authorizationHeader = e.getValue().get(0);
            }
        }
        return authorizationHeader;
    }

    /**
     * Select only headers that are in the signed headers list.
     * @param headers The request headers. These headers can have MiXeD CaSE keys
     * @param signedHeaders A list of signed headers. This will all be lowercase.
     */
    public static Map<String, List<String>> filterHeaders(
            Map<String, List<String>> headers,
            List<String> signedHeaders) {
        final Map<String, List<String>> filtered = new HashMap<>();
        // Currently O(N^2). Could be optimized.
        for (Map.Entry<String, List<String>> header : headers.entrySet()) {
            for (String signedHeader : signedHeaders) {
                if (signedHeader.equalsIgnoreCase(header.getKey())) {
                    filtered.put(signedHeader, header.getValue());
                } else if (signedHeader.equals("expect") && header.getKey().equalsIgnoreCase("x-expect")) {
                    // CASPER-1232
                    //
                    // Flamingo strips out 'expect' headers (all the '100 CONTINUE' work is
                    // done by the LB). But what happens if 'expect' is a signed header? If
                    // the header is stripped SIGV4 validation will fail! To work around this
                    // issue Flamingo will forward the 'expect' header as 'X-Expect'.
                    filtered.put(signedHeader, header.getValue());
                }
            }
        }
        return filtered;
    }

    /**
     * Normalize a collection of headers. This means:
     *  1) Converting keys to lowercase.
     *  2) Trimming and concatenating the values
     *  3) Putting the keys into sorted order.
     * @return A sorted map.
     */
    public static SortedMap<String, String> normalizeHeaders(Map<String, List<String>> headers) {
        final ImmutableSortedMap.Builder<String, String> builder = ImmutableSortedMap.naturalOrder();
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            final String k = entry.getKey().toLowerCase();
            final String v = String.join(",", entry.getValue().stream().map(String::trim).collect(Collectors.toList()));
            builder.put(k, v);
        }
        return builder.build();
    }

    /**
     * Format a list of headers with semi-colons between. The list of headers should
     * already be sorted.
     */
    static String headerList(Iterable<String> headers) {
        return String.join(";", headers);
    }

    /**
     * Parse a list of semicolon-separated headers.
     */
    static List<String> parseHeaderList(String headers) {
        return Arrays.asList(headers.split(";"));
    }

    //---------------------------------------------- canonical request -------------------------------------------------
    /**
     * Generate the canonical representation of a request.
     *
     * @param httpMethod The HTTP action being performed (e.g. "GET")
     * @param path The uri path being retrieved (e.g. "/myobject.txt"); this uri should be encoded.
     * @param queryParameters Parameters for the query string
     * @param normalizedHeaders The signed headers. These should be normalized already.
     * @param bodySha256 The hash of the request body. If the request doesn't have
     *                   a body then this should be the hash of an empty string.
     *                   Note that this is the hash, NOT the actual body.
     */
    public static String canonicalRequest(
            String httpMethod,
            String path,
            Map<String, String> queryParameters,
            Map<String, String> normalizedHeaders,
            String bodySha256) {
        final List<String> lines = new ArrayList<>();
        lines.add(httpMethod);
        lines.add(uriEncodePath(path));
        lines.add(canonicalQueryString(queryParameters));
        for (Map.Entry<String, String> entry : normalizedHeaders.entrySet()) {
            lines.add(String.format("%s:%s", entry.getKey(), entry.getValue()));
        }
        lines.add("");
        lines.add(headerList(normalizedHeaders.keySet()));
        lines.add(bodySha256);
        return String.join("\n", lines);
    }

    /**
     * Generate the canonical version of a query string.
     */
    static String canonicalQueryString(Map<String, String> queryParameters) {
        // We need the parameters in sorted order
        final List<String> keys = new ArrayList<>(queryParameters.keySet());
        Collections.sort(keys);

        final List<String> segments = new ArrayList<>();
        for (String k : keys) {
            segments.add(String.format("%s=%s", k, uriEncode(queryParameters.get(k))));
        }
        return String.join("&", segments);
    }

    /**
     * Amazon specifies that the URI used to form canonical request in signature calculation needs to be URI-encoded.
     * To be consistent with Amazon, we need to handle both when the path in URI has been encoded (e.g. Java SDK) and
     * when it has not been encoded (e.g. Python requests library, C++ SDK). To do that, we always try to decode the
     * path first, and re-encode it according to SIGV4 specifications.
     */
    static String uriEncodePath(String path) {
        try {
            path = uriEncode(URLDecoder.decode(path, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new UncheckedIOException(e);
        }
        // We should not encode '/' in the path
        return path.replace("%2F", "/");
    }

    /**
     * URI encode a string for canonical request generation:
     *  - URI encode every byte except the unreserved characters: 'A'-'Z', 'a'-'z', '0'-'9', '-', '.', '_', and '~'.
     *  - The space character is a reserved character and must be encoded as "%20" (and not as "+").
     *  - Each URI encoded byte is formed by a '%' and the two-digit hexadecimal value of the byte.
     *  - Letters in the hexadecimal value must be uppercase, for example "%1A".
     *  - Encode the forward slash character, '/', everywhere except in the object key name. For example, if the object
     *    key name is photos/Jan/sample.jpg, the forward slash in the key name is not encoded. The latter case is
     *    handled by {@link #uriEncodePath(String)}
     *
     * This method is adapted from SdkHttpUtils.java in AWS S3 SDK - using {@link URLEncoder#encode(String, String)}
     * with post processing to conform to RFC 3986.
     */
    @VisibleForTesting
    static String uriEncode(String input) {
        try {
            final String output = URLEncoder.encode(input, "UTF-8");
            return output.replace("+", "%20").replace("*", "%2A").replace("%7E", "~");
        } catch (UnsupportedEncodingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Format bytes as lowercase hex.
     */
    public static String hex(byte[] data) {
        final StringBuilder sb = new StringBuilder(data.length * 2);
        for (byte b : data) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    //---------------------------------------- time parsing and formatting ---------------------------------------------
    /**
     * Format an instant as a time.
     */
    static String formatTime(Instant when) {
        return TIME_FORMATTER.withZone(ZoneOffset.UTC).format(when);
    }

    /**
     * Format an instant as a date.
     */
    public static String formatDate(Instant when) {
        return DATE_FORMATTER.withZone(ZoneOffset.UTC).format(when);
    }

    /**
     * Get an expired date calculated from presigned request
     */
    public static Instant getExpireTime(String date, String expire) {
        long expireSeconds = Long.parseLong(expire);
        if (expireSeconds < 0) {
            throw new AuthorizationQueryParametersException("X-Amz-Expires must be non-negative");
        }
        if (expireSeconds > MAX_VALID_SECONDS) {
            final String error = "X-Amz-Expires must be less than a week (in seconds); that is, the given " +
                    "X-Amz-Expires must be less than 604800 seconds";
            throw new AuthorizationQueryParametersException(error);
        }
        Instant initialTime = parseTime(date);
        return initialTime.plusSeconds(expireSeconds);
    }

    /**
     * Parse a time from an HTTP header. This accepts two formats:
     *  1) 20170227T052605Z
     *  2) Mon, 27 Feb 2017 05:36:30 GMT
     */
    public static Instant parseHeaderTime(String timeString) throws DateTimeParseException {
        // There are two ways the date can be formatted. Try them both
        Instant time;
        try {
            time = parseTime(timeString);
        } catch (DateTimeParseException e) {
            time = DateUtil.httpFormattedStringToInstant(timeString);
        }
        return time;
    }

    /**
     * Given a string, parse a time. This accepts the 20170227T052605Z format.
     */
    static Instant parseTime(String time) throws DateTimeParseException {
        return Instant.from(LocalDateTime.parse(time, TIME_FORMATTER).toInstant(ZoneOffset.UTC));
    }

    /**
     * Parse a date in the 20170225 format.
     */
    static Instant parseDate(String date) {
        return Instant.from(LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyyMMdd"))
            .atStartOfDay(ZoneOffset.UTC));
    }
}
