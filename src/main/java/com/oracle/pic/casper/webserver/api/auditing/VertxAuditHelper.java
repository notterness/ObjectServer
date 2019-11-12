package com.oracle.pic.casper.webserver.api.auditing;

import com.oracle.pic.identity.authentication.Principal;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import org.apache.http.HttpHeaders;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Static helper methods for parsing vertx request and response fields for audit
 */
public final class VertxAuditHelper {

    private static final String V2_SIGNATURE_SCHEME_PREFIX = "Signature ";
    private static final String KEY_ID_PREFIX = "keyId=";
    private static final String SIGNATURE_MASK = "*****";
    private static final String BASIC_AUTH_HEADER_PREFIX = "Basic ";
    private static final String S3_AUTH_HEADER_PREFIX = "AWS4-HMAC-SHA256";
    private static final String S3_AUTH_PARAM_KEY = "X-Amz-Signature";

    private static final Pattern PAR_MATCHER_REGEX = Pattern.compile("^/p/([^/]+)/n/.*");
    private static final String PAR_PARAM_REPLACEMENT = "[SANITIZED]";

    private VertxAuditHelper() {
    }

    public static Map<String, String[]> getRequestParameters(HttpServerRequest request) {
        final Map<String, String[]> parameters = new HashMap<>();

        // If the request is an access via a PAR, extract the nonce
        String parNonce = null;
        Matcher parMatcher = PAR_MATCHER_REGEX.matcher(request.uri());
        if (parMatcher.find()) {
            parNonce = request.uri().split("/p/")[1].split("/n/")[0];
        }
        final MultiMap requestParam = request.params();
        for (String key : requestParam.names()) {
            if (key.equalsIgnoreCase(S3_AUTH_PARAM_KEY)) {
                parameters.put(key, new String[]{SIGNATURE_MASK});
            } else {
                final List<String> values = requestParam.getAll(key);
                if (parNonce != null && values.contains(parNonce)) {
                    values.set(values.indexOf(parNonce), PAR_PARAM_REPLACEMENT);
                }
                parameters.put(key, values.toArray(new String[values.size()]));
            }
        }
        return parameters;
    }

    public static Map<String, String[]> getRequestHeaders(HttpServerRequest request) {
        final Map<String, String[]> headers = new HashMap<>();
        final MultiMap requestHeaders = request.headers();
        for (String key : requestHeaders.names()) {
            List<String> values = requestHeaders.getAll(key);
            String[] valuesArray = values.toArray(new String[values.size()]);
            if (key.equalsIgnoreCase(HttpHeaders.AUTHORIZATION)) {
                maskSignature(valuesArray);
            } else if (key.equalsIgnoreCase(Principal.OPC_OBO_HEADER)) {
                maskAll(valuesArray);
            }
            headers.put(key, valuesArray);
        }
        return headers;
    }

    public static Map<String, String[]> getResponseHeaders(HttpServerResponse response) {
        final Map<String, String[]> headers = new HashMap<>();
        final MultiMap requestHeaders = response.headers();
        for (String key : requestHeaders.names()) {
            List<String> values = requestHeaders.getAll(key);
            headers.put(key, values.toArray(new String[values.size()]));
        }
        return headers;
    }

    public static String getCredentialId(HttpServerRequest request) {
        final String authHeader = request.getHeader(HttpHeaders.AUTHORIZATION);
        //this code is pulled from VertxServiceApiAuditEvent in sherlock-collector-vertx repo
        if (authHeader != null && authHeader.startsWith(V2_SIGNATURE_SCHEME_PREFIX)) {
            String[] params = authHeader.substring(V2_SIGNATURE_SCHEME_PREFIX.length()).split(",");

            for (int i = params.length - 1; i >= 0; i--) {
                String param = params[i].trim();

                if (param.startsWith(KEY_ID_PREFIX + "\"") && param.endsWith("\"")) {
                    return param.substring(KEY_ID_PREFIX.length() + 1, param.length() - 1);
                }
            }
        }
        return null;
    }

    private static void maskAll(String[] values) {
        for (int i = 0; i < values.length; i++) {
            values[i] = SIGNATURE_MASK;
        }
    }

    private static void maskSignature(String[] values) {
        for (int i = 0; i < values.length; i++) {
            String s = values[i];
            if (s.trim().toLowerCase(Locale.US).startsWith(V2_SIGNATURE_SCHEME_PREFIX.toLowerCase(Locale.US))) {
                values[i] = s.replaceAll("signature=\"[^,]+\"", "signature=\"" + SIGNATURE_MASK + "\"");
            } else if (s.trim().toLowerCase(Locale.US).startsWith(BASIC_AUTH_HEADER_PREFIX.toLowerCase(Locale.US))) {
                values[i] = "Basic " + SIGNATURE_MASK;
            } else if (s.trim().toLowerCase(Locale.US).startsWith(S3_AUTH_HEADER_PREFIX.toLowerCase(Locale.US))) {
                values[i] = s.replaceAll("Signature=[^,]+", "Signature=" + SIGNATURE_MASK);
            }
        }
    }
}
