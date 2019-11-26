package com.oracle.pic.casper.webserver.api.v2;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.v2.TracingConfiguration;
import com.oracle.pic.casper.common.guice.TracingModule;
import com.oracle.pic.casper.common.host.StaticHostInfoProvider;
import com.oracle.pic.casper.common.host.impl.DefaultStaticHostInfoProvider;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.rest.CommonHeaders;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import com.uber.jaeger.Tracer;
import io.opentracing.Span;
import org.glassfish.jersey.server.ContainerRequest;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    private static final Pattern PAR_MATCHER_REGEX = Pattern.compile("^/p/([^/]+)/n/.*");
    private static final String REQUEST_ID_KEY = "opcRequestId";
    public static final String EXTENDED_USER_DATA_KEY_PREFIX = "@";
    public static final Map<String, String> PRESERVED_CONTENT_HEADERS;
    public static final Map<String, String> PRESERVED_CONTENT_HEADERS_LOOKUP;
    private static final String TEST_AUTH_KEY = "testAuth";
    private static final String SKIP_AUTH_KEY = "skipAuth";

    static {
        PRESERVED_CONTENT_HEADERS_LOOKUP
                = ImmutableMap.<String, String>builder()
                .put(EXTENDED_USER_DATA_KEY_PREFIX + "^T", HttpHeaders.CONTENT_TYPE.toString())
                .put(EXTENDED_USER_DATA_KEY_PREFIX + "^L", HttpHeaders.CONTENT_LANGUAGE.toString())
                .put(EXTENDED_USER_DATA_KEY_PREFIX + "^C", HttpHeaders.CACHE_CONTROL.toString())
                .put(EXTENDED_USER_DATA_KEY_PREFIX + "^D", "Content-Disposition")
                .put(EXTENDED_USER_DATA_KEY_PREFIX + "^E", HttpHeaders.CONTENT_ENCODING.toString())
                .build();

        final ImmutableMap.Builder builder = ImmutableMap.<String, String>builder();
        for (Map.Entry<String, String> entry : PRESERVED_CONTENT_HEADERS_LOOKUP.entrySet()) {
            builder.put(entry.getValue().toLowerCase(Locale.ENGLISH), entry.getKey());
        }
        PRESERVED_CONTENT_HEADERS = builder.build();
    }

    public static MetricScope getMetricScope(String method, String realIp, String requestURI, TracingConfiguration tracingConfiguration, String opcRequestId) {
        final StaticHostInfoProvider staticHostInfoProvider = new DefaultStaticHostInfoProvider("web-server", "docker");

        Tracer tracer = new TracingModule().tracer(tracingConfiguration,
                staticHostInfoProvider);
        Span span = tracer.buildSpan(method)
                .ignoreActiveSpan() //this is super important since we are not thread-per-request
                .startManual();

        return MetricScope.create(method, span, tracer)
                .annotate("method", method)
                .annotate("path", casperSanitize(getPathAndQuery(requestURI)))
                // TODO : get client host and port
                .annotate("client", "unknown" + ":" + 7000)
                .annotate("clientIp", "unknown")
                .annotate("realClientIp", realIp)
                ///Add the opc-request-id here as an annotation to the metric scope
                .baggage(REQUEST_ID_KEY, opcRequestId);

    }

    public static <T> T runWithScope(MetricScope outerScope, Supplier<T> supplier, String innerScopeName) {
        MetricScope innerScope = outerScope.child(innerScopeName);
        try {
            return supplier.get();
        } catch (Throwable t) {
            innerScope.fail(t);
            outerScope.fail(t);
            throw t;
        } finally {
            innerScope.end();
            outerScope.end();
        }
    }

    private static String casperSanitize(String path) {
        Matcher parMatcher = PAR_MATCHER_REGEX.matcher(path);
        if (parMatcher.matches()) {
            // Trim off the leading "/p/"
            final String trimmed = path.substring(3);
            // Replace up to the next "/" with "[SANITIZED]"
            final String sanitized = trimmed.replaceFirst("^[^/]+", "[SANITIZED]");
            // Prefix the sanitized suffix with the "/p/" prefix and return
            return path.substring(0, 3) + sanitized;
        }
        return path;
    }

    private static String getPathAndQuery(@Nullable String url) {
        if (url == null) {
            return "";
        }

        try {
            URI uri = new URI(url);
            return Optional.ofNullable(uri.getPath()).orElse("") +
                    (Strings.isNullOrEmpty(uri.getQuery()) ? "" : "?" + uri.getQuery());
        } catch (URISyntaxException e) {
            return "";
        }
    }

    /**
     * We use pass-through implementations for authentication and authorization if any of these conditions are true:
     * - The web server flavor is INTEGRATION_TESTS and -DtestAuth was not passed.
     * - The -DskipAuth argument was passed.
     * - The region is LOCAL and -DtestAuth was not passed.
     */
    public static boolean skipAuth(WebServerFlavor flavor, ConfigRegion region) {
        return (flavor == WebServerFlavor.INTEGRATION_TESTS && !Boolean.getBoolean(TEST_AUTH_KEY)) ||
                Boolean.getBoolean(SKIP_AUTH_KEY) ||
                (region == ConfigRegion.LOCAL && !Boolean.getBoolean(TEST_AUTH_KEY));
    }

}