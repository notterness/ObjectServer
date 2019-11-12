package com.oracle.pic.casper.webserver.auth.dataplane;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Charsets;
import com.google.api.client.util.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.oracle.pic.casper.common.exceptions.AuthDataPlaneConnectionException;
import com.oracle.pic.casper.common.exceptions.AuthDataPlaneServerException;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.auth.dataplane.ServiceAuthenticator;
import com.oracle.pic.casper.webserver.auth.dataplane.model.AuthUserAndPass;
import com.oracle.pic.casper.webserver.auth.dataplane.model.AuthenticationPrincipal;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SigningKey;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SwiftAuthRequests;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SwiftCredentials;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SwiftUser;
import com.oracle.pic.identity.authentication.Principal;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * AuthDataPlaneClientApacheImpl is the Apache HTTP client implementation of the AuthDataPlaneClient interface.
 *
 * See the class docs for AuthDataPlaneClient for details on what this class does.
 */
public class AuthDataPlaneClientApacheImpl implements AuthDataPlaneClient {

    /**
     * The AuthDataPlaneClient is for the same service as the AuthenticatorClient, but that client only exposes four
     * parameters for tuning retry/backoff behavior: the maximum elapsed time (for all tries and retries), the socket
     * timeout (for both send/recv and connect), the initial backoff for retries and the multiplier for subsequent
     * retries. These settings are a balance to address two extreme cases: all requests fail at the limit of the timeout
     * or all requests fail immediately with a 5xx error. In the former case, we will do three retries, in the latter
     * case we will do seven retries with this configuration.
     */
    public static final Duration MAX_ELAPSED_TIME = Duration.ofSeconds(6);
    public static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(1);
    public static final Duration SOCKET_TIMEOUT = Duration.ofSeconds(4);
    public static final Duration INITIAL_BACKOFF = Duration.ofMillis(200);
    public static final double BACKOFF_MULT = 2;

    private final CloseableHttpClient client;
    private final HttpHost host;
    private final ObjectMapper mapper;

    private final ServiceAuthenticator serviceAuthenticator;

    /**
     * LimitConnectionKeepAliveStrategy limits the maximum time that an HTTP/1.1 keep-alive connection can be idle
     * before it is closed by the client.
     *
     * This implementation limits the keep-alive connection to the min of the time requested by the server and the max
     * time configured by the constructor.
     */
    public static class LimitConnectionKeepAliveStrategy implements ConnectionKeepAliveStrategy {

        private final long maxIdleTimeMillis;

        LimitConnectionKeepAliveStrategy(long maxIdleTimeMillis) {
            this.maxIdleTimeMillis = maxIdleTimeMillis;
        }

        @Override
        public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
            // obtain the default value if the server returns one
            final long duration = DefaultConnectionKeepAliveStrategy.INSTANCE.getKeepAliveDuration(response, context);
            if (0 < duration && duration < maxIdleTimeMillis) {
                return duration;
            }

            return maxIdleTimeMillis;
        }
    }

    public AuthDataPlaneClientApacheImpl(HttpHost host,
                                         ObjectMapper mapper,
                                         @Nullable ServiceAuthenticator serviceAuthenticator) {
        this.host = host;
        this.mapper = mapper;
        this.serviceAuthenticator = serviceAuthenticator;

        final RequestConfig reqConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(500)
                .setConnectTimeout((int) CONNECT_TIMEOUT.toMillis())
                .setSocketTimeout((int) SOCKET_TIMEOUT.toMillis())
                .build();

        this.client = HttpClients.custom()
                .setMaxConnTotal(1000)
                .setMaxConnPerRoute(100)
                .setDefaultRequestConfig(reqConfig)
                .setKeepAliveStrategy(new LimitConnectionKeepAliveStrategy(30000))
                .disableContentCompression()
                .disableCookieManagement()
                .disableAuthCaching()
                .build();
    }

    @Override
    public SwiftCredentials getSwiftCredentials(SwiftUser swiftUser) {
        final String reqBody = SwiftUser.toJSON(swiftUser, mapper);

        final HttpPost req = new HttpPost(Urls.swiftCredentials());
        req.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON);
        req.setEntity(new StringEntity(reqBody, Charsets.UTF_8));
        addS2SAuthHeaders(req, "POST", Urls.swiftCredentials(), reqBody);

        return execute(
                req, (res, resBody) -> SwiftCredentials.fromJSON(resBody, mapper));
    }

    @Override
    public Principal authenticateSwift(@Nullable String tenantOcid,
                                       String tenantName,
                                       Map<String, List<String>> headers) {
        final AuthUserAndPass authUserAndPass = AuthUserAndPass.fromHeaders(headers);
        final String reqBody = SwiftAuthRequests.toJSON(authUserAndPass, tenantName, tenantOcid, mapper);

        final HttpPost req = new HttpPost(Urls.swiftAuth());
        req.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON);
        req.setEntity(new StringEntity(reqBody, Charsets.UTF_8));
        addS2SAuthHeaders(req, "POST", Urls.swiftAuth(), reqBody);

        return execute(
                req, (res, resBody) -> AuthenticationPrincipal.fromJSON(resBody, mapper));
    }

    private void addS2SAuthHeaders(HttpUriRequest req, String method, URI uri, String body) {
        if (serviceAuthenticator != null) {
            final byte[] bytes = body == null ? null : body.getBytes(Charsets.UTF_8);
            final Map<String, String> svcAuthHeaders = serviceAuthenticator.getSignedRequestHeaders(
                    method, uri, ImmutableMap.of(), Optional.ofNullable(bytes));
            svcAuthHeaders.forEach(req::setHeader);
            req.removeHeaders(HttpHeaders.CONTENT_LENGTH);
        }
    }

    @Override
    public SigningKey getSigningKey(URI uri) {
        final HttpGet request = new HttpGet(uri);
        addS2SAuthHeaders(request, "GET", uri, null);
        return execute(
                request,
                (response, body) -> SigningKey.fromJSON(body, mapper));
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(client);
    }

    @FunctionalInterface
    private interface ResponseHandler<T> {
        T handleResponse(CloseableHttpResponse response, String body);
    }

    private <T> T execute(HttpUriRequest req, ResponseHandler<T> handler) {
        try {
            CloseableHttpResponse response = client.execute(host, req);
            HttpEntity entity = null;
            try {
                int status = response.getStatusLine().getStatusCode();
                entity = response.getEntity();

                String body = "";
                if (entity != null) {
                    try (InputStream inputStream = entity.getContent()) {
                        body = CharStreams.toString(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                    }
                }

                if (status < 200 || status > 399) {
                    if (req.getMethod().equals("HEAD") || Strings.isNullOrEmpty(body)) {
                        throw new AuthDataPlaneServerException(status);
                    }

                    throw new AuthDataPlaneServerException(status, body);
                }

                if (handler != null) {
                    return handler.handleResponse(response, body);
                }

                return null;
            } finally {
                EntityUtils.consumeQuietly(entity);
            }
        } catch (IOException ex) {
            throw new AuthDataPlaneConnectionException(ex);
        }
    }
}
