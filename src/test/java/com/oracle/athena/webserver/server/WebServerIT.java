package com.oracle.athena.webserver.server;


import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.BadMessageException;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test represents a basic integration test for the ServerChannelLayer class.
 * <p></p>
 * What makes this an integration test vs. a unit test?
 * <ul>
 *     <li>It does not contain "Test" or "Tests" anywhere in the name and ends with "IT"</li>
 *     <li>It requires that the {@link ServerChannelLayer#HTTP_TCP_PORT} be available on the machine performing the build</li>
 *     <li>It tests something end-to-end. These will typically manifest themselves as client interactions.</li>
 * </ul>
 */
class WebServerIT {

    private static final String TARGET_HOST = "http://localhost:" + ServerChannelLayer.HTTP_TCP_PORT + "/";
    private static HttpClient client;
    private static WebServer server;

    /**
     * Before any test in this class is run, start up a server.
     */
    @BeforeAll
    private static void beforeAllTests() throws Exception {
        /*
            FIXME: The number of threads, port selected, and client ID provided should be dynamic
            Right now, these values are either defaulted or using a fixed value, but as we scale this test suite to be a
            full fledged test suite these values will eventually collide.  To remedy this, we should have some global
            state keeping track of what ports are being used by which tests and which clients are assigned to them.

            Load tests should be handled in a separate test area similar to how the "manual" ones are handled today.
         */
        server = new WebServer(WebServerFlavor.INTEGRATION_TESTS, 1);
        server.start();
        client = new HttpClient();
        // in async mode, force this particular client to send all events in order
        client.setStrictEventOrdering(true);
        client.start();

    }

    /**
     * After every test in this class has run, stop the server.
     */
    @AfterAll
    private static void afterAllTests() throws Exception {
        server.stop();
        client.stop();
    }

    /**
     * This test merely attempts to connect a client to our custom Web Server and send a simple message across to it.
     */
    @Test
    void validateBasicConnection() throws InterruptedException, ExecutionException, TimeoutException {
        Request request = client.newRequest(TARGET_HOST).method(HttpMethod.PUT).content(new StringContentProvider("Hello world."));
        // send the request synchronously - this particular client supports async calls as well
        ContentResponse response = request.send();
        assertEquals(response.getStatus(), HttpStatus.OK_200, "A basic connection should result in a 200.");
    }

    /**
     * Similar to the basic connection test, this test simply sends an array as its contents
     */
    @Test
    void validateBasicBufferSend() throws InterruptedException, ExecutionException, TimeoutException {
        byte[] array = UUID.randomUUID().toString().getBytes();
        Request request = client.newRequest(TARGET_HOST).method(HttpMethod.PUT).content(new BytesContentProvider(array));
        ContentResponse response = request.send();
        assertEquals(response.getStatus(), HttpStatus.OK_200, "A basic connection should result in a 200.");
    }

    /**
     * This test validates that when a basic connection is passed in with a mix of both "\r" and "\n" the parser
     * behaves correctly
     */
    @Test
    @Disabled("This doesn't work and I suspect it's because of the control characters.")
    void validateFunkyPayload() throws TimeoutException, InterruptedException, ExecutionException {
        final String payload = "{\n" +
                "  \"cidrBlock\": \"172.16.0.0/16\",\n" +
                "  \"compartmentId\": \"ocid1.compartment.oc1.aaaaaaaauwjnv47knr7uuuvqar5bshnspi6xoxsfebh3vy72fi4swgrkvuvq\",\n" +
                "  \"displayName\": \"Apex Virtual Cloud Network\"\n" +
                "}\n\r\n";
        Request request = client.newRequest(TARGET_HOST);
        request.method(HttpMethod.PUT);
        request.content(new StringContentProvider(payload));
        InputStreamResponseListener listener = new InputStreamResponseListener();
        // Wait for the response headers to arrive
        Response response = listener.get(5, TimeUnit.SECONDS);
        assertNotNull(response);
    }

    /**
     * This particular test validates that we get an error back from the server when we send an incorrect content-length
     * header value.
     */
    @Test
    void validateMalformedContentLengthHeaders() throws InterruptedException, TimeoutException {
        // alternative method of getting stuff asynchronously
        InputStreamResponseListener listener = new InputStreamResponseListener();
        Request request = client.newRequest(TARGET_HOST);
        request.method(HttpMethod.PUT);
        request.content(new StringContentProvider("Hello world!"));
        request.header(HttpHeader.CONTENT_LENGTH, "10000");
        request.send(listener);
        // Wait for the response headers to arrive
        try {
            Response response = listener.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertTrue(e.getCause().getClass().isAssignableFrom(BadMessageException.class));
            // this is a safe cast, but any cast should be frowned upon in production code
            BadMessageException bme = (BadMessageException) e.getCause();
            assertEquals(bme.getCode(), HttpStatus.INTERNAL_SERVER_ERROR_500);
            assertTrue(bme.getReason().contains(HttpHeader.CONTENT_LENGTH.asString()));
        }
    }
}
