package com.oracle.pic.casper.webserver.api.backend.putobject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.net.HostAndPort;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.backend.putobject.storageserver.ErrorResponse;
import com.oracle.pic.casper.webserver.api.backend.putobject.storageserver.PutObjectResponse;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * A state machine for writing a replica of a chunk to a storage server.
 *
 * SSClientMachine is a state machine that receives events from a PutObjectMachine, makes changes to its internal state,
 * asks the environment to take actions (through the HttpClientRequest and HttpClientResponse interfaces), and exposes
 * getters that are used by the PutObjectMachine to make its own state changes.
 *
 * -- How Chunk Replicas Are Written --
 *
 * The "happy" path behavior for the write of a chunk replica to a storage server is the following:
 *
 *  1. The request is sent to the storage server with a Content-Length and Expect: 100-Continue header.
 *  2. The storage server responds with "100 Continue".
 *  3. The request begins to stream the bytes of the chunk to the storage server, pausing when the queue is full, and
 *     resuming when it drains, until all of the bytes have been sent.
 *  4. The storage server returns a 200 OK that contains the number of bytes it read and the SHA-256 digest of those
 *     bytes.
 *  5. The request compares the digest computed by the storage server with a digest computed by the request as it
 *     streamed the bytes. If they match, the request is considered a success.
 *
 * The storage server can return a 4xx or 5xx error instead of "100 Continue", in which case the bytes of the response
 * are read, the error is logged, and the request is considered to have failed. The storage server can also return a
 * 4xx or 5xx error anytime while the request is streaming data to it, or after the request has finished streaming data
 * to it.
 *
 * When the state machine determines that the request has failed, either due to an error response, or a request from
 * the PutObjectMachine to abort the request, it will do a check to see if it should close the underlying HTTP
 * connection (to prevent it from being re-used). If the state machine believes that the request is completed (that is,
 * all the bytes of the request and response have been sent and/or received), then it will leave the connection open for
 * re-use. Otherwise it will close it.
 *
 * This implementation assumes that the storage server will never return a 200 OK response before it has read all of the
 * bytes of the request. If a faulty storage server does that, the only way we can catch it is if the computed SHA-256
 * is incorrect. Currently we do not send the SHA-256 as part of the headers, so the storage server would have to
 * correctly guess the SHA-256 in order to return a correct response before reading all the bytes. That seems unlikely,
 * so we kept the implementation simple.
 *
 * -- States --
 *
 * This is a (very) high level overview of the states of the storage server client. These are not used directly by the
 * implementation, but they should help understand why the implementation is structured as it is.
 *
 * There are three states that the HTTP request to the storage server can be in:
 *
 *  A. We have not received a "100 Continue" from the storage server.
 *  B. We have received a "100 Continue" from the storage server, but have not yet written all the bytes of the request.
 *  C. We have received a "100 Continue" from the storage server and have written all the bytes of the request.
 *
 * There are five states that the HTTP response from the storage server can be in:
 *
 *  1. The response message has not arrived.
 *  2. The response message has arrived, but it was malformed (for example, there was no Content-Length).
 *  3. The response message has arrived, was well formed, but not all the bytes of the response have been read.
 *  4. The response message has arrived, was well formed, all the bytes have been read, and either the response was
 *     an error (4xx/5xx), the body was malformed (e.g. invalid JSON, wrong fields) or it was a 200 response, but the
 *     checksum and/or length did not match what was sent.
 *  5. The response message has arrived, was well formed, all the bytes have been read and the response was a 200 with
 *     the correct checksum and length.
 *
 * These sets of states are orthogonal, so there are fifteen overall states to track.
 *
 * There is also a 16th state, which occurs when the connection to the storage server is closed. In that state, no more
 * events will arrive, and the request is considered to have failed.
 *
 * -- Events --
 *
 * The state machine has methods for handling the following events (see the descriptions in PutObjectMachine):
 *
 *  - HttpServerRequest#handler: handled by the serverReqBuf method.
 *  - HttpClientRequest#continueHandler: handled by the clientReqContinue method.
 *  - HttpClientRequest#handler: handled by the clientResArrive method.
 *  - HttpClientResponse#handler: handled by the clientResBuffer method.
 *  - close: used to close the connection by PutObjectMachine (e.g., when the whole request has failed).
 *
 * TODO: Add early response detection, or live with assumption that 200 OK only arrives after all bytes are sent?
 */
final class SSClientMachine {
    private static final Logger LOG = LoggerFactory.getLogger(SSClientMachine.class);

    enum Status {
        /**
         * The request is still being sent and/or the response is still being received.
         * States: A1, A3, B1, B3, C1, C3 (see class docs)
         */
        PENDING,

        /**
         * The request was sent and the response received with the correct checksum.
         * States: C5 (see class docs)
         */
        SUCCEEDED,

        /**
         * The request has failed, see the FailedReason for the code that explains why.
         * States: A2, A4, A5, B2, B4, B5, C2, C4 (see class docs)
         */
        FAILED
    }

    private final ObjectMapper mapper;
    private final HttpClientRequest clientReq;
    private final String requestId;
    private final HostAndPort hostAndPort;
    private final int length;

    /**
     * The HTTP message part of the response from the storage server, or null if no response has arrived.
     */
    private HttpClientResponse clientRes = null;
    /**
     * The value of the Content-Length header of the response. This is -1 if clientRes is null, and also if
     * clientRes is not null, but the response message is malformed (e.g., it does not have a Content-Length header).
     */
    private int clientResLen = -1;
    /**
     * The body of the response from the storage server. When clientResLen is -1, this always has length zero, otherwise
     * it has length between 0 and clientResLen.
     */
    private Buffer clientResBody = Buffer.buffer();
    /**
     * When clientResBody.length() == clientResLen, this is true if the parsed response was an error, was unparseable,
     * or was a success but contained the wrong checksum. Otherwise this is false.
     */
    private boolean clientResErr = false;

    /**
     * True if the storage server has returned a "100 Continue" message.
     */
    private boolean continued = false;
    /**
     * The number of bytes sent to the storage server as part of the request body.
     */
    private int bytesSent = 0;
    /**
     * True if the connection was closed, after which the request is considered to have failed unless it had previously
     * succeeded.
     */
    private boolean closed = false;

    SSClientMachine() {
        mapper = null;
        clientReq = null;
        requestId = null;
        hostAndPort = null;
        length = 0;
    }

    SSClientMachine(
            ObjectMapper mapper, HttpClientRequest clientReq, String requestId, HostAndPort hostAndPort, int length) {
        Preconditions.checkArgument(length >= 0);

        this.mapper = Preconditions.checkNotNull(mapper);
        this.clientReq = Preconditions.checkNotNull(clientReq);
        this.requestId = Preconditions.checkNotNull(requestId);
        this.hostAndPort = Preconditions.checkNotNull(hostAndPort);
        this.length = length;
    }

    Status getStatus() {
        if (closed || clientResErr) {
            return Status.FAILED;
        }

        if (continued &&
                bytesSent == length &&
                clientRes != null &&
                clientResLen > 0 &&
                clientResBody.length() == clientResLen &&
                !clientResErr) {
            return Status.SUCCEEDED;
        }

        return Status.PENDING;
    }

    int getBytesSent() {
        return bytesSent;
    }

    int getContentLength() {
        return length;
    }

    boolean isContinued() {
        return continued;
    }

    boolean isQueueFull() {
        return continued && bytesSent < length && !closed && clientReq.writeQueueFull();
    }

    boolean isWritable() {
        return continued &&
                clientRes == null &&
                !clientReq.writeQueueFull() &&
                bytesSent < length &&
                !closed;
    }

    boolean isClosed() {
        return closed;
    }

    void serverReqBuffer(Buffer buffer) {
        Verify.verify(isWritable());
        Verify.verify(bytesSent + buffer.length() <= length);

        clientReq.write(buffer);
        bytesSent += buffer.length();

        if (bytesSent == length) {
            clientReq.end();
        }
    }

    void clientReqContinue() {
        Verify.verify(!continued && clientRes == null && bytesSent == 0 && !closed);

        if (length == 0) {
            clientReq.end();
        }

        continued = true;
    }

    void clientResArrive(HttpClientResponse response) {
        Verify.verify(clientRes == null && !closed);

        clientRes = response;

        final String contentLenStr = clientRes.getHeader(HttpHeaders.CONTENT_LENGTH);
        if (contentLenStr == null) {
            close();
        } else {
            try {
                clientResLen = Integer.parseInt(contentLenStr);
                if (clientResLen <= 0) {
                    close();
                }
            } catch (NumberFormatException ex) {
                close();
            }
        }
    }

    void clientResBuffer(Buffer buffer, @Nullable String expectedDigest) {
        Verify.verify(clientRes != null &&
                clientResLen > 0 &&
                clientResBody.length() + buffer.length() <= clientResLen &&
                !clientResErr &&
                !closed);

        clientResBody.appendBuffer(buffer);

        if (clientResBody.length() == clientResLen) {
            if (clientRes.statusCode() == HttpResponseStatus.CREATED) {
                try {
                    final PutObjectResponse putObjectResponse =
                            mapper.readValue(clientResBody.getBytes(), PutObjectResponse.class);

                    if (expectedDigest == null) {
                        LOG.warn("[{}] Received a successful response before all data had been written", requestId);
                        clientResErr = true;
                    } else if (!expectedDigest.equals(putObjectResponse.getDigestValue()) ||
                            length != putObjectResponse.getLength()) {
                        LOG.warn("[{}] The checksums and/or lengths did not match", requestId);
                        clientResErr = true;
                    }
                } catch (IOException ex) {
                    LOG.error("[{}] Failed to parse the response body from the storage server", requestId, ex);
                    close(true /* force */);
                }
            } else {
                try {
                    final ErrorResponse errRes = mapper.readValue(clientResBody.getBytes(), ErrorResponse.class);
                    clientResErr = true;
                } catch (IOException ex) {
                    LOG.error("[{}] Failed to parse the response body from the storage server", requestId, ex);
                    close(true /* force */);
                }
            }

            if (continued && bytesSent < length) {
                LOG.error("[{}] The full response was received from the storage server before the request was " +
                        "complete, so the connection will be closed", requestId);
                close();
            }
        }
    }

    void close() {
        close(false);
    }

    private void close(boolean force) {
        /*
         * We avoid closing the connection if it can be safely re-used, which occurs in the following cases:
         *
         *  1. The request body was fully written out, and the response was fully read.
         *  2. The request was not continued, and a response was fully read.
         *
         * Vert.x will not send any more events for a connection in either of those states, so it is safe to mark the
         * connection as closed in our state, but not actually close it.
         *
         * There are cases where the request is potentially re-usable, but our application is unable to parse the
         * response data. In those cases, we use the force flag to force a close on the underlying connection.
         */

        boolean r1 = continued &&
                bytesSent == length &&
                clientRes != null &&
                clientResLen > 0 &&
                clientResBody.length() == clientResLen;
        boolean r2 = !continued &&
                bytesSent == 0 &&
                clientRes != null &&
                clientResLen > 0 &&
                clientResBody.length() == clientResLen;

        if (force || (!closed && !r1 && !r2)) {
            if (clientReq.connection() != null) {
                clientReq.connection().close();
            }
            closed = true;
        }
    }
}
