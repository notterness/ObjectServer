package com.oracle.pic.casper.storageclient.core.replicated;

import com.google.common.base.Preconditions;
import com.oracle.oci.casper.jopenssl.CryptoMessageDigest;
import com.oracle.pic.casper.common.config.v2.VolumeStorageClientConfiguration;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.encryption.DigestUtils;
import com.oracle.pic.casper.common.exceptions.ConflictException;
import com.oracle.pic.casper.common.exceptions.InternalServerErrorException;
import com.oracle.pic.casper.common.exceptions.InvalidDigestException;
import com.oracle.pic.casper.common.host.HostInfo;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.model.Stripe;
import com.oracle.pic.casper.common.rest.CommonHeaders;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.vertx.ClientExceptionTranslator;
import com.oracle.pic.casper.common.vertx.HttpClientUtil;
import com.oracle.pic.casper.common.vertx.stream.TrailingDigestReadStream;
import com.oracle.pic.casper.storageclient.SCRequestContext;
import com.oracle.pic.casper.storageclient.StorageServerConstants;
import com.oracle.pic.casper.storageclient.StorageServerUri;
import com.oracle.pic.casper.storageclient.core.AthenaPutRequestHandler;
import com.oracle.pic.casper.storageclient.core.PutRequestHandler;
import com.oracle.pic.casper.storageclient.erasure.codec.Codec;
import com.oracle.pic.casper.storageclient.erasure.stream.BlockReadStream;
import com.oracle.pic.casper.storageclient.models.PutResult;
import com.oracle.pic.casper.storageclient.models.VolumeStorageContext;
import com.oracle.pic.casper.webserver.api.v2.Chunk;
import io.netty.buffer.ByteBuf;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * {@code ReplicatedPutRequestHandler} is a handler to store an object to a volume backed by N number of replicated
 * stores. If the objects are not stored successfully on the minimum required number of servers, the request will fail.
 *
 * This class will append the calculated digest at the end of the read stream and increment the content length
 * accordingly. The header {@link CommonHeaders#TRAILING_DIGEST} will be set so that clients can be backwards
 * compatible.
 */
public class AthenaReplicatedPutRequestHandler extends ReplicatedWriteRequestHandler implements AthenaPutRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedPutRequestHandler.class);

    /**
     * Instantiates a new instance of {@code ReplicatedPutRequestHandler}.
     *
     * @param vertx              instance of Vert.x
     * @param httpClient         instance of http client
     * @param configuration      the configuration
     */
    AthenaReplicatedPutRequestHandler(Vertx vertx,
                                HttpClient httpClient,
                                VolumeStorageClientConfiguration configuration,
                                HostReachabilityReporter hostReachabilityReporter) {
        super(vertx, httpClient, configuration, hostReachabilityReporter);
    }

    @Override
    public PutResult put(SCRequestContext scRequestContext,
                                            VolumeStorageContext volumeContext,
                                            Codec codec,
                                            Chunk chunk) {
        return put(scRequestContext, volumeContext, codec, chunk, null);
    }

    @Override
    public PutResult put(SCRequestContext scRequestContext,
                                            VolumeStorageContext volumeContext,
                                            Codec codec,
                                            Chunk chunk,
                                            Integer minimumWritesOverride) {
        PutStateMachine stateMachine = new PutStateMachine(scRequestContext, volumeContext, codec, chunk, minimumWritesOverride);
        return stateMachine.start();
    }

    /**
     * Internal context for each individual request to a storage server.
     */
    private abstract static class PutContext extends BaseContext {
        private HttpClientRequest request = null;
        private HttpClientResponse response = null;
        private Throwable throwable = null;
        private boolean cancelled = false;
        private boolean requestCompleted = false;
        private boolean responseCompleted = false;
        private long lastStallTimestamp;
        private boolean continueReceived;

        PutContext(HostInfo hostInfo, MetricScope metricScope, int stripePosition) {
            super(hostInfo, metricScope, stripePosition);
        }

        @Override
        public String toString() {
            return "PutContext{" +
                    "request=" + getRequest() +
                    ", response=" + getResponse() +
                    ", throwable=" + getThrowable() +
                    ", cancelled=" + isCancelled() +
                    ", requestCompleted=" + isRequestCompleted() +
                    ", responseCompleted=" + isResponseCompleted() +
                    ", lastStallTimestamp=" + getLastStallTimestamp() +
                    ", continueReceived=" + isContinueReceived() +
                    ", ok=" + isOk() +
                    ", metricScope=" + getMetricScope() +
                    ", hostInfo=" + getHostInfo() +
                    ", timeoutTimerId=" + getTimeoutTimerId() +
                    '}';
        }

        abstract void updateDigest(Buffer buffer, long bytesTransferred);

        abstract Digest getDigest();

        /**
         * Include the digest (32 bytes) to the {@link io.vertx.core.streams.WriteStream}.
         * This should be done at the end of the data to work with {@link TrailingDigestReadStream}.
         * This is not written to disk with the rest of the chunk/block data, but peeled off the end of the stream and
         * compared to the digest that storage-server computed, and the request will fail if any mismatch is detected.
         */
        private void writeTrailingDigest() {
            Digest calculatedDigest = getDigest();
            byte[] rawDigest = calculatedDigest.getValue();
            request.write(Buffer.buffer(rawDigest));
            request.end();
        }

        /**
         * Returns true if the connection is still ok. This is determined by the following conditions:
         * 1. The request has not been cancelled and
         * 2. The request has not yet been responded by the storage server that something has
         * failed
         *
         * @return a boolean value indicating whether the connection is still ok or not
         */
        public boolean isOk() {
            return !isCancelled() && getThrowable() == null;
        }

        /**
         * The PUT request to the storage server.
         */
        public HttpClientRequest getRequest() {
            return request;
        }

        public void setRequest(HttpClientRequest request) {
            this.request = request;
        }

        /**
         * The PUT response from the storage server.
         */
        public HttpClientResponse getResponse() {
            return response;
        }

        public void setResponse(HttpClientResponse response) {
            this.response = response;
        }

        /**
         * The throwable that may be thrown from the PUT operation to the storage server.
         */
        public Throwable getThrowable() {
            return throwable;
        }

        public void setThrowable(Throwable throwable) {
            this.throwable = throwable;
        }

        /**
         * The cancelled flag.
         */
        public boolean isCancelled() {
            return cancelled;
        }

        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }

        /**
         * The requestCompleted flag indicating whether the request has been sent out completely.
         */
        public boolean isRequestCompleted() {
            return requestCompleted;
        }

        public void setRequestCompleted(boolean requestCompleted) {
            this.requestCompleted = requestCompleted;
        }

        /**
         * The responseCompleted flag indicating whether the response has been received.
         */
        public boolean isResponseCompleted() {
            return responseCompleted;
        }

        public void setResponseCompleted(boolean responseCompleted) {
            this.responseCompleted = responseCompleted;
        }

        /**
         * The last stall timestamp.
         */
        public long getLastStallTimestamp() {
            return lastStallTimestamp;
        }

        public void setLastStallTimestamp(long lastStallTimestamp) {
            this.lastStallTimestamp = lastStallTimestamp;
        }

        /**
         * Whether or not 100-Continue response is received.
         */
        public boolean isContinueReceived() {
            return continueReceived;
        }

        public void setContinueReceived(boolean continueReceived) {
            this.continueReceived = continueReceived;
        }
    }

    static class SharedDigest {
        private DigestAlgorithm digestAlgorithm;
        private CryptoMessageDigest digest;
        private byte[] calculatedDigest = null;
        private long bytesDigested = 0L;

        SharedDigest(DigestAlgorithm digestAlgorithm) {
            this.digestAlgorithm = digestAlgorithm;
            this.digest = DigestUtils.messageDigestFromAlgorithm(digestAlgorithm);
        }

        Digest getDigest() {
            if (calculatedDigest == null) {
                calculatedDigest = digest.digest();
            }

            return new Digest(digestAlgorithm, calculatedDigest);
        }

        /**
         * Update the digest if we've transferred more bytes, otherwise ignore. This is done so the first put context to
         * get here updates the digest. Since the digest is shared, later calls for the same data can be ignored.
         * @param buffer the data buffer
         * @param bytesTransferred the total count of bytes transferred from the incoming read stream
         */
        void updateDigest(Buffer buffer, long bytesTransferred) {
            if (bytesTransferred > bytesDigested) {
                digest.update(buffer.getByteBuf().nioBuffer());
                bytesDigested = bytesTransferred;
            }
        }
    }

    /**
     * In MirroredPutContext, the digest is shared, and the first context to process a buffer of data updates it.
     */
    private static class MirroredPutContext extends PutContext {
        SharedDigest sharedDigest;

        MirroredPutContext(HostInfo hostInfo, MetricScope metricScope, int stripePosition, SharedDigest sharedDigest) {
            super(hostInfo, metricScope, stripePosition);
            this.sharedDigest = sharedDigest;
        }

        @Override
        void updateDigest(Buffer buffer, long bytesTransferred) {
            sharedDigest.updateDigest(buffer, bytesTransferred);
        }

        @Override
        Digest getDigest() {
            return sharedDigest.getDigest();
        }
    }

    /**
     * With EC different data is written to each server, so we compute per-context digests.
     */
    private static class EcPutContext extends PutContext {
        private DigestAlgorithm digestAlgorithm;
        private CryptoMessageDigest digest;
        private byte[] calculatedDigest = null;

        EcPutContext(HostInfo hostInfo, MetricScope metricScope, int stripePosition, DigestAlgorithm digestAlgorithm) {
            super(hostInfo, metricScope, stripePosition);
            this.digestAlgorithm = digestAlgorithm;
            this.digest = DigestUtils.messageDigestFromAlgorithm(digestAlgorithm);
        }

        @Override
        void updateDigest(Buffer buffer, long bytesTransferred) {
            digest.update(buffer.getByteBuf().nioBuffer());
        }

        @Override
        Digest getDigest() {
            if (calculatedDigest == null) {
                calculatedDigest = digest.digest();
            }

            return new Digest(digestAlgorithm, calculatedDigest);
        }
    }

    /**
     * A state machine to handle the all the interactions between storage client and storage servers.
     */
    private class PutStateMachine extends ReplicatedWriteStateMachine {
        private final CompletableFuture<PutResult> result;
        private final Codec codec;
        private final List<PutContext> putContexts;
        private final BitSet fullSet = new BitSet();
        private final Thread currentThread;
        private final MetricScope rootScope;
        private int connectCount;
        private boolean startWriting;
        private int writeCount;
        private long bytesTransferred = 0L;
        private long stallTimerId = INVALID_TIMER_ID;
        private long blockSize;
        private Chunk chunk;

        /**
         * Instantiates a new Put state machine.
         * @param scRequestContext the request context
         * @param volumeContext    the volume context
         * @param codec            the codec
         * @param chunk            the chunk
         */
        PutStateMachine(SCRequestContext scRequestContext, VolumeStorageContext volumeContext, Codec codec,
                        Chunk chunk, Integer minimumWritesOverride) {
            super(scRequestContext, volumeContext, minimumWritesOverride);
            this.result = new CompletableFuture<>();
            this.codec = codec;
            if (getVolumeStorageContext().getStripeDefinition().isEC()) {
                this.blockSize = subBlockSize * volumeContext.getStripeDefinition().getK();
            } else {
                this.blockSize = chunk.getSize();
            }
            this.chunk = chunk;
            this.putContexts = new ArrayList<>(volumeContext.getStripeDefinition().getN());
            this.currentThread = Thread.currentThread();
            this.rootScope = getScRequestContext().getMetricScope()
                    .child("put")
                    .annotate("volume", volumeContext.getVolumeId())
                    .annotate("von", volumeContext.getVon());
        }

        /**
         * Cancels all pending requests to storage nodes and mark the task as sendCompleted with failure.
         * <p/>
         * When the request fails, it need to ensure that the entire data is read off the stream. If not, an
         * asynchronous HTTP client may wait forever to send the remaining data.
         * <p/>
         * Note that this method may be called more than once per request but we will ensure that the logic inside
         * will be called only once.
         */
        @Override
        protected void failRequest() {
            SSC_PUT_FAILURES.mark();
            if (!result.isDone()) {
                try {
                    putContexts.forEach(this::cancelPendingRequest);

                    // TODO: Define a proper exception for this
                    StringBuilder sb = new StringBuilder("[");
                    sb.append(getScRequestContext().getOpcRequestId());
                    sb.append(String.format("] Put quorum failed:%n"));
                    boolean conflict = false;
                    for (PutContext putContext : putContexts) {
                        HostInfo hostInfo = putContext.getHostInfo();
                        if (putContext.getThrowable() != null) {
                            conflict |= putContext.getThrowable() instanceof ConflictException;
                            sb.append(String.format("   %s:%d failed with %s%n",
                                    hostInfo.getHostname(), hostInfo.getPort(),
                                    putContext.getThrowable()));
                        } else {
                            sb.append(String.format("   %s:%d had not failed yet%n",
                                    hostInfo.getHostname(), hostInfo.getPort()));
                        }
                    }
                    // A ConflictException is the only user-triggerable exception we expect at the storage client.
                    Throwable ex = conflict ?
                            new ConflictException(sb.toString()) : new InternalServerErrorException(sb.toString());
                    logger.trace("Generated failure exception: ", ex);
                    rootScope.fail(ex);
                    String userErrorMessage = "The Object was updated Concurrently.";
                    Throwable userEx = conflict ?
                            new ConflictException(userErrorMessage) : new InternalServerErrorException();
                    result.completeExceptionally(userEx);
                } catch (Exception ex) {
                    logger.error("Got an exception while failing the PUT request.", ex);
                }
            }
        }

        @Override
        protected boolean isRequestFailed() {
            return result.isCompletedExceptionally();
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        /**
         * Start putting the data to storage servers.
         */
        public PutResult start() {
            logger.debug("Starting a PUT operation for {}/{}", getVolumeId(), getVonId());
            // we are not ready to read it yet

            // Adjust the content length to account for a (mandatory) trailing SHA256 digest
            final long encodedContentLength = codec.getEncodedBlockContentLength(chunk.getSize(), subBlockSize) +
                    DigestAlgorithm.SHA256.getLengthInBytes();
            Duration responseTimeout = getConfiguration().getHttpClientWriteResponseTimeout();
            Stripe<HostInfo> hostInfoStripe = getVolumeStorageContext().getHostInfoStripe();
            // SharedDigest is used to avoid computing the chunk/block digest repeatedly for mirrored volumes.
            final SharedDigest sharedDigest = new SharedDigest(DigestAlgorithm.SHA256);

            hostInfoStripe.getPresentParts().forEach(part -> {
                HostInfo hostInfo = part.part();
                String hostName = hostInfo.getHostname();
                int port = hostInfo.getPort();
                HttpClientRequest clientRequest =
                        getHttpClient().put(port, hostName, StorageServerUri.getObjectV2Uri(getVolumeId(), getVonId()));

                clientRequest.putHeader(HttpHeaders.CONTENT_LENGTH, Long.toString(encodedContentLength));
                clientRequest.putHeader(CommonHeaders.TRAILING_DIGEST, "true");
                clientRequest.putHeader(HttpHeaders.EXPECT, "100-Continue");
                addCommonHeaders(clientRequest, getScRequestContext());

                MetricScope metricScope = rootScope
                        .child("put")
                        .annotate("host", hostName)
                        .annotate("port", port);
                //Add them for the correct child scope
                addTracingHeaders(clientRequest, metricScope);

                PutContext putContext;
                if (getVolumeStorageContext().getStripeDefinition().isEC()) {
                    putContext = new EcPutContext(hostInfo, metricScope, part.position(), DigestAlgorithm.SHA256);
                } else {
                    putContext = new MirroredPutContext(hostInfo, metricScope, part.position(), sharedDigest);
                }
                putContext.setRequest(clientRequest);
                putContexts.add(putContext);
                putContext.setTimeoutTimer(getVertx(), clientRequest, responseTimeout);
                clientRequest.continueHandler(x -> handleContinue(putContext));
                clientRequest.exceptionHandler(x -> handleException(putContext, x));
                clientRequest.handler(x -> this.handleResponse(putContext, x));

                clientRequest.sendHead();
            });
            calculateAndSetErrorThreshold(putContexts.size());
            // TODO: convert this to using non-vertx client
            try {
                return result.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Couldn't upload to storage server", e);
            }
        }

        public void abort() {
            failRequest();
        }

        /**
         * Waits for enough number of connections to be ready before starting sending the data.
         *
         * @param putContext Internal context
         */
        private void handleContinue(PutContext putContext) {
            assert currentThread == Thread.currentThread();

            final HostInfo hostInfo = putContext.getHostInfo();
            logger.debug("Receiving 100-Continue putting {}/{} to {}:{}",
                    getVolumeId(), getVonId(), hostInfo.getHostname(), hostInfo.getPort());

            putContext.cancelTimeoutTimer(getVertx());

            if (putContext.isCancelled()) {
                // try to abort the request if it is already cancelled. This is done by closing the connection
                // in the middle of the request.
                logger.debug("Aborting request to put {}/{} to {}:{}", getVolumeId(), getVonId(),
                        hostInfo.getHostname(), hostInfo.getPort());
                HttpClientUtil.abort(putContext.getRequest());
            } else {
                putContext.setContinueReceived(true);

                // hold off on sending anything until we get a quorum
                if (!startWriting && ++connectCount >= getMinimumSuccessThreshold()) {
                    logger.debug("Start sending data for {}/{}", getVolumeId(), getVonId());
                    startWriting = true;
                    for (Chunk subChunk : chunk.getSubChunks((int)blockSize)) {
                        handleRead(Buffer.buffer(subChunk.getData()));
                    }
                    handleReadEnd();
                }
            }
        }

        /**
         * Handles the exception from connecting to the storage server i.e. connection close, dead server, etc. but
         * not any exceptions at the protocol level.
         *
         * @param putContext Internal context
         * @param throwable  The throwable from connecting to the storage server
         */
        private void handleException(PutContext putContext, Throwable throwable) {
            assert currentThread == Thread.currentThread();

            // ignore any exception handling if the PUT request call already completes.
            // to workaround a bug in Vertx 3.0
            if (putContext.isResponseCompleted()) {
                return;
            }

            putContext.cancelTimeoutTimer(getVertx());

            HostInfo hostInfo = putContext.getHostInfo();
            logger.error("Error on put of {}/{} connect to {}:{} failed, bytes transferred={}, time taken={}",
                    getVolumeId(), getVonId(),
                    hostInfo.getHostname(), hostInfo.getPort(),
                    bytesTransferred,
                    System.currentTimeMillis() - getScRequestContext().getStartTime(),
                    throwable);
            // don't try to double count the exception
            if (putContext.isOk()) {
                putContext.getMetricScope().fail(throwable);
                putContext.setThrowable(throwable);
                // only try to cancel the request if we haven't received a completed response yet
                if (!putContext.isResponseCompleted()) {
                    cancelPendingRequest(putContext);
                }
                reportFailedCall(putContext.getHostInfo());
                incrementErrorCountAndCheck();
            } else {
                logger.debug("A previous error on put of {}/{} was already recorded ({}). " +
                                "The current error count is {}. Ignoring.",
                        getVolumeId(), getVonId(), throwable, getErrorCount());
            }
        }

        /**
         * Handles all the protocol exception (i.e. a request that returns a 4XX/5XX response.
         *
         * @param putContext Internal context
         * @param throwable  the exception returned from the server
         */
        private void handleProtocolException(PutContext putContext, Throwable throwable) {
            assert currentThread == Thread.currentThread();

            putContext.cancelTimeoutTimer(getVertx());

            HostInfo hostInfo = putContext.getHostInfo();
            logger.error("Got an error putting {}/{} to {}({}:{}), bytes transferred={}, time taken={}",
                    getVolumeId(), getVonId(),
                    hostInfo.getHostname(), hostInfo.getPort(),
                    bytesTransferred,
                    System.currentTimeMillis() - getScRequestContext().getStartTime(),
                    throwable);
            // don't try to double count the exception
            if (putContext.isOk()) {
                putContext.getMetricScope().fail(throwable);
                putContext.setThrowable(throwable);
                reportFailedCall(putContext.getHostInfo());
                incrementErrorCountAndCheck();
            }
        }

        /**
         * When we get a response back from the storage server, we need to evaluate it
         * Note: this method is called for each successful/failed responses from storage servers.
         *
         * @param putContext PUT context
         * @param response   HTTP response object
         */
        private void handleResponse(PutContext putContext, HttpClientResponse response) {
            assert currentThread == Thread.currentThread();

            putContext.cancelTimeoutTimer(getVertx());

            putContext.setResponseCompleted(true);
            HostInfo hostInfo = putContext.getHostInfo();
            int statusCode = response.statusCode();
            logger.debug("Received response code {} on put of {}/{} to {}:{}",
                    statusCode, getVolumeId(), getVonId(),
                    hostInfo.getHostname(), hostInfo.getPort());
            putContext.setResponse(response);
            if (HttpResponseStatus.isError(statusCode)) {
                ClientExceptionTranslator.readException(response, ex -> handleProtocolException(putContext, ex));
            } else if (HttpResponseStatus.isSuccessful(statusCode)) {
                reportSuccessfulCall(putContext.getHostInfo());
                response.bodyHandler(b -> handleResponseBody(putContext, b));
            } else {
                logger.error("Response code {} on put of {}/{} is not expected from {}:{}",
                        statusCode, getVolumeId(), getVonId(),
                        hostInfo.getHostname(), hostInfo.getPort());
                handleProtocolException(putContext, new IllegalStateException("The error code " + statusCode +
                        " is not expected."));
            }
        }

        /**
         * Reads a successful response body to verify MD5 checksum.
         *
         * @param putContext the PUT context
         * @param jsonBlob   the response
         */
        private void handleResponseBody(PutContext putContext, Buffer jsonBlob) {
            assert currentThread == Thread.currentThread();

            try {
                JsonObject json = new JsonObject(jsonBlob.toString());

                final String digestValue = json.getString(StorageServerConstants.DIGEST_VALUE);
                final DigestAlgorithm digestAlgorithm =
                        DigestAlgorithm.valueOf(json.getString(StorageServerConstants.DIGEST_ALGORITHM));
                Digest storageServerDigest = Digest.fromBase64Encoded(digestAlgorithm, digestValue);
                Digest calculatedDigest = putContext.getDigest();
                validateDigest(calculatedDigest, storageServerDigest, putContext);

                putContext.getMetricScope().end();

                // if it comes to this point, it passes the validation, let's bump it up
                incrementSuccessCountAndCheck(json, () -> {
                    PutResult r = new PutResult(bytesTransferred);
                    result.complete(r);
                    rootScope.end();
                });
                SSC_PUT_BYTES_TRANS.inc(bytesTransferred);

            } catch (Exception ex) {
                HostInfo hostInfo = putContext.getHostInfo();
                logger.error("Got an error while processing JSON response from put of {}/{} to {}:{}",
                        getVolumeId(), getVonId(), hostInfo.getHostname(), hostInfo.getPort(), ex);
                handleProtocolException(putContext, ex);
            }
        }

        private void validateDigest(Digest expected, Digest actual, PutContext context) {
            if (!expected.equals(actual)) {
                final HostInfo hostInfo = context.getHostInfo();
                logger.warn("SS that we were writing {}/{} to got a corrupted byte stream: {}:{}. " +
                                "Expecting {} but received {}",
                        getVolumeId(), getVonId(), hostInfo.getHostname(), hostInfo.getPort(), expected, actual);
                final String msg =
                        "Data corruption detected for chunk " + getVolumeId() + "/" + getVonId() +
                                " retrieved from server " + hostInfo.getHostname() + ":" + hostInfo.getPort();
                throw new InvalidDigestException(expected, actual, msg);
            }
        }

        /**
         * When the end of the read stream is reached, tell all the storage servers that we are done.
         * Note: this method is expected to be called only once when it reaches EOF on the read stream.
         */
        private void handleReadEnd() {
            assert currentThread == Thread.currentThread();

            logger.debug("Finish sending data for {}/{}, bytes transferred={}, time taken={}",
                    getVolumeId(), getVonId(), bytesTransferred,
                    System.currentTimeMillis() - getScRequestContext().getStartTime());
            for (PutContext putContext : putContexts) {
                if (!putContext.isCancelled()) {

                    //include the trailing digest
                    putContext.writeTrailingDigest();
                    // mark the request as completed.
                    putContext.setRequestCompleted(true);
                }
                putContext.cancelTimeoutTimer(getVertx());
            }

            // if there is any pending timer, cancel it
            cancelStallTimer();
        }

        /**
         * Cancels any outstanding requests if possible.
         *
         * @param putContext Internal context
         */
        private void cancelPendingRequest(PutContext putContext) {
            if (!putContext.isCancelled()) {
                final HostInfo hostInfo = putContext.getHostInfo();
                if (!putContext.isRequestCompleted()) {
                    if (putContext.isContinueReceived()) {
                        // only try to close the connection if the connection is alive
                        logger.debug("Trying to cancel the pending put of {}/{} to {}:{}",
                                getVolumeId(), getVonId(), hostInfo.getHostname(), hostInfo.getPort());
                        HttpClientUtil.abort(putContext.getRequest());
                    } else {
                        logger.debug("Trying to cancel the pending put of {}/{} to {}:{}, but it has not " +
                                        "received 100-Continue from the server yet",
                                getVolumeId(), getVonId(), hostInfo.getHostname(), hostInfo.getPort());
                    }
                }
                // this storage server will not receive any more data.
                if (putContext.getResponse() != null) {
                    logger.debug("Already received the entire response for {}:{}. Ignoring.",
                            hostInfo.getHostname(), hostInfo.getPort());
                } else {
                    // we haven't received the response so we cannot cancel the request just yet. we need to ensure
                    // that we read the entire TCP response. Otherwise, we will have a connection leak. So the best
                    // way to protect this is to ensure that we read the data into a blackhole instead.
                    logger.debug("Change the response handler for the request to {}:{}",
                            hostInfo.getHostname(), hostInfo.getPort());
                    try {
                        // try out best to change the handler. This call may fail if the request has been completed
                        // already.
                        putContext.getRequest().handler(this::handleResponseAfterCancelled);
                    } catch (IllegalStateException ex) {
                        // if it comes to this, it means that the request is already completed. It's too late to do
                        // anything now.
                        logger.trace("Fail to reset the request handler", ex);
                    }
                }
                // mark the request as cancelled so that handleRead() will not try to send the data to this storage
                // server anymore.
                putContext.setCancelled(true);
            }
        }

        /**
         * Handles the response returned by the storage server once the request has been cancelled but it has not
         * yet return a response at the cancellation time.
         *
         * @param response Vert.x HTTP client response
         */
        private void handleResponseAfterCancelled(HttpClientResponse response) {
            // if the data still keep coming, it will be drained to a black hole.
            try {
                response.handler(buf -> {
                });
            } catch (IllegalStateException ex) {
                // you might run into this exception, let's just ignore them
                logger.trace("Fail to reset the response handler", ex);
            }
        }

        /**
         * Writes data to all the connected storage servers and tries to pause if there is a backpressure
         * Note: this method is called once per ReadStream read call.
         *
         * @param buffer the buffer that contains the data
         */
        private void handleRead(Buffer buffer) {
            if (getVolumeStorageContext().getStripeDefinition().isEC()) {
                int blocksSetSize = subBlockSize * getVolumeStorageContext().getStripeDefinition().getK();
                Preconditions.checkArgument((buffer.length() == blocksSetSize) ||
                        (writeCount == chunk.getSize() / blocksSetSize));
            }
            writeCount++;

            try {
                Buffer[] encodedData = codec.encodeBuffer(buffer,
                        codec.getPaddedBlockSize(buffer.length(), subBlockSize));
                bytesTransferred += buffer.length();

                for (int i = 0; i < putContexts.size(); ++i) {
                    PutContext putContext = putContexts.get(i);
                    // keep writing the data as long as we have not seen any problem with the PUT to the storage server
                    if (putContext.isOk()) {
                        HttpClientRequest request = putContext.getRequest();

                        // Important: the number of put contexts may vary.
                        // Always use the stripe position rather than the index in the list!
                        Buffer dataBuffer = encodedData[putContext.getStripePosition()];
                        request.write(dataBuffer);
                        putContext.updateDigest(dataBuffer, bytesTransferred);

                        if (request.writeQueueFull()) {
                            fullSet.set(i);
                            request.drainHandler(x -> handleDrain(putContext));
                            putContext.setLastStallTimestamp(System.currentTimeMillis());
                        }
                    } else {
                        fullSet.clear(i);
                    }
                }
            } catch (Exception ex) {
                logger.error("Error while transferring data for {}/{}.", getVolumeId(), getVonId(), ex);
            } finally {
                if (!fullSet.isEmpty()) {
                    startStallTimer();
                }
            }
        }

        /**
         * When the write stream to a storage server is drained, we need to see whether we can resume the read or not.
         *
         * @param putContext Internal context
         */
        private void handleDrain(PutContext putContext) {
            putContext.setLastStallTimestamp(-1);
            int pos = putContexts.indexOf(putContext);
            assert pos >= 0 : "Could not find pos";
            fullSet.clear(pos);
            if (fullSet.isEmpty()) {
                // if there is any pending stall timer, cancel it.
                cancelStallTimer();
            }
        }

        /**
         * Cancel any pending stall timer if there is any.
         */
        private void cancelStallTimer() {
            if (stallTimerId != INVALID_TIMER_ID) {
                logger.trace("Canceling stall timer");
                getVertx().cancelTimer(stallTimerId);
                stallTimerId = INVALID_TIMER_ID;
            }
        }

        /**
         * Start the stall timer if not already started.
         */
        private void startStallTimer() {
            if (stallTimerId == INVALID_TIMER_ID) {
                int currentCount = writeCount;
                logger.trace("Starting stall timer, writeCount={}", currentCount);
                stallTimerId = getVertx().setTimer(getConfiguration().getStallTimeout().toMillis(),
                        this::handleReadStall);
            }
        }

        /**
         * This method is scheduled to be called every time there is a stall for a period of time. If the request has
         * been stalled for a long period of time, it will try to cancel any stall request and move on.
         */
        private void handleReadStall(long timerId) {
            assert currentThread == Thread.currentThread();

            // stall occur for too long without making any progress
            for (int i = fullSet.nextSetBit(0); i >= 0; i = fullSet.nextSetBit(i + 1)) {
                PutContext putContext = putContexts.get(i);
                long stallTime = System.currentTimeMillis() - putContext.getLastStallTimestamp();
                final HostInfo hostInfo = putContext.getHostInfo();
                if (putContext.isOk()) {
                    logger.warn("Server: {}:{} has been stalled for {} ms",
                            hostInfo.getHostname(), hostInfo.getPort(), stallTime);
                    cancelPendingRequest(putContext);
                    reportFailedCall(putContext.getHostInfo());
                    incrementErrorCountAndCheck();
                } else {
                    logger.debug("Server: {}({}:{}) has been stalled for {} ms but it already failed. " +
                                    "Ignoring.",
                            hostInfo.getHostname(), hostInfo.getPort(), hostInfo.getControlPort(), stallTime);
                }
            }
            fullSet.clear();
            // force the read to be resumed
        }
    }
}
