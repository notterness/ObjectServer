package com.oracle.pic.casper.webserver.api.backend.putobject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;

import com.google.common.net.HostAndPort;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.encryption.DigestUtils;
import com.oracle.pic.casper.common.exceptions.InvalidDigestException;
import com.oracle.pic.casper.objectmeta.StorageObjectInfo;
import com.oracle.pic.casper.volumemeta.models.StorageServerInfo;
import com.oracle.pic.casper.webserver.api.backend.putobject.storageserver.SSClientRequestFactory;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A state machine for writing an object as a sequence of chunks to replicated volumes.
 *
 * -- Overview --
 *
 * PutObjectMachine is a state machine that receives events from the environment, makes changes to its internal state
 * and then asks the environment to take action on its behalf. The events are sent to the state machine by calling
 * methods on a PutObjectMachine instance. The PutObjectMachine instance asks the environment to take actions by
 * calling methods on interfaces (PutObjectActions, HttpClientRequest, etc). The state machine is designed around the
 * event and action model of the Vert.x framework, and the rest of this documentation assumes that Vert.x is being used
 * as the "environment".
 *
 * -- How Objects Are Written --
 *
 * Objects written to Casper can be large (up to 50GB), so they are internally broken into chunks (128MB). The chunks of
 * an object are stored in separate logical volumes that are replicated on three physical storage servers. The state
 * machine is responsible for making sure that all of the following is done for each chunk:
 *
 *  1. A volume and volume object number (VON) are selected for the chunk (from the volume service).
 *  2. The metadata for the chunk is written to the database (along with the volume ID and VON) before any data is
 *     written to storage servers.
 *  3. The data for the chunk is written to at least two of the three storage servers and verified (via a SHA-256 hash).
 *  4. The metadata for the chunk is updated in the database to indicate that it is complete.
 *
 * The state machine is also responsible for making sure that the following is done for the whole object:
 *
 *  1. The metadata for the object is written to the database before any chunks are created or any data is written to
 *     storage servers.
 *  2. All of the chunks for the object are successfully written to storage servers in volumes (as above).
 *  3. The metadata for the object is updated to indicate that it is complete.
 *
 * The state machine uses a ring buffer to decouple incoming data from the customer request from outgoing data to the
 * storage servers. As data arrives from the customer request it is added to the ring buffer. Connections to storage
 * servers for each chunk maintain indexes into the ring buffer which they use to determine what to send to the storage
 * server. The ring buffer is fixed size, so it can fill if a storage server connection is moving too slowly. In that
 * case, the customer connection is "paused" (no more data is read from it, so the internal TCP buffers start filling,
 * and then the window size will start shrinking) until more room is available in the ring buffer. The ring buffer also
 * makes it possible to continue reading and writing data while concurrent database operations are in progress.
 *
 * -- Threading Model --
 *
 * The state machine assumes the threading model adopted by the Vert.x framework. In particular, it assumes:
 *
 *  A. A PutObjectMachine instance is only accessed from a single Vert.x event loop thread. This class is NOT thread
 *     safe.
 *  B. Concurrent activities are run on a Vert.x worker thread and, when they are completed, call back to the
 *     PutObjectMachine on the original event loop thread. See the documentation for PutObjectActions for more details
 *     on the requirements here.
 *
 * The PutObjectActions implementation passed to the PutObjectMachine is the only place where thread-safety is very
 * important, as it can be used from both Vert.x worker threads and the event loop thread.
 *
 * An important property of the PutObjectMachine is that it is deterministic. That means that the same sequence of
 * events sent to two different PutObjectMachine instances will result in the exact same sequence of actions.
 *
 * -- Events --
 *
 * The state machine has methods for handling the following events from Vert.x:
 *
 *  - HttpServerRequest#handler: a buffer of data has arrived from the customer request. These events are handled by the
 *    serverReqBuffer method.
 *  - HttpServerRequest#endHandler: all of the data for the request has arrived. These events are hanlded by the
 *    serverReqEnd method.
 *  - HttpServerRequest#exceptionHandler: a protocol error was encountered while reading the body of the HTTP request
 *    from the customer. These events are handled by the serverException method.
 *  - HttpServerResponse#closeHandler: the client has unexpectedly closed their connection. These events are handled by
 *    the serverResClose method.
 *  - HttpServerResponse#exceptionHandler: a protocol error was encountered while writing the HTTP response. These
 *    events are handled by the serverException method.
 *  - HttpClientRequest#continueHandler: the storage server has returned a "100 Continue" response, indicating that it
 *    is safe to start sending the body of the request. Requests to the storage server always use "Expect: 100-Continue"
 *    so we require this event to make progress. These events are handled by the clientReqContinue method.
 *  - HttpClientRequest#handler: the status and headers of the response from the storage server have arrived. This can
 *    occur at any point during the request, even if we have not yet sent all the bytes of the body, as the storage
 *    server can experience an error at any point in the request. These events are handled by the clientResArrive
 *    method.
 *  - HttpClientRequest#drainHandler: the connection to the storage server had a full write queue that has now drained,
 *    so it is safe to send more data. These events are handled by the clientReqDrain method.
 *  - HttpClientRequest#exceptionHandler: the request encountered a protocol error while sending data to the server.
 *    These events are handled by the clientException method.
 *  - HttpClientResponse#handler: a buffer of data for the storage server response has arrived. These events are handled
 *    by the clientResBuffer method.
 *  - HttpClientResponse#exceptionHandler: there was a protocol error while processing the HTTP response from the
 *    storage server. These events are handled by the clientException method.
 *
 * The state machine makes is possible for multiple chunks to be writing at the same time, so we have to identify each
 * of the storage server clients with three variables: the chunk sequence number (an integer that starts at zero),
 * the hostname of the storage server and the port of the storage server. That uniquely identifies all of the
 * HttpClientRequest and HttpClientResponse objects, and those are required arguments to the methods that handle those
 * events.
 *
 * In addition to events from Vert.x, the state machine also has methods for handling the completion of asynchronous
 * database operations. Those events are:
 *
 *  - objectAndChunkBegan: this event is sent when the volume ID and VON have been selected for the first chunk, and
 *    the metadata for the object and the first chunk have been durably written to the database. This event can only
 *    arrive after the state machine has called PutObjectActions#beginObjectAndChunk.
 *  - chunkBegan: this event is sent when the volume ID and VON have been selected for the Nth chunk (where N > 1)
 *    and the metadata for the chunk has been durably written to the database. This event can only arrive after the
 *    state machine has called PutObjectActions#beginChunk.
 *  - chunkEnded: this event is sent when the metadata for the chunk has been updated in the database to indicate that
 *    it was successfully written. This event can only arrive after the state machine has called
 *    PutObjectActions#endChunk.
 *  - objectEnded: this event is sent when the metadata for the object has been updated in the database to indicate that
 *    it was successfully written. This event can only arrive after the state machine has called
 *    PutObjectActions#endObject.
 *  - actionException: this event is sent when any asynchronous database operation has failed, and can arrive after any
 *    of the PutObjectActions methods have been called. In all cases, the failure of one of these operations triggers
 *    the failure of the entire request.
 *
 * The expected flow of control is that the state machine will call the enabling method (as noted above), which will
 * trigger an asynchronous operation on a Vert.x worker thread. When that operation completes, it will return to the
 * Vert.x event loop and call the associated event method on the state machine. This will typically be done with the
 * "Vertx#executeBlocking" method.
 *
 * Finally, the state machine has methods for handling timeouts:
 *
 *  - clientTimeout: this event is sent when a timeout occurs for a single storage server client. These events will
 *    always cause that request to fail, but may not cause the overall request to fail (as we only require two out of
 *    three clients for a given chunk to succeed).
 *
 * -- Actions --
 *
 * The state machine may call the following action methods on Vert.x objects:
 *
 *  - HttpServerRequest#pause: called when the ring buffer is full and we can no longer accept data from the connection.
 *  - HttpServerRequest#resume: called when the ring buffer is no longer full.
 *  - HttpClientRequest#write: called to write data from the ring buffer to the storage server.
 *  - HttpClientRequest#writeQueueFull: called to check if the client can accept more writes.
 *  - HttpClientRequest#end: called when all of the data for a chunk has been written to the storage server.
 *  - HttpClientRequest#connection#close: called when the connection is in a bad state and must be closed.
 *
 * The state machine also makes use of the PutObjectActions interface to start asynchronous operations and timers. The
 * documentation for those classes covers the details of what each operation does and how they are expected to interact
 * with the state machine.
 *
 * -- Timers --
 *
 * The state machine uses timers to enforce an upper-bound on the latency of the following operations:
 *
 *  1. The database operations each have a timeout. These are entirely handled in PutObjectActionsImpl.
 *  2. The customer request has an idle timeout which measures the time since the last bytes of data arrived
 *     for the body of the HTTP request. In the future, to prevent abuse, we may also need a minimum throughput, to
 *     prevent malicious users from sending one byte in every idle timeout period to keep the connection alive.
 *  3. Storage server clients have a timeout that measures the time from when the request is made until a
 *     "100 Continue" or error response is received from the storage server.
 *  4. If the request to a storage server has a full queue, there is a timeout that measures the time until the queue
 *     is drained. In the future, we may also want a minimum throughput here.
 *  5. Storage server clients have a timeout that measures the time from when the last byte of data was sent until the
 *     response is received.
 *
 * -- Buffer Processing and Validation --
 *
 * The state machine uses the PutObjectActions interface to process data from the customer as it arrives. This is used
 * for things like MD5 computation and encryption, and depends on the caller. The PutObjectActions documentation
 * provides details on exactly what is expected of implementations. The data received from the customer is processed
 * by the PutObjectActions#processBuffer method before it is sent to storage servers.
 *
 * The state machine internally tracks the following checksums:
 *
 *  - An MD5 checksum for the entire object that is computed on the data sent from the customer.
 *  - A SHA-256 checksum for each chunk which is computed on the buffers processed by PutObjectActions#processBuffer
 *    (which is the data sent to the storage servers).
 *
 * These checksums are used to validate the data sent to the storage servers and they are passed to the operations that
 * update the metadata database.
 *
 * -- Storage Server HTTP Protocol --
 *
 * The storage server HTTP protocol is documented in the SSClientRequestFactory class docs. In addition to what is
 * documented there, the state machine assumes that a storage server will never return a 200 OK response before it has
 * received all of the bytes of the request. Addressing that case adds more state to the state machine, and complicates
 * the logic. It should be impossible for a storage server to return the correct SHA-256 before it has received all of
 * the bytes, so even if a storage server violates this behavior, due to a bug, we will still reject the request after
 * checking the SHA-256.
 *
 * Separately, the state machine always sends a Content-Length header, and will never use "Transfer-Encoding: Chunked".
 *
 * -- Web Server HTTP Protocol --
 *
 * The state machine assumes that the HTTP request from the customer has a Content-Length. The logic here does not
 * support "Transfer-Encoding: Chunked", and it is a large change to add support.
 *
 * We allow customers to send us zero-byte objects, and we currently create a single chunk for those objects and send
 * the "data" to a volume. That mirrors the behavior of the code being replaced by this, but it's something we should
 * revisit in the future.
 *
 * -- Return Values and Responses --
 *
 * The state machine does not send the response back to the customer, as the response can differ by API (S3, Swift,
 * Casper native). Instead, it returns a CompletableFuture when the "start()" method is called. That future will be
 * completed with either a StorageObjectInfo, or with a PutObjectException. If there is an exception, the
 * PutObjectException#isConnectionClean() method will return false if the caller is expected to close the
 * server connection after responding to the customer. In this case, the state machine believes that there are still
 * un-read bytes on the connection, and that it is unsuitable for re-use.
 *
 * Note that all of the public methods of this class are guaranteed to *never* throw an exception. If a method would
 * have thrown an exception (generally due to a bug), it will abort the request and complete the CompletableFuture with
 * the exception. The alternative would be to require that the caller of these methods handle the exceptions, which is
 * both difficult and unnecessary. Note that the public methods were designed to be called from the Vert.x event loop.
 * When a Vert.x callback throws an exception Vert.x simply logs it and continues with the request, which can cause
 * small issues to become worse. This approach shuts down the request as soon as it detects that something unexpected
 * is happening.
 *
 * TODO: need to check the generation numbers coming back from storage servers.
 * TODO: find a way to send the SHA-256 down to the storage server
 * TODO: add a mode that dumps "core" when the request fails, especially for an unexpected reason
 */
public final class PutObjectMachine {
    private static final Logger LOG = LoggerFactory.getLogger(PutObjectMachine.class);

    private final PutObjectActions actions;
    private final SSClientRequestFactory ssClientReqFactory;
    private final int maxChunkSize;
    private final List<ChunkState> chunks;
    private final RingBuffer<Buffer, SSClientMachine> ring;
    private final CompletableFuture<StorageObjectInfo> future;
    private final HttpServerRequest serverReq;
    private final long contentLen;
    @Nullable
    private final Digest expectedDigest;
    private final ObjectMapper mapper;
    private final String requestID;

    private final MessageDigest digestMD5;

    private boolean paused;
    private boolean aborted;

    private int bytesReceived;
    private boolean endObjectActionStarted;
    private boolean endObjectActionCompleted;

    public PutObjectMachine(PutObjectActions actions,
                            SSClientRequestFactory ssClientReqFactory,
                            int maxChunkSize,
                            int ringBufferSize,
                            HttpServerRequest serverReq,
                            long contentLen,
                            @Nullable Digest expectedDigest,
                            ObjectMapper mapper,
                            String requestID) {
        Preconditions.checkNotNull(actions);
        Preconditions.checkNotNull(ssClientReqFactory);
        Preconditions.checkArgument(maxChunkSize > 0);
        Preconditions.checkArgument(ringBufferSize >= 2);
        Preconditions.checkArgument(contentLen >= 0);

        this.actions = Preconditions.checkNotNull(actions);
        this.ssClientReqFactory = Preconditions.checkNotNull(ssClientReqFactory);
        this.maxChunkSize = maxChunkSize;
        this.serverReq = Preconditions.checkNotNull(serverReq);
        this.contentLen = contentLen;
        this.expectedDigest = expectedDigest;
        this.mapper = Preconditions.checkNotNull(mapper);
        this.requestID = Preconditions.checkNotNull(requestID);

        digestMD5 = DigestUtils.messageDigestFromAlgorithm(DigestAlgorithm.MD5);

        chunks = new ArrayList<>();
        ring = new RingBuffer<>(ringBufferSize);
        future = new CompletableFuture<>();
        paused = false;
        aborted = false;
        bytesReceived = 0;
        endObjectActionStarted = false;
        endObjectActionCompleted = false;
    }

    public CompletableFuture<StorageObjectInfo> start() {
        try {
            Preconditions.checkState(!aborted);
            LOG.trace("[{}] start", requestID);

            final ChunkState chunk = createChunk();
            Verify.verify(chunk.getChunkNum() == 0);
            actions.beginObjectAndChunk(chunk.getLength(), this);
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'start' method", ex);
        }

        return future;
    }

    public void objectAndChunkBegan(VolumeReplicaMetadata volRepMeta) {
        try {
            LOG.trace("[{}] objectAndChunkBegan", requestID);

            if (!aborted) {
                final ChunkState chunk = getChunk(0);
                Verify.verify(chunk.getClients().isEmpty());
                connectChunkClients(0, chunk, volRepMeta);
            }
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'objectAndChunkBegan' method", ex);
        }
    }

    public void chunkBegan(int chunkNum, VolumeReplicaMetadata volRepMeta) {
        try {
            LOG.trace("[{}] chunkBegan({})", requestID, chunkNum);

            if (!aborted) {
                final ChunkState chunk = chunks.get(chunkNum);
                Verify.verify(chunk.getClients().isEmpty());
                connectChunkClients(chunkNum, chunk, volRepMeta);
            }
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'chunkBegan' method", ex);
        }
    }

    public void chunkEnded(int chunkNum) {
        try {
            LOG.trace("[{}] chunkEnded({})", requestID, chunkNum);

            if (!aborted) {
                final ChunkState chunk = getChunk(chunkNum);
                chunk.endChunkCompleted();

                // Check to see if all chunks are done
                if (bytesReceived == contentLen) {
                    Verify.verify(!endObjectActionStarted);
                    if (chunks.stream().allMatch(ChunkState::isComplete)) {
                        final Digest computedDigest = new Digest(DigestAlgorithm.MD5, digestMD5.digest());
                        if (expectedDigest != null && !expectedDigest.equals(computedDigest)) {
                            // TODO: Fix these messages
                            abortRequest("The MD5 digests do not match", new InvalidDigestException("NO MATCH"));
                        } else {
                            endObjectActionStarted = true;
                            actions.endObject(computedDigest, this);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'chunkEnded' method", ex);
        }
    }

    public void objectEnded(StorageObjectInfo ssInfo) {
        try {
            LOG.trace("[{}] objectEnded", requestID);

            if (!aborted) {
                Verify.verify(endObjectActionStarted && !endObjectActionCompleted);
                endObjectActionCompleted = true;
                future.complete(ssInfo);
            }
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'objectEnded' method", ex);
        }
    }

    public void actionException(String actionName, Throwable throwable) {
        LOG.trace("[{}] actionException", requestID, throwable);
        abortRequest("The asynchronous action '" + actionName + "' failed, and the request will be aborted", throwable);
    }

    public void serverReqBuffer(Buffer buffer) {
        try {
            LOG.trace("[{}] serverReqBuffer", requestID);

            if (aborted) {
                return;
            }

            Preconditions.checkArgument(buffer.length() <= maxChunkSize);
            Preconditions.checkArgument(bytesReceived + buffer.length() <= contentLen);
            Verify.verify(!paused);
            Verify.verify(ring.remainingCapacity() >= 2);
            Verify.verify(chunks.size() > 0);

            digestMD5.update(buffer.getByteBuf().nioBuffer());
            bytesReceived += buffer.length();

            final ChunkState curChunk = chunks.get(chunks.size() - 1);

            if (curChunk.getBytesReceived() + buffer.length() <= curChunk.getLength()) {
                buffer = actions.processBuffer(curChunk.getChunkNum(), buffer);
                ring.add(buffer);
                curChunk.recvBuffer(buffer);

                if (curChunk.getBytesReceived() == curChunk.getLength() && bytesReceived < contentLen) {
                    final ChunkState chunk = createChunk();
                    actions.beginChunk(chunk.getChunkNum(), chunk.getLength(), this);
                }
            } else {
                final int remaining = curChunk.getLength() - curChunk.getBytesReceived();
                final Buffer curBuf = actions.processBuffer(curChunk.getChunkNum(), buffer.getBuffer(0, remaining));

                ring.add(curBuf);
                curChunk.recvBuffer(curBuf);

                final ChunkState newChunk = createChunk();
                actions.beginChunk(newChunk.getChunkNum(), newChunk.getLength(), this);

                final Buffer newBuf = actions.processBuffer(
                        newChunk.getChunkNum(), buffer.getBuffer(remaining, buffer.length()));
                ring.add(newBuf);
                newChunk.recvBuffer(newBuf);
            }

            if (bytesReceived < contentLen && ring.remainingCapacity() < 2) {
                paused = true;
                serverReq.pause();
            }

            for (int i = 0; i < chunks.size(); i++) {
                final ChunkState chunk = chunks.get(i);
                for (Map.Entry<HostAndPort, SSClientMachine> entry : chunk.getClients().entrySet()) {
                    writeToClient(i, entry.getKey(), entry.getValue());
                }
            }
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'serverReqBuffer' method", ex);
        }
    }

    public void serverReqEnd() {
        try {
            LOG.trace("[{}] serverReqEnd", requestID);

            if (!aborted) {
                Verify.verify(!paused);
                final ChunkState curChunk = chunks.get(chunks.size() - 1);
                Verify.verify(curChunk.getLength() == curChunk.getBytesReceived());
            }
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'serverReqEnd' method", ex);
        }
    }

    public void serverException(Throwable throwable) {
        LOG.trace("[{}] serverException", requestID, throwable);
        abortRequest("The customer request had a protocol failure and will be aborted", throwable);
    }

    public void serverResClose() {
        LOG.trace("[{}] serverResClose", requestID);
        abortRequest("The customer unexpectedly closed the connection", null);
    }

    public void clientReqContinue(int chunkNum, HostAndPort hostAndPort) {
        try {
            Verify.verify(!aborted);
            LOG.trace("[{}] clientReqContinue({},{})", requestID, chunkNum, hostAndPort);

            final ChunkState chunk = getChunk(chunkNum);
            final SSClientMachine client = getClient(chunk, hostAndPort);
            Verify.verify(!client.isClosed());

            actions.stopClientTimer(chunkNum, hostAndPort, PutObjectActions.ClientTimer.CONTINUE, this);

            client.clientReqContinue();
            if (contentLen == 0) {
                actions.startClientTimer(chunkNum, hostAndPort, PutObjectActions.ClientTimer.RESPONSE, this);
            } else {
                writeToClient(chunkNum, hostAndPort, client);
            }
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'clientReqContinue' method", ex);
        }
    }

    public void clientReqDrain(int chunkNum, HostAndPort hostAndPort) {
        try {
            Verify.verify(!aborted);
            LOG.trace("[{}] clientReqDrain({},{})", requestID, chunkNum, hostAndPort);

            final ChunkState chunk = getChunk(chunkNum);
            final SSClientMachine client = getClient(chunk, hostAndPort);
            Verify.verify(!client.isClosed());

            actions.stopClientTimer(chunkNum, hostAndPort, PutObjectActions.ClientTimer.DRAIN, this);
            writeToClient(chunkNum, hostAndPort, client);
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'clientReqDrain' method", ex);
        }
    }

    public void clientException(int chunkNum, HostAndPort hostAndPort, Throwable throwable) {
        try {
            LOG.trace("[{}] clientException({},{})", requestID, chunkNum, hostAndPort, throwable);

            if (!aborted) {
                final ChunkState chunk = getChunk(chunkNum);
                final SSClientMachine client = getClient(chunk, hostAndPort);

                abortClientRequest(chunk, client, throwable);
            }
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'clientException' method", ex);
        }
    }

    public void clientTimeout(int chunkNum, HostAndPort hostAndPort, PutObjectActions.ClientTimer timer) {
        try {
            LOG.trace("[{}] clientTimeout({},{},{})", requestID, chunkNum, hostAndPort, timer.name());

            if (!aborted) {
                final ChunkState chunk = getChunk(chunkNum);
                final SSClientMachine client = getClient(chunk, hostAndPort);

                abortClientRequest(chunk, client, null);
            }
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'clientTimeout' method", ex);
        }
    }

    public void clientResArrive(int chunkNum, HostAndPort hostAndPort, HttpClientResponse clientRes) {
        try {
            Verify.verify(!aborted);
            LOG.trace("[{}] clientResArrive({},{})", requestID, chunkNum, hostAndPort);

            final ChunkState chunk = getChunk(chunkNum);
            final SSClientMachine client = getClient(chunk, hostAndPort);
            Verify.verify(!client.isClosed());

            if (!client.isContinued()) {
                actions.stopClientTimer(chunkNum, hostAndPort, PutObjectActions.ClientTimer.CONTINUE, this);
            } else {
                actions.stopClientTimer(chunkNum, hostAndPort, PutObjectActions.ClientTimer.RESPONSE, this);
            }

            client.clientResArrive(clientRes);
            ring.unregisterReader(client);
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'clientResArrive' method", ex);
        }
    }

    public void clientResBuffer(int chunkNum, HostAndPort hostAndPort, Buffer buffer) {
        try {
            Verify.verify(!aborted);
            LOG.trace("[{}] clientResBuffer({},{})", requestID, chunkNum, hostAndPort);

            final ChunkState chunk = getChunk(chunkNum);
            final SSClientMachine client = getClient(chunk, hostAndPort);
            Verify.verify(!client.isClosed());

            client.clientResBuffer(buffer, chunk.getDigestBase64());

            if (client.getStatus() == SSClientMachine.Status.SUCCEEDED &&
                    !chunk.isEndChunkStarted() && chunk.isSuccess()) {
                actions.endChunk(chunkNum, chunk.getDigest(), chunk.getLength(), this);
                chunk.endChunkStarted();
            } else if (client.getStatus() == SSClientMachine.Status.FAILED && chunk.isFailed()) {
                abortRequest("TODO", null);
            }
        } catch (Exception ex) {
            abortRequest("Unexpected exception in 'clientResBuffer' method", ex);
        }
    }

    private ChunkState getChunk(int chunkNum) {
        Preconditions.checkArgument(chunkNum < chunks.size(), "The chunk number was not a valid chunk");
        return chunks.get(chunkNum);
    }

    private SSClientMachine getClient(ChunkState chunk, HostAndPort hostAndPort) {
        final SSClientMachine client = chunk.getClients().get(hostAndPort);
        Verify.verifyNotNull(client);
        return client;
    }

    private ChunkState createChunk() {
        int chunksSize = chunks.isEmpty() ? 0 : chunks.stream().mapToInt(ChunkState::getLength).sum();
        int length = Math.min((int) (contentLen - chunksSize), maxChunkSize);

        final ChunkState chunk = new ChunkState(chunks.size(), length);

        chunks.add(chunk);
        ring.registerReader(chunk.getBookmark());

        return chunk;
    }

    private void connectChunkClients(int chunkNum, ChunkState chunk, VolumeReplicaMetadata volRepMeta) {
        chunk.setClients(new HashMap<>(volRepMeta.getStorageServers().size()));

        for (StorageServerInfo serverInfo : volRepMeta.getStorageServers()) {
            final HostAndPort hostAndPort = HostAndPort.fromParts(serverInfo.getHostName(), serverInfo.getPort());
            final HttpClientRequest clientReq = ssClientReqFactory.putObject(
                    hostAndPort, volRepMeta.getVolumeId(), volRepMeta.getVon(), chunk.getLength());

            clientReq.continueHandler(v -> clientReqContinue(chunkNum, hostAndPort));
            clientReq.handler(clientRes -> {
                clientRes.handler(buffer -> clientResBuffer(chunkNum, hostAndPort, buffer));
                clientRes.exceptionHandler(throwable -> clientException(chunkNum, hostAndPort, throwable));
                clientResArrive(chunkNum, hostAndPort, clientRes);
            });
            clientReq.drainHandler(v -> clientReqDrain(chunkNum, hostAndPort));
            clientReq.exceptionHandler(throwable -> clientException(chunkNum, hostAndPort, throwable));

            clientReq.sendHead();

            final SSClientMachine client = new SSClientMachine(
                    mapper, clientReq, requestID, hostAndPort, chunk.getLength());

            actions.startClientTimer(chunkNum, hostAndPort, PutObjectActions.ClientTimer.CONTINUE, this);
            chunk.getClients().put(hostAndPort, client);
            ring.registerReader(client, chunk.getBookmark());
        }

        ring.unregisterReader(chunk.getBookmark());
    }

    private void writeToClient(int chunkNum, HostAndPort hostAndPort, SSClientMachine client) {
        if (client.getBytesSent() == client.getContentLength()) {
            return;
        }

        if (client.isWritable()) {
            do {
                final Buffer buf = ring.poll(client);
                if (buf == null) {
                    break;
                }

                client.serverReqBuffer(buf);
            } while (!client.isQueueFull() && client.getBytesSent() < client.getContentLength());
        }

        if (client.getBytesSent() == client.getContentLength()) {
            actions.startClientTimer(chunkNum, hostAndPort, PutObjectActions.ClientTimer.RESPONSE, this);
            ring.unregisterReader(client);
        } else if (client.isQueueFull()) {
            actions.startClientTimer(chunkNum, hostAndPort, PutObjectActions.ClientTimer.DRAIN, this);
        }

        if (paused && ring.remainingCapacity() >= 2) {
            paused = false;
            serverReq.resume();
        }
    }

    /**
     * Abort the request, cleaning up all resources and connections.
     *
     * It is very important that this method *never* throw an exception.
     */
    private void abortRequest(String message, @Nullable Throwable throwable) {
        if (aborted) {
            return;
        }

        for (ChunkState chunk : chunks) {
            try {
                chunk.getClients().values().forEach(SSClientMachine::close);
            } catch (Exception ex) {
                LOG.warn("[{}] Unexpected exception while closing a client connection", requestID, ex);
            }
        }

        try {
            ring.clear();
            actions.abortObject();
        } catch (Exception ex) {
            LOG.warn("[{}] Unexpected exception while aborting a request", requestID, ex);
        }

        aborted = true;
        future.completeExceptionally(new PutObjectException(message, throwable, bytesReceived == contentLen));
    }

    private void abortClientRequest(ChunkState chunk, SSClientMachine client, @Nullable Throwable throwable) {
        if (client.isClosed()) {
            return;
        }

        ring.unregisterReader(client);

        client.close();
        if (chunk.isFailed()) {
            abortRequest("TODO", throwable);
        }
    }
}
