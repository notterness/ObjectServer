package com.oracle.pic.casper.webserver.api.backend.putobject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.encryption.DigestUtils;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.BucketKey;
import com.oracle.pic.casper.objectmeta.ChecksumType;
import com.oracle.pic.casper.objectmeta.ObjectId;
import com.oracle.pic.casper.objectmeta.ObjectKey;
import com.oracle.pic.casper.objectmeta.StorageObjectInfo;
import com.oracle.pic.casper.volumemeta.models.HostStatus;
import com.oracle.pic.casper.volumemeta.models.StorageServerInfo;
import com.oracle.pic.casper.webserver.api.backend.putobject.storageserver.ErrorResponse;
import com.oracle.pic.casper.webserver.api.backend.putobject.storageserver.PutObjectResponse;
import com.oracle.pic.casper.webserver.api.backend.putobject.storageserver.SSClientRequestFactory;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.impl.SocketAddressImpl;
import org.assertj.core.api.Assertions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

/**
 * Unit tests for the PutObjectMachine.
 *
 * PutObjectMachine is a state machine that accepts events and calls out to actions through interfaces. These tests
 * simulate sequences of events and check that the machine calls the expected actions. The following shorthand is used
 * to refer to events:
 *
 *  - STR: the "start" event, always called.
 *  - SRB: a "serverRequestBuffer" event, which has the following variants:
 *    - SRBp: a buffer that partially fills the current chunk.
 *    - SRBf: a buffer that exactly fills the current chunk.
 *    - SRBo: a buffer that overfills the current chunk.
 *    - SRBwxy: a buffer where w in [p,f,o] (with meaning as above) and client with chunkNum x and replica y is full.
 *  - SRX: the "serverException" event.
 *  - SSC: the "serverResClose" event.
 *  - OCB: the "objectAndChunkBegan" event.
 *  - AEX: the "actionException" event.
 *  - GTO: the "globalTimeout" event.
 *  - OED: the "objectEnded" event.
 *  - CBGx: the "chunkBegan" event for chunk x.
 *  - CEDx: the "chunkEnded" event for chunk x.
 *  - CRCxy: the "clientRequestContinue" event for chunkNum x, replica y.
 *  - CRDxy: the "clientRequestDrain" event for chunkNum x, replica y.
 *  - CSAxy: the "clientResponseArrived" event for chunkNum x, replica y.
 *  - CSBxy: the "clientResponseBuffer" event for chunkNum x, replica y.
 *  - CEXxy: the "clientException" event for chunkNum x, replica y.
 *  - CTOxy: the "clientTimeout" event for chunkNum x, replica y.
 *
 * The unit tests are named for the sequence of events under test, for example:
 *
 *  test_STR_SRBp_SRBf()
 *
 * is a test that will execute "start()", followed by "serverRequestBuffer" twice, once partially filling the current
 * chunk, the second time exactly filling it.
 *
 * Here is a list of the actions that can be taken by the PutObjectMachine:
 *
 *  - HttpServerRequest#pause
 *  - HttpServerRequest#resume
 *
 *  - HttpServerResponse#setStatusCode
 *  - HttpServerResponse#end
 *
 *  - HttpClientRequest#write
 *  - HttpClientRequest#end
 *  - HttpClientRequest#writeQueueFull
 *
 *  - PutObjectActions#beginObjectAndChunk
 *  - PutObjectActions#beginChunk
 *  - PutObjectActions#endChunk
 *  - PutObjectActions#endObject
 *
 * The tests use Mockito as a "spy" to verify that the expected actions are taken in response to the events.
 *
 * The goal of these tests is to achieve 100% statement coverage for PutObjectMachine and SSClientMachine. These tests,
 * by necessity, can only cover a tiny percentage of all the possible orderings of events. The PutObjectMachineChecker
 * attempts to cover a larger subset of those orderings in an automated fashion. The integration of the machine with
 * Vert.x is tested in PutObjectMachineIT.
 */
public final class PutObjectMachineTest {

    private static int DEFAULT_RING_SIZE = 100;

    // The default is to have exactly two chunks. In many of the tests below, the DEFAULT_MAX_CHUNK_SIZE is used as the
    // content-length of the request to simulate an object with exactly one chunk.
    private static int DEFAULT_MAX_CHUNK_SIZE = 4;
    private static int DEFAULT_CONTENT_LEN = 2 * DEFAULT_MAX_CHUNK_SIZE;

    private static final class MockClientResponse {
        final HttpClientResponse clientRes;
        final Buffer clientResBody;

        MockClientResponse(ObjectMapper mapper, int status, PutObjectResponse response) {
            clientRes = mock(HttpClientResponse.class);
            try {
                clientResBody = Buffer.buffer(mapper.writeValueAsBytes(response));
            } catch (JsonProcessingException ex) {
                throw new RuntimeException(ex);
            }

            when(clientRes.statusCode()).thenReturn(status);
            when(clientRes.getHeader(HttpHeaders.CONTENT_LENGTH)).thenReturn(Integer.toString(clientResBody.length()));
        }

        MockClientResponse(ObjectMapper mapper, int status, ErrorResponse response) {
            clientRes = mock(HttpClientResponse.class);
            try {
                clientResBody = Buffer.buffer(mapper.writeValueAsBytes(response));
            } catch (JsonProcessingException ex) {
                throw new RuntimeException(ex);
            }

            when(clientRes.statusCode()).thenReturn(status);
            when(clientRes.getHeader(HttpHeaders.CONTENT_LENGTH)).thenReturn(Integer.toString(clientResBody.length()));
        }
    }

    private static MockClientResponse createSuccessResponse(ObjectMapper mapper, Buffer buffer) {
        return new MockClientResponse(mapper, HttpResponseStatus.CREATED,
                new PutObjectResponse(buffer.length(), sha256Base64(buffer), DigestAlgorithm.SHA256.name(), 1));
    }

    private static MockClientResponse createErrorResponse(ObjectMapper mapper) {
        return new MockClientResponse(
                mapper, HttpResponseStatus.INTERNAL_SERVER_ERROR, new ErrorResponse("", "", "", "", ""));
    }

    private static Digest digestSHA256(Buffer buffer) {
        final MessageDigest digest = DigestUtils.messageDigestFromAlgorithm(DigestAlgorithm.SHA256);
        digest.update(buffer.getBytes());
        return new Digest(DigestAlgorithm.SHA256, digest.digest());
    }

    private static Digest digestMD5(Buffer... buffers) {
        final MessageDigest digest = DigestUtils.messageDigestFromAlgorithm(DigestAlgorithm.MD5);
        for (Buffer buffer : buffers) {
            digest.update(buffer.getBytes());
        }
        return new Digest(DigestAlgorithm.MD5, digest.digest());
    }

    private static String sha256Base64(Buffer buffer) {
        try {
            return Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(buffer.getBytes()));
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final class Mocks {
        final PutObjectActions actions;
        final SSClientRequestFactory factory;
        final HttpServerRequest serverReq;
        final Map<HostAndPort, HttpClientRequest> clientReqs;

        Mocks() {
            clientReqs = new HashMap<>();
            actions = mock(PutObjectActions.class);
            when(actions.processBuffer(anyInt(), any())).then(invocation -> {
                Object[] args = invocation.getArguments();
                return args[1];
            });
            factory = (hostAndPort, volId, von, length) -> {
                HttpClientRequest clientReq = mock(HttpClientRequest.class);
                HttpConnection conn = mock(HttpConnection.class);
                when(clientReq.connection()).thenReturn(conn);
                clientReqs.put(hostAndPort, clientReq);
                return clientReq;
            };
            serverReq = mock(HttpServerRequest.class);
            HttpConnection conn = mock(HttpConnection.class);
            when(serverReq.connection()).thenReturn(conn);
            when(conn.remoteAddress()).thenReturn(new SocketAddressImpl(0, "remotehost"));
            when(conn.localAddress()).thenReturn(new SocketAddressImpl(0, "localhost"));
        }

        HttpClientRequest getRequest(int chunkNum, int replica) {
            return clientReqs.get(HostAndPort.fromParts("localhost", port(chunkNum, replica)));
        }
    }

    private static int port(int chunkNum, int replica) {
        return chunkNum * 10 + replica;
    }

    private static int controlPort(int chunkNum, int replica) {
        return chunkNum * 100 + replica;
    }

    private static HostAndPort hostAndPort(int chunkNum, int replica) {
        return HostAndPort.fromParts("localhost", port(chunkNum, replica));
    }

    private static Buffer buffer(int size) {
        return buffer(size, "x");
    }

    private static Buffer buffer(int size, String c) {
        return Buffer.buffer(String.format("%1$" + size + "s", c));
    }

    private StorageObjectInfo createStorageObjectInfo(long length) {
        return new StorageObjectInfo(new ObjectKey(new BucketKey("ns", "bucket", Api.V2), "obj"),
                new ObjectId("objid"),
                "etag",
                new Date(),
                new Date(),
                length,
                "md5",
                new HashMap<>(),
                null,
                ChecksumType.MD5,
                1,
                null,
                null,
                null,
                ArchivalState.NotApplicable);
    }

    private VolumeReplicaMetadata createVolRepMeta(int chunkNum) {
        final int ssId = 1;
        final int volId = 1;
        final int von = 1;

        final List<StorageServerInfo> servers = new ArrayList<>(3);
        servers.add(new StorageServerInfo(
                "ad1", ssId, "localhost", port(chunkNum, 0), controlPort(chunkNum, 0), HostStatus.LIVE));
        servers.add(new StorageServerInfo(
                "ad1", ssId, "localhost", port(chunkNum, 1), controlPort(chunkNum, 1), HostStatus.LIVE));
        servers.add(new StorageServerInfo(
                "ad1", ssId, "localhost", port(chunkNum, 2), controlPort(chunkNum, 2), HostStatus.LIVE));
        return new VolumeReplicaMetadata(volId, von, servers);
    }

    private PutObjectMachine createMachine(Mocks mocks, int maxChunkSize, int ringSize, long contentLen) {
        return new PutObjectMachine(mocks.actions, mocks.factory, maxChunkSize, ringSize, mocks.serverReq, contentLen,
                null, mapper, "test");
    }

    private void assertPutObjectMachineException(CompletableFuture<StorageObjectInfo> future, boolean clean) {
        Assertions.assertThat(future.isCompletedExceptionally()).isTrue();
        try {
            future.get();
        } catch (Exception ex) {
            Assertions.assertThat(ex).isInstanceOf(ExecutionException.class);
            Assertions.assertThat(ex).hasCauseInstanceOf(PutObjectException.class);

            PutObjectException pomex = (PutObjectException) ex.getCause();
            if (clean) {
                Assertions.assertThat(pomex.isServerConnClean()).isTrue();
            } else {
                Assertions.assertThat(pomex.isServerConnClean()).isFalse();
            }
        }
    }

    private void verifyClientRequestCreated(HttpClientRequest req) {
        verify(req).continueHandler(any());
        verify(req).handler(any());
        verify(req).drainHandler(any());
        verify(req).exceptionHandler(any());
        verify(req).sendHead();
    }

    private void verifyStartClientTimers(Mocks mocks, PutObjectMachine machine) {
        verifyStartClientTimers(mocks, 0, machine);
    }

    private void verifyStartClientTimers(Mocks mocks, int chunkNum, PutObjectMachine machine) {
        verify(mocks.actions).startClientTimer(
                chunkNum, hostAndPort(chunkNum, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.actions).startClientTimer(
                chunkNum, hostAndPort(chunkNum, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.actions).startClientTimer(
                chunkNum, hostAndPort(chunkNum, 2), PutObjectActions.ClientTimer.CONTINUE, machine);
    }

    private ObjectMapper mapper;

    @BeforeClass
    public void beforeClass() {
        mapper = new ObjectMapper();
    }

    /**
     * Check that the database activity to begin the object and chunk is started.
     */
    @Test
    public void test_STR() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);
        CompletableFuture<StorageObjectInfo> future = machine.start();

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);

        verifyNoMoreInteractions(mocks.actions);

        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * Have the server connection raise an exception and check that the request is closed.
     */
    @Test
    public void test_STR_SRX() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);
        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.serverException(new RuntimeException("test"));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verify(mocks.actions).abortObject();

        verifyNoMoreInteractions(mocks.actions);
        verifyZeroInteractions(mocks.serverReq);
        assertPutObjectMachineException(future, false /* clean */);
    }

    /**
     * Send a single buffer that partially fills the first chunk.
     */
    @Test
    public void test_STR_SRBp() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);
        CompletableFuture<StorageObjectInfo> future = machine.start();
        Buffer buffer = buffer(DEFAULT_MAX_CHUNK_SIZE - 1);
        machine.serverReqBuffer(buffer);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verify(mocks.actions).processBuffer(0, buffer);

        verifyNoMoreInteractions(mocks.actions);
        Assertions.assertThat(mocks.clientReqs).isEmpty();
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * Send a single buffer that exactly fills the first chunk (but doesn't finish the whole object). Check that the
     * second chunk is created.
     */
    @Test
    public void test_STR_SRBf() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);
        CompletableFuture<StorageObjectInfo> future = machine.start();
        Buffer buffer = buffer(DEFAULT_MAX_CHUNK_SIZE);
        machine.serverReqBuffer(buffer);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verify(mocks.actions).processBuffer(0, buffer);
        verify(mocks.actions).beginChunk(1, DEFAULT_MAX_CHUNK_SIZE, machine);

        verifyNoMoreInteractions(mocks.actions);
        Assertions.assertThat(mocks.clientReqs).isEmpty();
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * Send a buffer that partially fills the first chunk, then a buffer that fills the first chunk and part of the
     * second. Check that the second chunk is created.
     */
    @Test
    public void test_STR_SRBp_SRBo() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        Buffer buffer0 = buffer(DEFAULT_MAX_CHUNK_SIZE - 1);
        Buffer buffer1 = buffer(DEFAULT_MAX_CHUNK_SIZE - 2);
        machine.serverReqBuffer(buffer0);
        machine.serverReqBuffer(buffer1);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verify(mocks.actions).processBuffer(0, buffer0);
        verify(mocks.actions).processBuffer(0, buffer1.getBuffer(0, 1));
        verify(mocks.actions).processBuffer(1, buffer1.getBuffer(1, buffer1.length()));
        verify(mocks.actions).beginChunk(1, DEFAULT_MAX_CHUNK_SIZE, machine);

        verifyNoMoreInteractions(mocks.actions);
        Assertions.assertThat(mocks.clientReqs).isEmpty();
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * Send a buffer that exactly fills the first chunk, then a second buffer that partially fills the second chunk.
     * Check that the second chunk is created.
     */
    @Test
    public void test_STR_SRBf_SRBp() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        Buffer buffer0 = buffer(DEFAULT_MAX_CHUNK_SIZE);
        machine.serverReqBuffer(buffer0);
        Buffer buffer1 = buffer(DEFAULT_MAX_CHUNK_SIZE - 1);
        machine.serverReqBuffer(buffer1);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verify(mocks.actions).processBuffer(0, buffer0);
        verify(mocks.actions).processBuffer(1, buffer1);
        verify(mocks.actions).beginChunk(1, DEFAULT_MAX_CHUNK_SIZE, machine);

        verifyNoMoreInteractions(mocks.actions);
        Assertions.assertThat(mocks.clientReqs).isEmpty();
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * Send a buffer that partially fills the chunk and completely fills the ring buffer, checking that the server
     * request is paused.
     */
    @Test
    public void test_STR_SRBp_RingFull() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, 2, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        Buffer buffer = buffer(DEFAULT_MAX_CHUNK_SIZE - 1);
        machine.serverReqBuffer(buffer);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verify(mocks.actions).processBuffer(0, buffer);
        verify(mocks.serverReq).pause();

        verifyNoMoreInteractions(mocks.actions);
        Assertions.assertThat(mocks.clientReqs).isEmpty();
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * The object and first chunk are done, check that clients are created.
     */
    @Test
    public void test_STR_OCB() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);
        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(mocks.actions);
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A buffer is written that partially fills the chunk, check that the clients do not write it out (as they have not
     * yet been continued).
     */
    @Test
    public void test_STR_OCB_SRBp() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer = buffer(DEFAULT_MAX_CHUNK_SIZE - 1);
        machine.serverReqBuffer(buffer);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verify(mocks.actions).processBuffer(0, buffer);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A storage server returns an error instead of a "100-Continue", check that no data is written.
     */
    @Test
    public void test_STR_OCB_CSA00() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        MockClientResponse res00 = new MockClientResponse(mapper, 500, new ErrorResponse("", "", "", "", ""));
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A client is continued, but not buffers are written, check that no data is written.
     */
    @Test
    public void test_STR_OCB_CRC00() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientReqContinue(0, hostAndPort(0, 0));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(1)).writeQueueFull();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A client is continued and a buffer is written, check that only the continued client writes out the buffer.
     */
    @Test
    public void test_STR_OCB_CRC00_SRBp() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientReqContinue(0, hostAndPort(0, 0));
        Buffer buffer = buffer(DEFAULT_MAX_CHUNK_SIZE - 1);
        machine.serverReqBuffer(buffer);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * Two clients are continued, one before a buffer is written and one after, check that both of the continued clients
     * write out the buffer.
     */
    @Test
    public void test_STR_OCB_CRC00_SRBp_CRC01() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);
        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientReqContinue(0, hostAndPort(0, 0));
        Buffer buffer = buffer(DEFAULT_MAX_CHUNK_SIZE - 1);
        machine.serverReqBuffer(buffer);
        machine.clientReqContinue(0, hostAndPort(0, 1));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).write(buffer);

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * All three clients are continued before a buffer is written, check that all three clients write the buffer and
     * end the request (since there is only one buffer in the entire body).
     */
    @Test
    public void test_STR_OCB_CRC00_CRC01_CRC02_SRBf() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_CONTENT_LEN, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);
        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientReqContinue(0, hostAndPort(0, 0));
        machine.clientReqContinue(0, hostAndPort(0, 1));
        machine.clientReqContinue(0, hostAndPort(0, 2));
        Buffer buffer = buffer(DEFAULT_CONTENT_LEN);
        machine.serverReqBuffer(buffer);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_CONTENT_LEN, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).write(buffer);
        verify(mocks.getRequest(0, 1)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 2), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 2), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 2)).write(buffer);
        verify(mocks.getRequest(0, 2)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 2), PutObjectActions.ClientTimer.RESPONSE, machine);

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A client is continued, the full chunk is written, and then a response arrives from the client. This won't change
     * any state that is visible to these tests.
     */
    @Test
    public void test_STR_OCB_CRC00_SRBf_CSA00() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_CONTENT_LEN, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);
        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientReqContinue(0, hostAndPort(0, 0));
        Buffer buffer = buffer(DEFAULT_CONTENT_LEN);
        machine.serverReqBuffer(buffer);
        MockClientResponse res00 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_CONTENT_LEN, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A client is continued, the full chunk is written, the response arrives and a buffer arrives for it. This won't
     * change any state visible to these tests.
     */
    @Test
    public void test_STR_OCB_CRC00_SRBf_CSA00_CSB00() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_CONTENT_LEN, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);
        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientReqContinue(0, hostAndPort(0, 0));
        Buffer buffer = buffer(DEFAULT_CONTENT_LEN);
        machine.serverReqBuffer(buffer);
        MockClientResponse res00 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 0), res00.clientResBody);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_CONTENT_LEN, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * The full chunk is written, two clients continue, have responses arrive, have buffers arrive and then have their
     * responses end. Check that the chunk was successful and the endChunk action is taken.
     */
    @Test
    public void test_STR_OCB_SRBf_CRC00_CRC01_CSA00_CSA01_CSB00_CSB01() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_CONTENT_LEN, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer = buffer(DEFAULT_CONTENT_LEN);
        machine.serverReqBuffer(buffer);
        machine.clientReqContinue(0, hostAndPort(0, 0));
        machine.clientReqContinue(0, hostAndPort(0, 1));
        MockClientResponse res00 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);
        MockClientResponse res01 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 1), res01.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 0), res00.clientResBody);
        machine.clientResBuffer(0, hostAndPort(0, 1), res01.clientResBody);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_CONTENT_LEN, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res00.clientRes).statusCode();
        verify(res00.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).write(buffer);
        verify(mocks.getRequest(0, 1)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res01.clientRes).statusCode();
        verify(res01.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);
        verify(mocks.actions).endChunk(0, digestSHA256(buffer), DEFAULT_CONTENT_LEN, machine);

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(res00.clientRes);
        verifyNoMoreInteractions(res01.clientRes);
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * The first chunk is ended and the task to end the object has started.
     */
    @Test
    public void test_STR_OCB_SRBf_CRC00_CRC01_CSA00_CSA01_CSB00_CSB01_CED0() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_CONTENT_LEN, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);
        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer = buffer(DEFAULT_CONTENT_LEN);
        machine.serverReqBuffer(buffer);
        machine.clientReqContinue(0, hostAndPort(0, 0));
        machine.clientReqContinue(0, hostAndPort(0, 1));
        MockClientResponse res00 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);
        MockClientResponse res01 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 1), res01.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 0), res00.clientResBody);
        machine.clientResBuffer(0, hostAndPort(0, 1), res01.clientResBody);
        machine.chunkEnded(0);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_CONTENT_LEN, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res00.clientRes).statusCode();
        verify(res00.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).write(buffer);
        verify(mocks.getRequest(0, 1)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res01.clientRes).statusCode();
        verify(res01.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);
        verify(mocks.actions).endChunk(0, digestSHA256(buffer), DEFAULT_CONTENT_LEN, machine);
        verify(mocks.actions).endObject(digestMD5(buffer), machine);


        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(res00.clientRes);
        verifyNoMoreInteractions(res01.clientRes);
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * The first chunk is ended, the task to end the object has started and a server exception arrives, check that the
     * future is failed (with an exception), but that only the client that has taken no writes is closed, and the server
     * connection and the other clients are still open (as they have finished their HTTP requests and responses).
     */
    @Test
    public void test_STR_OCB_SRBf_CRC00_CRC01_CSA00_CSA01_CSB00_CSB01_CED0_SRX() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_CONTENT_LEN, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);
        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer = buffer(DEFAULT_CONTENT_LEN);
        machine.serverReqBuffer(buffer);
        machine.clientReqContinue(0, hostAndPort(0, 0));
        machine.clientReqContinue(0, hostAndPort(0, 1));
        MockClientResponse res00 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);
        MockClientResponse res01 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 1), res01.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 0), res00.clientResBody);
        machine.clientResBuffer(0, hostAndPort(0, 1), res01.clientResBody);
        machine.chunkEnded(0);
        machine.serverException(new RuntimeException("test"));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_CONTENT_LEN, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res00.clientRes).statusCode();
        verify(res00.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).write(buffer);
        verify(mocks.getRequest(0, 1)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res01.clientRes).statusCode();
        verify(res01.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);
        verify(mocks.actions).endChunk(0, digestSHA256(buffer), DEFAULT_CONTENT_LEN, machine);
        verify(mocks.actions).endObject(digestMD5(buffer), machine);
        verify(mocks.getRequest(0, 2), atLeast(1)).connection();
        verify(mocks.actions).abortObject();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(res00.clientRes);
        verifyNoMoreInteractions(res01.clientRes);
        verifyZeroInteractions(mocks.serverReq);
        assertPutObjectMachineException(future, true /* clean */);
    }

    /**
     * The full object has ended and the response has been written out.
     */
    @Test
    public void test_STR_OCB_SRBf_CRC00_CRC01_CSA00_CSA01_CSB00_CSB01_CED0_OED() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_CONTENT_LEN, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);
        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer = buffer(DEFAULT_CONTENT_LEN);
        machine.serverReqBuffer(buffer);
        machine.clientReqContinue(0, hostAndPort(0, 0));
        machine.clientReqContinue(0, hostAndPort(0, 1));
        MockClientResponse res00 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);
        MockClientResponse res01 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 1), res01.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 0), res00.clientResBody);
        machine.clientResBuffer(0, hostAndPort(0, 1), res01.clientResBody);
        machine.chunkEnded(0);
        machine.objectEnded(createStorageObjectInfo(DEFAULT_CONTENT_LEN));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_CONTENT_LEN, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res00.clientRes).statusCode();
        verify(res00.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).write(buffer);
        verify(mocks.getRequest(0, 1)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res01.clientRes).statusCode();
        verify(res01.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);
        verify(mocks.actions).endChunk(0, digestSHA256(buffer), DEFAULT_CONTENT_LEN, machine);
        verify(mocks.actions).endObject(digestMD5(buffer), machine);

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(res00.clientRes);
        verifyNoMoreInteractions(res01.clientRes);
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isTrue();
    }

    /**
     * Write out two chunks and end the object.
     */
    @Test
    public void testTwoChunks() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer0 = buffer(DEFAULT_MAX_CHUNK_SIZE);
        machine.serverReqBuffer(buffer0);
        machine.clientReqContinue(0, hostAndPort(0, 0));
        machine.clientReqContinue(0, hostAndPort(0, 1));
        machine.clientReqContinue(0, hostAndPort(0, 2));
        MockClientResponse res00 = createSuccessResponse(mapper, buffer0);
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);
        MockClientResponse res01 = createSuccessResponse(mapper, buffer0);
        machine.clientResArrive(0, hostAndPort(0, 1), res01.clientRes);
        MockClientResponse res02 = createSuccessResponse(mapper, buffer0);
        machine.clientResArrive(0, hostAndPort(0, 2), res02.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 0), res00.clientResBody);
        machine.clientResBuffer(0, hostAndPort(0, 1), res01.clientResBody);
        machine.clientResBuffer(0, hostAndPort(0, 2), res02.clientResBody);
        machine.chunkEnded(0);

        Buffer buffer1 = buffer(DEFAULT_MAX_CHUNK_SIZE, "y");
        machine.serverReqBuffer(buffer1);
        machine.serverReqEnd();
        machine.chunkBegan(1, createVolRepMeta(1));
        machine.clientReqContinue(1, hostAndPort(1, 0));
        machine.clientReqContinue(1, hostAndPort(1, 1));
        machine.clientReqContinue(1, hostAndPort(1, 2));
        MockClientResponse res10 = createSuccessResponse(mapper, buffer1);
        machine.clientResArrive(1, hostAndPort(1, 0), res10.clientRes);
        MockClientResponse res11 = createSuccessResponse(mapper, buffer1);
        machine.clientResArrive(1, hostAndPort(1, 1), res11.clientRes);
        MockClientResponse res12 = createSuccessResponse(mapper, buffer1);
        machine.clientResArrive(1, hostAndPort(1, 2), res12.clientRes);
        machine.clientResBuffer(1, hostAndPort(1, 0), res10.clientResBody);
        machine.clientResBuffer(1, hostAndPort(1, 1), res11.clientResBody);
        machine.clientResBuffer(1, hostAndPort(1, 2), res12.clientResBody);
        machine.chunkEnded(1);
        machine.objectEnded(createStorageObjectInfo(DEFAULT_CONTENT_LEN));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verify(mocks.actions).processBuffer(0, buffer0);
        verify(mocks.actions).processBuffer(1, buffer1);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer0);
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res00.clientRes).statusCode();
        verify(res00.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).write(buffer0);
        verify(mocks.getRequest(0, 1)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res01.clientRes).statusCode();
        verify(res01.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 2), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 2), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 2)).write(buffer0);
        verify(mocks.getRequest(0, 2)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 2), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 2), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res02.clientRes).statusCode();
        verify(res02.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);
        verify(mocks.actions).endChunk(0, digestSHA256(buffer0), DEFAULT_MAX_CHUNK_SIZE, machine);

        verifyClientRequestCreated(mocks.getRequest(1, 0));
        verifyClientRequestCreated(mocks.getRequest(1, 1));
        verifyClientRequestCreated(mocks.getRequest(1, 2));

        verify(mocks.actions).beginChunk(1, DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, 1, machine);

        verify(mocks.actions).stopClientTimer(1, hostAndPort(1, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(1, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(1, 0)).write(buffer1);
        verify(mocks.getRequest(1, 0)).end();
        verify(mocks.actions).startClientTimer(1, hostAndPort(1, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(1, hostAndPort(1, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res10.clientRes).statusCode();
        verify(res10.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).stopClientTimer(1, hostAndPort(1, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(1, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(1, 1)).write(buffer1);
        verify(mocks.getRequest(1, 1)).end();
        verify(mocks.actions).startClientTimer(1, hostAndPort(1, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(1, hostAndPort(1, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res11.clientRes).statusCode();
        verify(res11.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).stopClientTimer(1, hostAndPort(1, 2), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(1, 2), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(1, 2)).write(buffer1);
        verify(mocks.getRequest(1, 2)).end();
        verify(mocks.actions).startClientTimer(1, hostAndPort(1, 2), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(1, hostAndPort(1, 2), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res12.clientRes).statusCode();
        verify(res12.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);
        verify(mocks.actions).endChunk(1, digestSHA256(buffer1), DEFAULT_MAX_CHUNK_SIZE, machine);

        verify(mocks.actions).endObject(digestMD5(buffer0, buffer1), machine);

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(mocks.getRequest(1, 0));
        verifyNoMoreInteractions(mocks.getRequest(1, 1));
        verifyNoMoreInteractions(mocks.getRequest(1, 2));
        verifyNoMoreInteractions(res00.clientRes);
        verifyNoMoreInteractions(res01.clientRes);
        verifyNoMoreInteractions(res02.clientRes);
        verifyNoMoreInteractions(res10.clientRes);
        verifyNoMoreInteractions(res11.clientRes);
        verifyNoMoreInteractions(res12.clientRes);
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isTrue();
    }

    /**
     * A server buffer arrives with a client that has continued, but which has a full queue. Checks that the write to
     * the client does not occur.
     */
    @Test
    public void test_STR_OCB_CRC00_SRBp00() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientReqContinue(0, hostAndPort(0, 0));
        when(mocks.getRequest(0, 0).writeQueueFull()).thenReturn(true);
        Buffer buffer0 = buffer(DEFAULT_MAX_CHUNK_SIZE / 2);
        machine.serverReqBuffer(buffer0);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.actions).processBuffer(0, buffer0);
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.DRAIN, machine);

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A server buffer arrives with a client that has continued but is full, then the client drains. Checks that the
     * write to the client happens after it is drained.
     */
    @Test
    public void test_STR_OCB_CRC00_SRBp00_CRD00() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientReqContinue(0, hostAndPort(0, 0));
        Buffer buffer0 = buffer(DEFAULT_MAX_CHUNK_SIZE / 2);
        when(mocks.getRequest(0, 0).writeQueueFull()).thenReturn(true);
        machine.serverReqBuffer(buffer0);
        when(mocks.getRequest(0, 0).writeQueueFull()).thenReturn(false);
        machine.clientReqDrain(0, hostAndPort(0, 0));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer0);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.DRAIN, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.DRAIN, machine);
        verify(mocks.getRequest(0, 0)).write(buffer0);

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A server buffer arrives in a ring buffer that has only two slots (and no client that can write), so the buffer
     * causes the ring to be full immediately (as we require the ring to have at least two slots free in case we need
     * to split a buffer). Checks that the server request is paused as expected.
     */
    @Test
    public void test_STR_OCB_SRBp_RingFull() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, 2, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer0 = buffer(DEFAULT_MAX_CHUNK_SIZE / 2);
        machine.serverReqBuffer(buffer0);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer0);
        verify(mocks.serverReq).pause();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A server buffer arrives in a ring buffer that has only two slots (and no client that can write), so the buffer
     * causes the ring to be full immediately. Then the clients all become writable, so the ring buffer is emptied.
     * Checks that the server request is paused when the ring buffer is full, and then resumed when it empties.
     */
    @Test
    public void test_STR_OCB_SRBp_RingFull_CRC00_CRC01_CRC02_RingNotFull() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, 2, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer0 = buffer(DEFAULT_MAX_CHUNK_SIZE / 2);
        machine.serverReqBuffer(buffer0);
        machine.clientReqContinue(0, hostAndPort(0, 0));
        machine.clientReqContinue(0, hostAndPort(0, 1));
        machine.clientReqContinue(0, hostAndPort(0, 2));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer0);
        verify(mocks.serverReq).pause();
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer0);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).write(buffer0);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 2), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 2), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 2)).write(buffer0);
        verify(mocks.serverReq).resume();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * The asynchronous task to begin the object and chunk throws an exception, check that no client exceptions were
     * created and that the future has completed exceptionally.
     */
    @Test
    public void test_STR_AEX() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.actionException("beginObjectAndChunk", new RuntimeException("test_STR_AEX"));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verify(mocks.actions).abortObject();
        verifyNoMoreInteractions(mocks.actions);

        verifyZeroInteractions(mocks.serverReq);
        assertPutObjectMachineException(future, false /* clean */);
    }

    /**
     * The asynchronous task to begin a second chunk throws an exception, check that the server connection is closed,
     * all the clients for the first chunk are closed and no clients were created for the second chunk.
     */
    @Test
    public void test_STR_OCB_SRBf_SRBp_AEX() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer0 = buffer(DEFAULT_MAX_CHUNK_SIZE);
        machine.serverReqBuffer(buffer0);
        Buffer buffer1 = buffer(DEFAULT_MAX_CHUNK_SIZE - 1);
        machine.serverReqBuffer(buffer1);
        machine.actionException("beginObjectAndChunk", new RuntimeException("test_STR_OCB_SRBf_SRBp_AEX"));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).beginChunk(1, DEFAULT_MAX_CHUNK_SIZE, machine);
        verify(mocks.actions).processBuffer(0, buffer0);
        verify(mocks.actions).processBuffer(1, buffer1);
        verify(mocks.getRequest(0, 0), atLeast(1)).connection();
        verify(mocks.getRequest(0, 1), atLeast(1)).connection();
        verify(mocks.getRequest(0, 2), atLeast(1)).connection();
        verify(mocks.actions).abortObject();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        assertPutObjectMachineException(future, false /* clean */);
    }

    /**
     * The asynchronous action to end the first chunk throws an exception. Check that the future has completed
     * exceptionally and is not clean, the two clients that finished writing were not closed (they can be re-used) and
     * the third client is closed.
     */
    @Test
    public void test_STR_OCB_SRBf_CRC00_CRC01_CSA00_CSA01_CSB00_CSB01_AEX() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer = buffer(DEFAULT_MAX_CHUNK_SIZE);
        machine.serverReqBuffer(buffer);
        machine.clientReqContinue(0, hostAndPort(0, 0));
        machine.clientReqContinue(0, hostAndPort(0, 1));
        MockClientResponse res00 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);
        MockClientResponse res01 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 1), res01.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 0), res00.clientResBody);
        machine.clientResBuffer(0, hostAndPort(0, 1), res01.clientResBody);
        machine.actionException("beginObjectAndChunk",
                new RuntimeException("test_STR_OCB_SRBf_CRC00_CRC01_CSA00_CSA01_CSB00_CSB01..AEX"));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res00.clientRes).statusCode();
        verify(res00.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).write(buffer);
        verify(mocks.getRequest(0, 1)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res01.clientRes).statusCode();
        verify(res01.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).endChunk(0, digestSHA256(buffer), DEFAULT_MAX_CHUNK_SIZE, machine);
        verify(mocks.actions).beginChunk(1, DEFAULT_MAX_CHUNK_SIZE, machine);
        verify(mocks.getRequest(0, 2), atLeast(1)).connection();
        verify(mocks.actions).abortObject();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(res00.clientRes);
        verifyNoMoreInteractions(res01.clientRes);
        verifyZeroInteractions(mocks.serverReq);
        assertPutObjectMachineException(future, false /* clean */);
    }

    /**
     * The asynchronous action to end the object throws an exception. Check that the future is completed with an
     * exception but is clean, that the two clients that finished writing are not closed and that the third client is
     * closed.
     */
    @Test
    public void test_STR_OCB_SRBf_CRC00_CSA00_CSB00_CRC01_CSA01_CSB01_CED0_AEX() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(
                mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_MAX_CHUNK_SIZE);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer = buffer(DEFAULT_MAX_CHUNK_SIZE);
        machine.serverReqBuffer(buffer);
        machine.clientReqContinue(0, hostAndPort(0, 0));
        MockClientResponse res00 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 0), res00.clientResBody);
        machine.clientReqContinue(0, hostAndPort(0, 1));
        MockClientResponse res01 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 1), res01.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 1), res01.clientResBody);
        machine.chunkEnded(0);
        machine.actionException("endObject", new RuntimeException("test_STR_OCB_SRBf_CRC00_CSA00_CSB00_CRC01_.._AEX"));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res00.clientRes).statusCode();
        verify(res00.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).write(buffer);
        verify(mocks.getRequest(0, 1)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res01.clientRes).statusCode();
        verify(res01.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).endChunk(0, digestSHA256(buffer), DEFAULT_MAX_CHUNK_SIZE, machine);
        verify(mocks.actions).endObject(digestMD5(buffer), machine);
        verify(mocks.getRequest(0, 2), atLeast(1)).connection();
        verify(mocks.actions).abortObject();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(res00.clientRes);
        verifyNoMoreInteractions(res01.clientRes);
        verifyZeroInteractions(mocks.serverReq);
        assertPutObjectMachineException(future, true /* clean */);
    }

    /**
     * Two of the three storage server clients fail due to error responses that arrive after all of the data has been
     * sent to them. Check that only the third storage server client is closed and that the future returns an
     * exception (as that chunk failed).
     */
    @Test
    public void test_STR_OCB_SRBf_CRC00_CSA00_CSB00_CRC01_CSA01() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(
                mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_MAX_CHUNK_SIZE);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer = buffer(DEFAULT_MAX_CHUNK_SIZE);
        machine.serverReqBuffer(buffer);
        machine.clientReqContinue(0, hostAndPort(0, 0));
        MockClientResponse res00 = createErrorResponse(mapper);
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 0), res00.clientResBody);
        machine.clientReqContinue(0, hostAndPort(0, 1));
        MockClientResponse res01 = createErrorResponse(mapper);
        machine.clientResArrive(0, hostAndPort(0, 1), res01.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 1), res01.clientResBody);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res00.clientRes).statusCode();
        verify(res00.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).write(buffer);
        verify(mocks.getRequest(0, 1)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res01.clientRes).statusCode();
        verify(res01.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.getRequest(0, 2), atLeast(1)).connection();
        verify(mocks.actions).abortObject();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(res00.clientRes);
        verifyNoMoreInteractions(res01.clientRes);
        verifyZeroInteractions(mocks.serverReq);
        assertPutObjectMachineException(future, true /* clean */);
    }

    /**
     * The client unexpectedly closes the server request after the object has begun, check that the client requests are
     * all closed and that the future contains an exception and is not clean.
     */
    @Test
    public void test_STR_OCB_SSC() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(
                mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_MAX_CHUNK_SIZE);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.serverResClose();

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verify(mocks.getRequest(0, 0), atLeast(1)).connection();
        verify(mocks.getRequest(0, 1), atLeast(1)).connection();
        verify(mocks.getRequest(0, 2), atLeast(1)).connection();
        verify(mocks.actions).abortObject();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        assertPutObjectMachineException(future, false /* clean */);
    }

    /**
     * A client connection throws an exception after it is created, check that the client is closed, but nothing else
     * is closed, and the future is not yet completed.
     */
    @Test
    public void test_STR_OCB_CEX00() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(
                mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_MAX_CHUNK_SIZE);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientException(0, hostAndPort(0, 0), new RuntimeException("test"));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verify(mocks.getRequest(0, 0), atLeast(1)).connection();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyZeroInteractions(mocks.getRequest(0, 1));
        verifyZeroInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A client connection has a timeout after it is created, check that the client is closed, but nothing else is
     * closed and the future is not yet completed.
     */
    @Test
    public void test_STR_OCB_CTO00() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(
                mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_MAX_CHUNK_SIZE);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientTimeout(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verify(mocks.getRequest(0, 0), atLeast(1)).connection();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * Two client connections timeout after they are created, check that all three clients are closed and the future
     * is completed with an exception and is not clean.
     */
    @Test
    public void test_STR_OCB_CTO00_CTO01() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(
                mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_MAX_CHUNK_SIZE);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientTimeout(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE);
        machine.clientTimeout(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verify(mocks.getRequest(0, 0), atLeast(1)).connection();
        verify(mocks.getRequest(0, 1), atLeast(1)).connection();
        verify(mocks.getRequest(0, 2), atLeast(1)).connection();
        verify(mocks.actions).abortObject();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        assertPutObjectMachineException(future, false /* clean */);
    }

    /**
     * A client returns a response with no content-length header.
     */
    @Test
    public void test_STR_OCB_CSA00_MissingContentLength() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        HttpClientResponse res00 = mock(HttpClientResponse.class);
        when(res00.statusCode()).thenReturn(HttpResponseStatus.OK);
        when(res00.getHeader(HttpHeaders.CONTENT_LENGTH)).thenReturn(null);
        machine.clientResArrive(0, hostAndPort(0, 0), res00);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(1)).connection();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A client returns a response with a content-length of zero.
     */
    @Test
    public void test_STR_OCB_CSA00_ContentLengthZero() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        HttpClientResponse res00 = mock(HttpClientResponse.class);
        when(res00.statusCode()).thenReturn(HttpResponseStatus.OK);
        when(res00.getHeader(HttpHeaders.CONTENT_LENGTH)).thenReturn("0");
        machine.clientResArrive(0, hostAndPort(0, 0), res00);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(1)).connection();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A client returns a response with a content-length that is not a number
     */
    @Test
    public void test_STR_OCB_CSA00_ContentLengthNotANumber() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        HttpClientResponse res00 = mock(HttpClientResponse.class);
        when(res00.statusCode()).thenReturn(HttpResponseStatus.OK);
        when(res00.getHeader(HttpHeaders.CONTENT_LENGTH)).thenReturn("NotANumber");
        machine.clientResArrive(0, hostAndPort(0, 0), res00);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(1)).connection();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A client returns a success response that is incorrectly formatted.
     */
    @Test
    public void test_STR_OCB_CSA00_CSB00_SuccessIncorrectlyFormatted() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        HttpClientResponse res00 = mock(HttpClientResponse.class);
        when(res00.statusCode()).thenReturn(HttpResponseStatus.OK);
        when(res00.getHeader(HttpHeaders.CONTENT_LENGTH)).thenReturn("1");
        machine.clientResArrive(0, hostAndPort(0, 0), res00);
        machine.clientResBuffer(0, hostAndPort(0, 0), Buffer.buffer("{"));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(1)).connection();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A client returns an error response that is incorrectly formatted.
     */
    @Test
    public void test_STR_OCB_CSA00_CSB00_ErrorIncorrectlyFormatted() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        HttpClientResponse res00 = mock(HttpClientResponse.class);
        when(res00.statusCode()).thenReturn(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        when(res00.getHeader(HttpHeaders.CONTENT_LENGTH)).thenReturn("1");
        machine.clientResArrive(0, hostAndPort(0, 0), res00);
        machine.clientResBuffer(0, hostAndPort(0, 0), Buffer.buffer("{"));

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(1)).connection();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * A zero-byte object is written successfully.
     */
    @Test
    public void test_STR_OCB_CRC00_CRC01_CRC02_CSA00_CSA01_CSA02_CSB00_CSB01_CSB02_CED0_OED() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, 0);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientReqContinue(0, hostAndPort(0, 0));
        machine.clientReqContinue(0, hostAndPort(0, 1));
        machine.clientReqContinue(0, hostAndPort(0, 2));
        Buffer buffer = Buffer.buffer("");
        MockClientResponse res00 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);
        MockClientResponse res01 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 1), res01.clientRes);
        MockClientResponse res02 = createSuccessResponse(mapper, buffer);
        machine.clientResArrive(0, hostAndPort(0, 2), res02.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 0), res00.clientResBody);
        machine.clientResBuffer(0, hostAndPort(0, 1), res01.clientResBody);
        machine.clientResBuffer(0, hostAndPort(0, 2), res02.clientResBody);
        machine.chunkEnded(0);
        machine.objectEnded(createStorageObjectInfo(DEFAULT_CONTENT_LEN));

        verify(mocks.actions).beginObjectAndChunk(0, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res00.clientRes).statusCode();
        verify(res00.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res01.clientRes).statusCode();
        verify(res01.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 2), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 2), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 2)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 2), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 2), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res02.clientRes).statusCode();
        verify(res02.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).endChunk(0, digestSHA256(buffer), 0, machine);
        verify(mocks.actions).endObject(digestMD5(buffer), machine);

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(res00.clientRes);
        verifyNoMoreInteractions(res01.clientRes);
        verifyNoMoreInteractions(res02.clientRes);
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isTrue();
    }

    /**
     * A 200 OK response from a storage server arrives before all the data for the chunk is sent.
     */
    @Test
    public void test_STR_OCB_CRC00_CSA00_CSB00_SuccessResponse() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        machine.clientReqContinue(0, hostAndPort(0, 0));
        MockClientResponse res00 = createSuccessResponse(mapper, buffer(DEFAULT_MAX_CHUNK_SIZE));
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 0), res00.clientResBody);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_MAX_CHUNK_SIZE, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res00.clientRes).statusCode();
        verify(res00.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.getRequest(0, 0), atLeast(1)).connection();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyZeroInteractions(mocks.serverReq);
        Assertions.assertThat(future.isDone()).isFalse();
    }

    /**
     * Two storage servers respond with the wrong digest value.
     */
    @Test
    public void test_STR_OCB_SRBf_CRC00_CSA00_CSB00_WrongDigest() {
        Mocks mocks = new Mocks();
        PutObjectMachine machine = createMachine(mocks, DEFAULT_CONTENT_LEN, DEFAULT_RING_SIZE, DEFAULT_CONTENT_LEN);

        CompletableFuture<StorageObjectInfo> future = machine.start();
        machine.objectAndChunkBegan(createVolRepMeta(0));
        Buffer buffer = buffer(DEFAULT_CONTENT_LEN);
        machine.serverReqBuffer(buffer);
        machine.clientReqContinue(0, hostAndPort(0, 0));
        machine.clientReqContinue(0, hostAndPort(0, 1));
        Buffer badBuffer = buffer(DEFAULT_CONTENT_LEN, "y");
        MockClientResponse res00 = createSuccessResponse(mapper, badBuffer);
        machine.clientResArrive(0, hostAndPort(0, 0), res00.clientRes);
        MockClientResponse res01 = createSuccessResponse(mapper, badBuffer);
        machine.clientResArrive(0, hostAndPort(0, 1), res01.clientRes);
        machine.clientResBuffer(0, hostAndPort(0, 0), res00.clientResBody);
        machine.clientResBuffer(0, hostAndPort(0, 1), res01.clientResBody);

        verify(mocks.actions).beginObjectAndChunk(DEFAULT_CONTENT_LEN, machine);
        verifyStartClientTimers(mocks, machine);
        verifyClientRequestCreated(mocks.getRequest(0, 0));
        verifyClientRequestCreated(mocks.getRequest(0, 1));
        verifyClientRequestCreated(mocks.getRequest(0, 2));
        verify(mocks.actions).processBuffer(0, buffer);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 0), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 0)).write(buffer);
        verify(mocks.getRequest(0, 0)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 0), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res00.clientRes).statusCode();
        verify(res00.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);

        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.CONTINUE, machine);
        verify(mocks.getRequest(0, 1), atLeast(0)).writeQueueFull();
        verify(mocks.getRequest(0, 1)).write(buffer);
        verify(mocks.getRequest(0, 1)).end();
        verify(mocks.actions).startClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(mocks.actions).stopClientTimer(0, hostAndPort(0, 1), PutObjectActions.ClientTimer.RESPONSE, machine);
        verify(res01.clientRes).statusCode();
        verify(res01.clientRes).getHeader(HttpHeaders.CONTENT_LENGTH);


        verify(mocks.getRequest(0, 2), atLeast(1)).connection();
        verify(mocks.actions).abortObject();

        verifyNoMoreInteractions(mocks.actions);
        verifyNoMoreInteractions(mocks.getRequest(0, 0));
        verifyNoMoreInteractions(mocks.getRequest(0, 1));
        verifyNoMoreInteractions(mocks.getRequest(0, 2));
        verifyNoMoreInteractions(res00.clientRes);
        verifyNoMoreInteractions(res01.clientRes);
        verifyZeroInteractions(mocks.serverReq);
        assertPutObjectMachineException(future, true /* clean */);
    }
}
