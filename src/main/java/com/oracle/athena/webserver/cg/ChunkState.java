package com.oracle.pic.casper.webserver.api.backend.putobject;

import com.google.common.base.Verify;
import com.google.common.net.HostAndPort;
import com.oracle.pic.casper.common.encryption.Digest;
import com.oracle.pic.casper.common.encryption.DigestAlgorithm;
import com.oracle.pic.casper.common.encryption.DigestUtils;
import io.vertx.core.buffer.Buffer;

import javax.annotation.Nullable;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * ChunkState contains all the state for the processing of a single chunk of an object being written by a customer.
 *
 * A PutObjectMachine can be writing to multiple chunks at the same time, and it creates one ChunkState object for each
 * such chunk. The ChunkState keeps track of how many bytes have been received from the customer, the digest (SHA256)
 * of those bytes and whether or not the chunk has been marked as complete in the database.
 */
final class ChunkState {
    /**
     * A bookmark used by the RingBuffer to mark where in the buffer the data for this chunk starts.
     */
    private final SSClientMachine bookmark;

    /**
     * The chunk sequence number (0-indexed).
     */
    private final int chunkNum;

    /**
     * The expected length of the chunk, in bytes.
     */
    private final int length;

    /**
     * The MessageDigest used to compute the SHA256 of the chunk as bytes arrive.
     */
    private final MessageDigest digest;

    /**
     * A map from the host and port of each storage server to the state machine for that client. These are the storage
     * servers that will hold copies of the chunk.
     */
    private Map<HostAndPort, SSClientMachine> clients;

    /**
     * The number of bytes received from the customer so far.
     */
    private int bytesReceived = 0;

    /**
     * True if an asynchronous task to end the chunk in the DB has been started.
     */
    private boolean endChunkStarted = false;

    /**
     * True if the asynchronous task to end the chunk in the DB has completed successfully.
     */
    private boolean endChunkCompleted = false;

    /**
     * The bytes of the SHA256, once all the bytes have arrived (null until then).
     */
    private byte[] digestBytes;

    /**
     * The base-64 encoded SHA256, once all the bytes have arrived (null until then).
     */
    private String digestBase64;

    ChunkState(int chunkNum, int length) {
        this.bookmark = new SSClientMachine();
        this.chunkNum = chunkNum;
        this.length = length;
        this.clients = new HashMap<>();

        digest = DigestUtils.messageDigestFromAlgorithm(DigestAlgorithm.SHA256);
        if (length == 0) {
            digestBytes = digest.digest();
            digestBase64 = Base64.getEncoder().encodeToString(digestBytes);
        }
    }

    public SSClientMachine getBookmark() {
        return bookmark;
    }

    public int getLength() {
        return length;
    }

    public int getChunkNum() {
        return chunkNum;
    }

    public int getBytesReceived() {
        return bytesReceived;
    }

    @Nullable
    public String getDigestBase64() {
        return digestBase64;
    }

    public Digest getDigest() {
        return new Digest(DigestAlgorithm.SHA256, digestBytes);
    }

    public boolean isEndChunkStarted() {
        return endChunkStarted;
    }

    void recvBuffer(Buffer buffer) {
        digest.update(buffer.getByteBuf().nioBuffer());
        bytesReceived += buffer.length();
        if (bytesReceived == length) {
            digestBytes = digest.digest();
            digestBase64 = Base64.getEncoder().encodeToString(digestBytes);
        }
    }

    void endChunkStarted() {
        Verify.verify(!endChunkStarted && !endChunkCompleted);
        endChunkStarted = true;
    }

    void endChunkCompleted() {
        Verify.verify(endChunkStarted && !endChunkCompleted);
        endChunkCompleted = true;
    }

    public Map<HostAndPort, SSClientMachine> getClients() {
        return clients;
    }

    public void setClients(Map<HostAndPort, SSClientMachine> clients) {
        this.clients = clients;
    }

    boolean isSuccess() {
        return clients.values().stream().filter(c -> c.getStatus() == SSClientMachine.Status.SUCCEEDED).count() >= 2;
    }

    boolean isFailed() {
        return clients.values().stream().filter(c -> c.getStatus() == SSClientMachine.Status.FAILED).count() >= 2;
    }

    boolean isComplete() {
        return isFailed() || (isSuccess() && endChunkStarted && endChunkCompleted);
    }
}
