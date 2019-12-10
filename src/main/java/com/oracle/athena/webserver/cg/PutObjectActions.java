package com.oracle.pic.casper.webserver.api.backend.putobject;

import com.google.common.net.HostAndPort;
import com.oracle.pic.casper.common.encryption.Digest;
import io.vertx.core.buffer.Buffer;

/**
 * Asynchronous activities, timers and buffer processing methods used by the PutObjectMachine.
 *
 * These are methods called by the PutObjectMachine to start asynchronous activities and timers. Implementations are
 * required to call back to the PutObjectMachine when the asynchronous activity has completed (if it is not stopped
 * first). The documentation for the individual methods explains what method(s) to call upon completion.
 *
 * The implementation of these methods must not immediately call back into the PutObjectMachine from the same thread
 * on which they were called. They must queue up the call to happen later on the PutObjectMachine thread. This class
 * was designed around Vert.x, so the expectation is that the implementation will use something like Vert.x's
 * executeBlocking method, which allows code to be executed on a worker thread, and then completed back on the event
 * loop thread.
 *
 * The methods of this class can interact with multiple event loop threads and an event loop thread, so implementations
 * of this class must be thread safe.
 */
public interface PutObjectActions {

    enum ClientTimer {
        CONTINUE,
        DRAIN,
        RESPONSE
    }

    /**
     * Start the asynchronous action to initialize the object and chunk metadata, and fetch the first volume ID and VON.
     *
     * When this task completes successfully, it must call PutObjectMachine#objectAndChunkBegan. If this task fails to
     * complete, it must call PutObjectMachine#actionException.
     */
    void beginObjectAndChunk(long chunkLen, PutObjectMachine machine);

    /**
     * Start the asynchronous task to initialize the chunk metadata and fetch the volume ID and VON for it.
     *
     * When this task completes successfully, it must call PutObjectMachine#chunkBegan. If this task fails to complete,
     * it must call PutObjectMachine#actionException.
     */
    void beginChunk(int chunkNum, long chunkLen, PutObjectMachine machine);

    /**
     * Start the asynchronous task to mark the chunk metadata as complete.
     *
     * When this task completes successfully, it must call PutObjectMachine#endChunk. If this task fails to complete,
     * it must call PutObjectMachine#actionException.
     */
    void endChunk(int chunkNum, Digest digest, long length, PutObjectMachine machine);

    /**
     * Start the asynchronous task to mark the object metadata as complete.
     *
     * When this task completes successfully, it must call PutObjectMachine#endObject. If this task fails to complete,
     * it must call PutObjectMachine#actionException.
     */
    void endObject(Digest digest, PutObjectMachine machine);

    /**
     * Abort the write of the object.
     *
     * This method can do any necessary clean up for the internal state of the PutObjectActions implementation. The
     * state machine does not require this method to do anything (it is safe for on-going database operations to
     * continue and timers to fire), so it is safe to have this method do nothing.
     */
    void abortObject();

    /**
     * Start a timer associated with a specific storage server client.
     *
     * If the timer completes before stopClientTimer is called for the same timer, then it must call
     * PutObjectMachine#clientTimeout.
     *
     * If this is called when the timer is alread running, the timer will be reset.
     */
    void startClientTimer(int chunkNum, HostAndPort hostAndPort, ClientTimer timer, PutObjectMachine machine);

    /**
     * Stop the timer associated with a specific storage server client.
     *
     * If the timer is not currently running when this method is called, this is a no-op.
     */
    void stopClientTimer(int chunkNum, HostAndPort hostAndPort, ClientTimer timer, PutObjectMachine machine);

    /**
     * Process a buffer, returning the new buffer.
     *
     * The new buffer can be the same as the buffer argument. If it is different, it must be exactly the same length.
     *
     * This method is synchronous.
     */
    Buffer processBuffer(int chunkNum, Buffer buffer);
}
