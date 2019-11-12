package com.oracle.pic.casper.webserver.vertx;

import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.vertx.stream.DelegateReadStream;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import java.util.Optional;

/**
 * The FixedCapacityReadStream class is a decorator over Vert.x's ReadStream to limit the number of bytes read
 * from a single stream instance. If the stream reaches capacity, i.e. if the maximum number of bytes have been
 * read or transferred, the stream will end, even if the delegate stream has more data. Any data excess of the
 * capacity will be stored in an overflow buffer. Expected usage will be to instantiate another FixedCapacityReadStream
 * instance, which will utilize the previous stream's overflow buffer before the delegate.
 *
 * This class assumes that the underlying delegate stream respects the pause method, as defined by the ReadStream
 * interface, and does not treat it as advisory. If not, this introduces a race condition that causes data corruption
 * (as data written to one FixedCapacityReadStream's instance after it has ended may not be read by the next instance).
 */
public class FixedCapacityReadStream extends DelegateReadStream<Buffer> implements OverflowableReadStream {
    /** The capacity, or maximum number of bytes, of this read stream. */
    private final long fixedCapacity;

    /** A counter for the number of bytes transferred by this stream. */
    private long bytesTransferred;

    /** A flag to indicate whether or not this stream has ended. */
    private boolean hasEnded;

    /** A caller-provided handler. If set, it is invoked when this stream ends. */
    private Handler<Void> endHandler;

    /** Any bytes remaining from the delegate exceeding this stream's capacity will be stored in this buffer. */
    private Optional<Buffer> overflowBuffer;

    /**
     * An optional overflow buffer from a previous OverflowableReadStream. If provided, this buffer will be used
     * before the delegate stream.
     */
    private Optional<Buffer> previousOverflowBuffer;

    /**
     * Instantiates a new FixedCapacityReadStream, which only reads a fixed number of bytes from the delegate read
     * stream.
     *
     * @param fixedCapacity    The capacity of this stream, which is the maximum number of bytes that can be read.
     * @param delegate         The delegate stream to read from.
     * @param delegateHasEnded Flag indicating whether or not the delegate has ended.
     * @param previousStream   An optional parameter to provide the previous OverflowableReadStream, if one preceded
     *                         this one, to access the delegate end state and any overflow buffer.
     */
    public FixedCapacityReadStream(
            long fixedCapacity, ReadStream<Buffer> delegate, boolean delegateHasEnded,
            Optional<FixedCapacityReadStream> previousStream) {
        super(delegate, delegateHasEnded);

        if (previousStream.isPresent()) {
            this.previousOverflowBuffer = previousStream.get().getOverflowBuffer();
        } else {
            this.previousOverflowBuffer = Optional.empty();
        }

        this.fixedCapacity = fixedCapacity;
        this.overflowBuffer = Optional.empty();
        this.hasEnded = false;
        this.bytesTransferred = 0;
    }

    /**
     * Set a data handler.
     *
     * @param handler As data is read, the handler will be called with the data.
     * @return a reference to this, so the API can be used fluently
     */
    @Override
    public ReadStream<Buffer> handler(Handler<Buffer> handler) {
        if (handler == null) {
            return super.handler(null);
        }

        Preconditions.checkState(!hasEnded, "Cannot set a handler when the read stream has ended.");

        if (!delegateHasEnded()) {
            getDelegate().pause();
        }

        Handler<Buffer> dataHandler = buffer -> {
            Preconditions.checkState(!hasEnded, "FixedCapacityReadStream data handler invoked after stream has ended!");

            final Buffer bufferToTransfer;
            if (bytesTransferred + buffer.length() > fixedCapacity) {
                // If the current buffer is larger than our capacity.
                long bytesToTransfer = fixedCapacity - bytesTransferred;
                bufferToTransfer = buffer.getBuffer(0, (int) bytesToTransfer);
                overflowBuffer = Optional.of(buffer.getBuffer((int) bytesToTransfer, buffer.length()));
            } else {
                bufferToTransfer = buffer;
            }

            bytesTransferred += bufferToTransfer.length();
            if (bytesTransferred == fixedCapacity) {
                // We have transferred the maximum number of bytes, so this FixedCapacityReadStream instance has ended.
                hasEnded = true;
                // Pause the underlying delegate stream.
                getDelegate().pause();
            }

            Preconditions.checkState(bytesTransferred <= fixedCapacity, "Bytes transferred (%d) > fixed capacity (%d)",
                    bytesTransferred, fixedCapacity);

            if (delegateHasEnded() && !overflowBuffer.isPresent()) {
                // The delegate has ended, and there is no overflow remaining.
                hasEnded = true;
            }

            handler.handle(bufferToTransfer);

            // Invoke the end handler if this stream has ended.
            if (hasEnded && endHandler != null) {
                endHandler.handle(null);
            }
        };

        if (previousOverflowBuffer.isPresent()) {
            // Immediately invoke the caller-provided data handler with the overflow buffer if it is present.
            dataHandler.handle(previousOverflowBuffer.get());
            previousOverflowBuffer = Optional.empty();
        }

        if (!delegateHasEnded()) {
            super.handler(dataHandler);
            getDelegate().resume();
        }

        return this;
    }

    @Override
    public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
        Preconditions.checkState(!hasEnded, "Cannot register an end handler after the stream has ended");

        this.endHandler = endHandler;

        // The end handler that we register with the delegate must be wrapped in order to differentiate the case
        // when the delegate ends versus the case when this FixedCapacityReadStream instance transfers the maximum
        // number of bytes.
        Handler<Void> wrappedEndHander = (Void v) -> {
            if (hasEnded) {
                // If this stream has already ended, do not invoke the end handler again.
               return;
            }

            hasEnded = true;
            endHandler.handle(v);
        };

        // Only register the end handler with the delegate if the delegate has not ended.
        if (!delegateHasEnded()) {
            super.endHandler(wrappedEndHander);
        }

        return this;
    }

    /**
     * @return The overflow buffer if the delegate stream was larger than the capacity.
     */
    public Optional<Buffer> getOverflowBuffer() {
        Preconditions.checkState(hasEnded, "Cannot get overflow buffer before stream has ended.");
        return overflowBuffer;
    }

    /**
     * @return true if this stream has ended and false otherwise.
     */
    public boolean hasEnded() {
        return hasEnded;
    }
}
