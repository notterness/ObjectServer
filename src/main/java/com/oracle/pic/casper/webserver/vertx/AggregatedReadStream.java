package com.oracle.pic.casper.webserver.vertx;

import com.oracle.pic.casper.common.vertx.stream.AbortableReadStream;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * The AggregatedReadStream class aggregates a sequence of input read streams into a single, output read stream.
 */
public class AggregatedReadStream<T> implements AbortableReadStream<T> {

    private static final Logger logger = LoggerFactory.getLogger(AggregatedReadStream.class);

    /**
     * Caller-provided supplier to read streams. Supplier will be called to provide the next read stream.
     */
    private final Iterator<? extends CompletableFuture<? extends ReadStream<T>>> readStreamIterator;
    /**
     * The current input read stream.
     */
    private ReadStream<T> currentStream;
    /**
     * Caller-provided data handler, which will be called with the data as data is read.
     */
    private Handler<T> handler;
    /**
     * Caller-provided exception handler, which will be called if an exception is encountered.
     */
    private Handler<Throwable> exceptionHandler;
    /**
     * Caller-provided end handler, which will be called after the last stream has ended.
     */
    private Handler<Void> endHandler;

    /**
     * Default constructor.
     *
     * @param readStreamIterator An iterator, which provides CompletableFutures containing a ReadStream to aggregate.
     */
    public AggregatedReadStream(Iterator<? extends CompletableFuture<? extends ReadStream<T>>> readStreamIterator) {
        this.readStreamIterator = readStreamIterator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReadStream<T> exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReadStream<T> handler(Handler<T> handler) {
        this.handler = handler;

        // if someone is trying to hook up the handler, let's start feeding the data immediately
        if (handler != null && currentStream == null) {
            // If we do not have a current stream, then let's set one up now that we have a data handler.
            setUpNextReadStream();
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReadStream<T> pause() {
        if (currentStream != null) {
            currentStream.pause();
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReadStream<T> resume() {
        if (currentStream != null) {
            currentStream.resume();
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReadStream<T> endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        return this;
    }

    @Override
    public void abort() {
        if (currentStream != null && currentStream instanceof AbortableReadStream) {
            ((AbortableReadStream) currentStream).abort();
        }
    }

    @Override
    public ReadStream<T> fetch(long amount) {
        if (currentStream != null) {
            currentStream.fetch(amount);
        }
        return this;
    }

    /**
     * Set up the next input read stream, which de-queues it and configures its handlers.
     */
    private void setUpNextReadStream() {
        if (readStreamIterator.hasNext()) {
            readStreamIterator.next().whenComplete((readStream, throwable) -> {
                if (throwable != null) {
                    // log at a debug level as the error should also get logged again by the exception handler
                    logger.debug("Error while trying to get the next read stream", throwable);
                    // if something has gone wrong, report the error and stop processing any further
                    handleException(throwable);
                    return;
                }
                // new stream to setup. Let's hook all the necessary handler
                currentStream = readStream;
                readStream.endHandler((v) -> setUpNextReadStream());
                readStream.exceptionHandler(this::handleException);
                readStream.handler(this::handleChunk);
            });
        } else {
            // no more stream means that we reach the end
            if (currentStream != null) {
                handleEnd();
                currentStream = null;
            }
        }
    }

    /**
     * Handles the exception by delegating the call to the provided exception handler.
     *
     * @param throwable the throwable
     */
    private void handleException(Throwable throwable) {
        Handler<Throwable> exceptionHandler = this.exceptionHandler;
        if (exceptionHandler != null) {
            exceptionHandler.handle(throwable);
        }
    }

    /**
     * Forwards the buffer to the provider read handler.
     *
     * @param buffer the buffer
     */
    private void handleChunk(T buffer) {
        Handler<T> handler = this.handler;
        if (handler != null) {
            handler.handle(buffer);
        }
    }

    /**
     * Invokes the end handler when there is no more bytes.
     */
    private void handleEnd() {
        Handler<Void> endHandler = this.endHandler;
        if (endHandler != null) {
            endHandler.handle(null);
        }
    }
}
