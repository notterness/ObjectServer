package com.oracle.athena.webserver.utils;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * A bounded, fixed-capacity queue that allows multiple readers to process entries at different rates.
 *
 * RingBuffer is a bounded queue with a fixed-capacity that allows writes and the tail of the queue, and reads from
 * multiple queue heads. Readers can be registered and unregistered with the RingBuffer, and a newly registered reader
 * is empty (a call to size(reader) will return 0). The size(reader) call will return the number of elements written
 * to the RingBuffer that the reader has not yet consumed. The size() of the RingBuffer is the max of the size of the
 * readers, and the remainingCapacity() is the total capacity minus the size().
 */
public final class RingBuffer<E, R> {
    /*
     * This implementation requires a ring that is one element larger than the requested capacity. That makes it
     * possible to differentiate two cases:
     *
     *   1. The reader index is equal to the writer index, and the reader is "empty".
     *   2. The reader index is equal to the writer index, and the reader is "full".
     *
     * The extra element means that we can interpret the positions as follows:
     *
     *   A. The write index points to the next element to be written (an empty element).
     *   B. Reader indexes point to the next element to read unless they are at the same index as the writer, in which
     *      case there is nothing more to be read.
     *   C. When the write index is one less than the minimum reader index, the ring is "full" (as, otherwise, if we
     *      wrote to that element and incremented the write index, it would look like that reader was empty).
     *
     * There are more efficient ways to implement this structure if you are using a better language, but Java chose to
     * not allow unsigned primitive values, so this is as good as it gets here.
     */

    private final Object[] ring;
    private final Map<R, Integer> readers;
    private int cur;

    /**
     * Constructs an empty RingBuffer with the given capacity and no readers.
     * @param capacity the capacity of the ring, which must be greater than zero.
     */
    public RingBuffer(int capacity) {
        Preconditions.checkArgument(capacity > 0, "The capacity must be greater than zero");

        ring = new Object[capacity + 1];
        readers = new HashMap<>();
        cur = 0;
    }

    /**
     * Adds the buffer to the ring, throwing an IllegalStateException if no space is currently available.
     */
    public void add(E element) {
        Preconditions.checkNotNull(element, "The ring buffer can not store null elements");
        Preconditions.checkState(!readers.isEmpty(), "There must be at least one registered reader");
        Preconditions.checkState(remainingCapacity() > 0, "The ring buffer is full");

        ring[cur] = element;
        cur = (cur + 1) % ring.length;
    }

    /**
     * Adds the buffer to the ring, returning false if the ring was full, and true otherwise.
     */
    public boolean offer(E element) {
        Preconditions.checkNotNull(element, "The ring buffer can not store null elements");
        Preconditions.checkState(!readers.isEmpty(), "There must be at least one registered reader");

        if (remainingCapacity() == 0) {
            return false;
        }

        ring[cur] = element;
        cur = (cur + 1) % ring.length;

        return true;
    }

    /**
     * Returns the number of buffers that can be written before the ring is full.
     */
    public int remainingCapacity() {
        return capacity() - size();
    }

    /**
     * Retrieves the next buffer for the given reader and increments the readers position, returning null if there are
     * no more elements to read.
     */
    public E poll(R reader) {
        Integer idx = readers.get(reader);
        if (idx == null) {
            throw new IllegalArgumentException("The requested reader has not been registered");
        }

        if (idx == cur) {
            return null;
        }

        readers.put(reader, (idx + 1) % ring.length);

        @SuppressWarnings("unchecked")
        E e = (E) ring[idx];
        return e;
    }

    /**
     * Retrieves the next buffer for the given reader but does not increment the readers position. Returns null if there
     * are no more elements to read.
     */
    public E peek(R reader) {
        Integer idx = readers.get(reader);
        if (idx == null) {
            throw new IllegalArgumentException("The requested reader has not been registerd");
        }

        if (idx == cur) {
            return null;
        }

        @SuppressWarnings("unchecked")
        E e = (E) ring[idx];
        return e;
    }

    /**
     * Returns the number of elements remaining to be read by this reader.
     */
    public int size(R reader) {
        Integer idx = readers.get(reader);
        if (idx == null) {
            throw new IllegalStateException("The requested reader has not been registered");
        }

        return (ring.length + (cur - idx)) % ring.length;
    }

    /**
     * Registers a reader with the ring. The reader will initially be empty (size(state) == 0).
     */
    public void registerReader(R reader) {
        if (readers.containsKey(reader)) {
            throw new IllegalArgumentException("The reader has already been registered");
        }

        readers.put(reader, cur);
    }

    /**
     * Registers a reader with the ring at the same position as the bookmark reader (which must be already registered).
     */
    public void registerReader(R reader, R bookmark) {
        if (readers.containsKey(reader)) {
            throw new IllegalArgumentException("The reader has already been registered");
        }

        if (!readers.containsKey(bookmark)) {
            throw new IllegalArgumentException("The bookmark reader has not been registered");
        }

        readers.put(reader, readers.get(bookmark));
    }

    /**
     * Unregisters a reader from the ring, which will increase the remainingCapacity of the ring if the index of the
     * reader was the smallest of the readers (and no other reader had the same index).
     */
    public void unregisterReader(R reader) {
        readers.remove(reader);
    }

    /**
     * Returns the total capacity of the ring.
     */
    public int capacity() {
        return ring.length - 1;
    }

    /**
     * Returns the total number of readable elements currently in the ring.
     *
     * If there are no registered readers, the size is always zero.
     */
    public int size() {
        if (readers.isEmpty()) {
            return 0;
        }

        int maxSize = Integer.MIN_VALUE;
        for (int reader : readers.values()) {
            int readerSize = (ring.length + (cur - reader)) % ring.length;
            if (readerSize > maxSize) {
                maxSize = readerSize;
            }
        }

        return maxSize;
    }

    /**
     * Unregisters all readers and empties the ring.
     */
    public void clear() {
        for (int i = 0; i < ring.length; i++) {
            ring[i] = null;
        }

        readers.clear();
        cur = 0;
    }
}
