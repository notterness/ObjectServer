package com.oracle.pic.casper.webserver.jnr;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A utility class for working with binary data.
 */
public enum Octets {
    LITTLE_ENDIAN(ByteOrder.LITTLE_ENDIAN),
    BIG_ENDIAN(ByteOrder.BIG_ENDIAN);

    public static final int SIZE_OF_INT = 4;
    public static final int SIZE_OF_LONG = 8;

    private ByteOrder order;

    /**
     * Construct a {@link Octets} instance with the given byte order
     * @param order This octets instance's byte order.
     */
    Octets(ByteOrder order) {
        this.order = order;
    }

    /**
     * Deserialize an int from bytes.
     *
     * @param bytes The serialized integer (must be 4 bytes long!)
     * @return The integer.
     */
    public int intFrom(byte[] bytes) {
        if (bytes.length != SIZE_OF_INT) {
            throw new RuntimeException(String.format("integers require 4 bytes; got %d bytes", bytes.length));
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.order(order);
        return buf.getInt();
    }

    /**
     * Serialize an int to bytes
     *
     * @param i The integer to serialize
     * @return The serialized integer (4 bytes long!)
     */
    public byte[] toByteArray(int i) {
        ByteBuffer buf = ByteBuffer.allocate(SIZE_OF_INT);
        buf.order(order);
        buf.putInt(i);
        return buf.array();
    }

    /**
     * Deserialize a long from bytes.
     *
     * @param bytes The serialized long (must be 8 bytes long!)
     * @return The long.
     */
    public long longFrom(byte[] bytes) {
        if (bytes.length != SIZE_OF_LONG) {
            throw new RuntimeException(String.format("integers require 4 bytes; got %d bytes", bytes.length));
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.order(order);
        return buf.getLong();
    }

    /**
     * Serialize a long to bytes
     *
     * @param l The integer to serialize
     * @return The serialized integer (4 bytes long!)
     */
    public byte[] toByteArray(long l) {
        ByteBuffer buf = ByteBuffer.allocate(SIZE_OF_LONG);
        buf.order(order);
        buf.putLong(l);
        return buf.array();
    }
}
