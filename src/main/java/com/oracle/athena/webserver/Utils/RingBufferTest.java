package com.oracle.athena.webserver.utils;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

public final class RingBufferTest {
    @Test
    public void testZeroCapacity() {
        Assertions.assertThatThrownBy(() -> new RingBuffer<>(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testAddBufferNoReaders() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        Assertions.assertThatThrownBy(() -> rb.add(1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testAddNullBuffer() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        Assertions.assertThatThrownBy(() -> rb.add(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testAddBufferWithOneFullReader() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        rb.add(1);
        Assertions.assertThatThrownBy(() -> rb.add(2)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testAddBufferWithTwoFullReaders() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        rb.registerReader(2);
        rb.add(1);
        Assertions.assertThatThrownBy(() -> rb.add(2)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testAddBufferWithOneFullAndOneNonFullReader() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(2);
        rb.registerReader(1);
        rb.registerReader(2);
        rb.add(1);
        Assertions.assertThat(rb.poll(1)).isEqualTo(1);
        rb.add(2);
        Assertions.assertThat(rb.poll(1)).isEqualTo(2);
        Assertions.assertThatThrownBy(() -> rb.add(3)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testOfferBufferNoReaders() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        Assertions.assertThatThrownBy(() -> rb.offer(1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testOfferNullBuffer() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        Assertions.assertThatThrownBy(() -> rb.offer(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testOfferBufferWithOneFullReader() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        rb.add(1);
        Assertions.assertThat(rb.offer(2)).isFalse();
    }

    @Test
    public void testOfferBufferWithTwoFullReaders() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        rb.registerReader(2);
        rb.add(1);
        Assertions.assertThat(rb.offer(2)).isFalse();
    }

    @Test
    public void testOfferBufferWithOneFullAndOneNonFullReader() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(2);
        rb.registerReader(1);
        rb.registerReader(2);
        rb.add(1);
        Assertions.assertThat(rb.poll(1)).isEqualTo(1);
        rb.add(2);
        Assertions.assertThat(rb.poll(1)).isEqualTo(2);
        Assertions.assertThat(rb.offer(3)).isFalse();
    }

    @Test
    public void testOfferBufferOneNonFullReader() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(2);
        rb.registerReader(1);
        Assertions.assertThat(rb.offer(3)).isTrue();
        Assertions.assertThat(rb.poll(1)).isEqualTo(3);
    }

    @Test
    public void testRemainingCapacityNoReaders() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        Assertions.assertThat(rb.remainingCapacity()).isEqualTo(rb.capacity());
    }

    @Test
    public void testRemainingCapacityOneReaderNoReads() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        Assertions.assertThat(rb.remainingCapacity()).isEqualTo(1);
    }

    @Test
    public void testRemainingCapacityOneReaderFullBufferNoReads() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        rb.add(1);
        Assertions.assertThat(rb.remainingCapacity()).isEqualTo(0);
    }

    @Test
    public void testRemainingCapacityOneReaderFullBufferAfterOneRead() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        rb.add(1);
        Assertions.assertThat(rb.poll(1)).isEqualTo(1);
        Assertions.assertThat(rb.remainingCapacity()).isEqualTo(1);
    }

    @Test
    public void testRemainingCapacityAfterWrapping() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(8);
        rb.registerReader(1);
        for (int i = 0; i < 9; i++) {
            rb.add(i);
            Assertions.assertThat(rb.poll(1)).isEqualTo(i);
        }
        Assertions.assertThat(rb.remainingCapacity()).isEqualTo(8);
    }

    @Test
    public void testPollNoSuchReader() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        Assertions.assertThatThrownBy(() -> rb.poll(2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPollEmptyReader() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        rb.add(1);
        Assertions.assertThat(rb.poll(1)).isEqualTo(1);
        Assertions.assertThat(rb.poll(1)).isNull();
    }

    @Test
    public void testPollAfterWrapping() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(8);
        rb.registerReader(1);
        for (int i = 0; i < 9; i++) {
            rb.add(i);
            Assertions.assertThat(rb.poll(1)).isEqualTo(i);
        }
        Assertions.assertThat(rb.remainingCapacity()).isEqualTo(8);
    }

    @Test
    public void testPeekNoSuchReader() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        Assertions.assertThatThrownBy(() -> rb.poll(2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPeekEmptyReader() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        Assertions.assertThat(rb.peek(1)).isNull();
    }

    @Test
    public void testPeekNonEmptyReader() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        rb.add(1);
        Assertions.assertThat(rb.peek(1)).isEqualTo(1);
    }

    @Test
    public void testReaderSizeNoSuchReader() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        
    }

    @Test
    public void testReaderSizeEmpty() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        Assertions.assertThat(rb.size(1)).isEqualTo(0);
    }

    @Test
    public void testReaderSizeNonEmpty() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        rb.add(1);
        Assertions.assertThat(rb.size(1)).isEqualTo(1);
    }

    @Test
    public void testRegisterReaderAgain() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        Assertions.assertThatThrownBy(() -> rb.registerReader(1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testUnregisterReader() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        rb.registerReader(1);
        rb.unregisterReader(1);
        Assertions.assertThatThrownBy(() -> rb.size(1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testCapacity() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        Assertions.assertThat(rb.capacity()).isEqualTo(1);
    }

    @Test
    public void testSizeNoReaders() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(1);
        Assertions.assertThat(rb.size()).isEqualTo(0);
    }

    @Test
    public void testSizeEmpty() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(16);
        rb.registerReader(1);
        Assertions.assertThat(rb.size()).isEqualTo(0);
    }

    @Test
    public void testSizeOneReaderNonEmpty() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(16);
        rb.registerReader(1);
        for (int i = 0; i < 16; i++) {
            rb.add(i);
        }
        Assertions.assertThat(rb.size()).isEqualTo(16);
        for (int i = 0; i < 16; i++) {
            Assertions.assertThat(rb.poll(1)).isEqualTo(i);
            Assertions.assertThat(rb.size()).isEqualTo(15 - i);
        }
    }

    @Test
    public void testRegisterReaderWithBookmark() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(8);
        rb.registerReader(1);
        rb.add(1);
        rb.add(2);
        Assertions.assertThat(rb.poll(1)).isEqualTo(1);
        rb.registerReader(2, 1);
        Assertions.assertThat(rb.poll(1)).isEqualTo(2);
        Assertions.assertThat(rb.poll(2)).isEqualTo(2);
    }

    @Test
    public void testSizeTwoReaders() {
        RingBuffer<Integer, Integer> rb = new RingBuffer<>(16);
        rb.registerReader(1);
        rb.registerReader(2);
        Assertions.assertThat(rb.size()).isEqualTo(0);
        for (int i = 0; i < 16; i++) {
            rb.add(i);
        }
        Assertions.assertThat(rb.size()).isEqualTo(16);
        for (int i = 0; i < 16; i++) {
            Assertions.assertThat(rb.poll(1)).isEqualTo(i);
            Assertions.assertThat(rb.size()).isEqualTo(16);
        }
        for (int i = 0; i < 16; i++) {
            Assertions.assertThat(rb.poll(2)).isEqualTo(i);
            Assertions.assertThat(rb.size()).isEqualTo(15 - i);
        }
    }
}
