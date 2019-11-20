package com.oracle.athena.webserver.server;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * This class represents an example of a Unit Test for the {@link WriteConnectionTests}. Unlike an integration tests,
 * which are denoted by *IT.java rather than *Test.java, these tests should be run during the build phase and should
 * always pass. An integration test requires some port/system resource and so has the potential to fail with some
 * increased frequency unrelated to build goals.
 */
class WriteConnectionTests {

    /**
     * This test isn't really a useful test, but does serve the purpose of showcasing how to define a unit test in
     * JUnit5. This test merely constructs a {@link ServerChannelLayer} object and verifies that its one getter provides
     * something meaningful after construction.
     *
     * @implNote @DisplayName is completely optional. It gives the report a friendly name than its default of the
     * method name. I would not expect there to be many @DisplayName annotations, but its left here as an example.
     */
    @Test
    @DisplayName("Transaction ID Retrieval")
    void canGetTransactionId() {
        final long transactionId = System.nanoTime();
        final WriteConnection writeConnection = new WriteConnection(transactionId);
        Assertions.assertEquals(transactionId, writeConnection.getTransactionId(),
                "The transaction id provided to the constructor should be the same id returned by" +
                        " writeConnection.getTransactionId().");
    }

    /**
     * Calling the {@link WriteConnection#writeAvailableData()} after constructing the object should be benign. That is,
     * calling {@link WriteConnection#writeAvailableData()} doesn't throw an unexpected error.
     */
    @Test
    void writeAvailableDataOnConstruction() {
        final long transactionId = System.nanoTime();
        final WriteConnection writeConnection = new WriteConnection(transactionId);
        writeConnection.writeAvailableData();
    }

    /**
     * Since {@link WriteConnection#closeChannel()} should be benign after the {@link WriteConnection} is constructed
     */
    @Test
    void canCloseChannelOnConstruction() {
        final long transactionId = System.nanoTime();
        final WriteConnection writeConnection = new WriteConnection(transactionId);
        writeConnection.closeChannel();
    }

    /**
     * We should be able to close {@link WriteConnection#close()} after the {@link WriteConnection} is constructed.
     */
    @Test
    void canCloseWriteConnectionAfterConstruction() {
        final WriteConnection writeConnection = new WriteConnection(System.nanoTime());
        writeConnection.close();
    }

    /**
     * {@link WriteConnection#writeData(WriteCompletion)} should be callable with a WriteCompletion object.
     */
    @Test
    void canCallWriteDataOnConstruction() {
        // note this is how we get around the "only one new() call".
        final WriteCompletion fakeWriteCompletion = Mockito.mock(WriteCompletion.class);
        final WriteConnection writeConnection = new WriteConnection(System.nanoTime());
        Assertions.assertTrue(writeConnection.writeData(fakeWriteCompletion), "Should be able to write data");
    }

    /**
     * {@link WriteConnection#writeData(WriteCompletion)} should be callable with a null WriteCompletion object.
     */
    @Test
    void canCallWriteDataWithNullOnConstruction() {
        final WriteConnection writeConnection = new WriteConnection(System.nanoTime());
        Assertions.assertTrue(writeConnection.writeData(null), "Should be able to write data");
    }

}
