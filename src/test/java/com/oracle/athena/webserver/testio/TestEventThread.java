package com.oracle.athena.webserver.testio;

import com.oracle.athena.webserver.manual.WebServerTest;
import com.oracle.athena.webserver.niosockets.EventPollThread;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

public class TestEventThread implements EventPollThread {

    private static final Logger LOG = LoggerFactory.getLogger(TestEventThread.class);

    private static final int TEST_IO_GENERATORS = 10;

    private final WebServerTest webServerTest;

    private final int eventPollThreadBaseId;

    private final LinkedList<IoInterface> freeConnections;
    private final LinkedList<Operation> waitingForConnections;

    private volatile boolean threadRunning;

    public TestEventThread(final int threadBaseId, final WebServerTest webServerTest) {
        this.eventPollThreadBaseId = threadBaseId;
        this.webServerTest = webServerTest;

        this.freeConnections = new LinkedList<>();
        this.waitingForConnections = new LinkedList<>();

        this.threadRunning = true;
    }


    /*
     ** Setup the Thread to handle the event loops for testing
     */
    public void start() {
        /*
         ** Create a collection of TestIoGenerator to fill in data for the test harness
         */
        for (int i = 0; i < TEST_IO_GENERATORS; i++) {
            TestIoGenerator connection = new TestIoGenerator(webServerTest);

            freeConnections.add(connection);
        }

    }

    /*
     ** Shutdown the Thread used to handle the event loop
     */
    public void stop() {
        /*
         ** Remove all the entries on the freeConnections list
         */
        int numFreeConnections = freeConnections.size();
        if (numFreeConnections != TEST_IO_GENERATORS) {
            System.out.println(" numFreeConnections: " + numFreeConnections + " expected TEST_IO_GENERATORS: " +
                    TEST_IO_GENERATORS);
        }
        freeConnections.clear();

        threadRunning = false;

    }

    /*
     ** TODO: Wire in the wakeup of the waitingOperation if there are no NioSocket
     **   available and add a test for this
     */
    public IoInterface allocateConnection(final Operation waitingOperation) {
        IoInterface connection =  freeConnections.poll();
        if (connection == null) {
            waitingForConnections.add(waitingOperation);
        }

        return connection;
    }

    public void releaseConnection(final IoInterface connection) {
        freeConnections.add(connection);

        Operation waitingOperation = waitingForConnections.poll();
        if (waitingOperation != null) {
            waitingOperation.event();
        }
    }

    /*
     **
     */
    public void handleRead(final IoInterface connection) {

    }

    /*
     **
     */
    public void handleWrite(final IoInterface connection) {

    }

    public void closeConnection(final IoInterface connection) {

    }

}
