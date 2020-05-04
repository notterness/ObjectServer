package com.webutils.webserver.testio;

import com.webutils.webserver.manual.WebServerTest;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;
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

    public int getEventPollThreadBaseId() {
        return eventPollThreadBaseId;
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

    public void releaseContext(final RequestContext requestContext) {

    }

    /*
    ** This is not used for the TestEventThread. The data is all generated directly through the passed in
    **   TestIoGenerator.
     */
    public boolean registerClientSocket(final SocketChannel clientChannel) {
        return true;
    }

    public boolean runComputeWork(final Operation computeOperation) { return false; }

    public void removeComputeWork(final Operation computeOperation) {}
}
