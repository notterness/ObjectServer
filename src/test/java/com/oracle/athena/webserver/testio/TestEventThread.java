package com.oracle.athena.webserver.testio;

import com.oracle.athena.webserver.manual.WebServerTest;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.niosockets.EventPollThread;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.operations.Operation;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;
import java.util.LinkedList;

public class TestEventThread implements EventPollThread {

    private static final Logger LOG = LoggerFactory.getLogger(TestEventThread.class);

    private static final int TEST_IO_GENERATORS = 10;

    private final WebServerFlavor webServerFlavor;

    private final WebServerTest webServerTest;

    private final int eventPollThreadBaseId;

    private final MemoryManager memoryManager;

    private final LinkedList<IoInterface> freeConnections;
    private final LinkedList<Operation> waitingForConnections;

    private volatile boolean threadRunning;

    public TestEventThread(final WebServerFlavor flavor, final int threadBaseId, final MemoryManager memoryManger,
                           final WebServerTest webServerTest) {

        this.webServerFlavor = flavor;
        this.eventPollThreadBaseId = threadBaseId;
        this.webServerTest = webServerTest;
        this.memoryManager = memoryManger;

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

    /*
     ** TODO: Change this to use a pool of pre-allocated RequestContext
     */
    public RequestContext allocateContext(){
        return new RequestContext(webServerFlavor, memoryManager, this);
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
