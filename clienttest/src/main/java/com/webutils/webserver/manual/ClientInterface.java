package com.webutils.webserver.manual;

import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.NioCliClient;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ClientInterface {

    protected final InetAddress serverIpAddr;
    protected final int serverTcpPort;

    private boolean statusSignalSent;

    /*
     ** The runningTestCount is used by the caller to keep track of the number of tests that
     **   are currently running.
     */
    protected final AtomicInteger runningTestCount;

    private final Object writeDone;

    protected final NioCliClient client;
    protected final EventPollThread eventThread;
    protected final int eventThreadId;

    protected final MemoryManager memoryManager;

    private static final String clientName = "ObjectCLI/0.0.1";

    ClientInterface(final NioCliClient cliClient, final MemoryManager memoryManger, final InetAddress serverIpAddr, final int serverTcpPort,
                    AtomicInteger testCount) {

        this.memoryManager = memoryManger;

        this.serverIpAddr = serverIpAddr;
        this.serverTcpPort = serverTcpPort;

        this.runningTestCount = testCount;
        this.runningTestCount.incrementAndGet();

        /*
         ** The testClient is responsible for providing the threads the Operation(s) will run on and the
         **   NIO Socket handling.
         */
        this.client = cliClient;
        this.eventThread = cliClient.getEventThread();
        this.eventThreadId = this.eventThread.getEventPollThreadBaseId();

        this.writeDone = new Object();
    }

    /*
     **
     */
    public abstract void execute();

    public void clientRequestCompleted(int result) {

        System.out.println("ClientInterface[" + clientName + "] clientRequestCompleted() result: " + result);

        synchronized (writeDone) {
            statusSignalSent = true;
            writeDone.notify();
        }
    }


    protected boolean waitForRequestComplete() {
        boolean status = true;

        synchronized (writeDone) {

            statusSignalSent = false;
            while (!statusSignalSent) {
                try {
                    writeDone.wait(100);
                } catch (InterruptedException int_ex) {
                    int_ex.printStackTrace();
                    status = false;
                    System.out.println("ClientInterface[" + clientName + "] waitForRequestComplete() timed out");

                    break;
                }
            }
        }

        return status;
    }

}
