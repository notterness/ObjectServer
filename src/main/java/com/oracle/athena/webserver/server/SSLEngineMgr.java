package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.*;
import io.grpc.netty.shaded.io.netty.internal.tcnative.SSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;


public class SSLEngineMgr {
    private static final Logger LOG = LoggerFactory.getLogger(SSLEngineMgr.class);

    public static enum Status {
        FINISHED,
        BUFFER_OVERFLOW,
        BUFFER_UNDERFLOW,
        NOT_HANDSHAKING,
        CONTINUE;
    }

    private SSLContext context;
    private SSLEngine engine;
    private LinkedList<BufferState> allocatedSSLAppBufferQueue;
    private LinkedList<BufferState> allocatedSSLNetBufferQueue;
    private SSLEngineResult.HandshakeStatus handshakeStatus;

    SSLEngineMgr(SSLContext context) {
        this.context = context;
        engine = context.createSSLEngine();

        allocatedSSLAppBufferQueue = new LinkedList<>();
        allocatedSSLNetBufferQueue = new LinkedList<>();
        handshakeStatus = SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
    }

    void setUseClientMode(boolean clientMode) {
        engine.setUseClientMode(clientMode);
    }

    void setNeedClientAuth(boolean clientAuth) {
        engine.setUseClientMode(clientAuth);
    }

    public int getAppBufferSize() {
        return engine.getSession().getApplicationBufferSize();
    }

    public int getNetBufferSize() {
        return engine.getSession().getPacketBufferSize();
    }

    public BufferState allocBufferState(WebServerSSLConnState connState, BufferStatePool bufferStatePool,
                                        BufferStateEnum state) {
        int appBufferSize = engine.getSession().getApplicationBufferSize();
        int netBufferSize = engine.getSession().getPacketBufferSize();

        return bufferStatePool.allocBufferState(connState, state, appBufferSize, netBufferSize);
    }

        /*
     ** Allocate a buffers for SSL handshaking.
     ** Server and client buffers are supposed to be large enough to hold all message data the server
     ** will send and expects to receive from the client. Since the messages to be exchanged will usually be less
     ** than 16KB long the capacity of these fields should also be smaller.  Expected buffer sizes are retrieved
     ** from the session object.
     */
    public int allocSSLHandshakeBuffers(WebServerSSLConnState connState, BufferStatePool bufferStatePool) {
        int appBufferSize = engine.getSession().getApplicationBufferSize();
        int netBufferSize = engine.getSession().getPacketBufferSize();

        while (allocatedSSLAppBufferQueue.size() < WebServerSSLConnState.NUM_SSL_APP_BUFFERS) {
            BufferState bufferState = bufferStatePool.allocBufferState(connState, BufferStateEnum.SSL_HANDSHAKE_APP_BUFFER, appBufferSize);
            if (bufferState != null) {
                allocatedSSLAppBufferQueue.add(bufferState);

            } else {
                /*
                 ** Unable to allocate memory, come back later
                 */
                //bufferAllocationFailed.set(true);
                return allocatedSSLAppBufferQueue.size();
            }
        }

        while (allocatedSSLNetBufferQueue.size() < WebServerSSLConnState.NUM_SSL_NET_BUFFERS) {
            BufferState bufferState = bufferStatePool.allocBufferState(connState, BufferStateEnum.SSL_HANDSHAKE_NET_BUFFER, netBufferSize);
            if (bufferState != null) {
                allocatedSSLNetBufferQueue.add(bufferState);

            } else {
                /*
                 ** Unable to allocate memory, come back later
                 */
                //bufferAllocationFailed.set(true);
                return allocatedSSLAppBufferQueue.size() + allocatedSSLAppBufferQueue.size();
            }
        }

        //setSSLBuffersNeeded(false);
        return allocatedSSLAppBufferQueue.size() + allocatedSSLAppBufferQueue.size();
    }


    public void freeSSLHandshakeBuffers(BufferStatePool bufferStatePool) {
        ListIterator<BufferState> iterApp = allocatedSSLAppBufferQueue.listIterator(0);
        ListIterator<BufferState> iterNet = allocatedSSLNetBufferQueue.listIterator(0);

        while (iterApp.hasNext()) {
            BufferState bufferState = iterApp.next();
            iterApp.remove();

            bufferStatePool.freeBufferState(bufferState);
        }

        while (iterNet.hasNext()) {
            BufferState bufferState = iterNet.next();
            iterNet.remove();

            bufferStatePool.freeBufferState(bufferState);
        }

    }

    public SSLEngineResult unwrap(BufferState bufferState) {
        ByteBuffer clientAppData = bufferState.getBuffer();
        ByteBuffer clientNetData = bufferState.getNetBuffer();
        SSLEngineResult result = null;

        clientAppData.clear();
        clientNetData.flip();
        while (clientNetData.hasRemaining()) {

            try {
                result = engine.unwrap(clientNetData, clientAppData);
            } catch (SSLException e) {
                //TODO: Return error to client, log it
                System.out.println("Unable to unwrap data.");
                e.printStackTrace();
                return null;
            }
        }
        return result;
    }

    public void wrap(BufferState bufferState) throws IOException {
        ByteBuffer clientAppData = bufferState.getBuffer();
        ByteBuffer clientNetData = bufferState.getNetBuffer();

        while (clientAppData.hasRemaining()) {
            clientNetData.clear();
            SSLEngineResult result  = engine.wrap(clientAppData, clientNetData);
            switch (result.getStatus()) {
                case OK:
                    System.out.println("Wrapped data.");
                    return;
                case BUFFER_OVERFLOW:
                    // FIXME: need to get a bigger buffer
                    break;
                case CLOSED:
                    //FIXME: ("Client wants to close connection...");
                    return;
                default:
                    //Log this state
                    System.out.println("Illegal state: " + result.getStatus().toString());
                    break;
            }
        }
    }



    public void beginHandshake() throws SSLException {
        engine.beginHandshake();
//        setSSLHandshakeRequired(true);
//        setSSLHandshakeSuccess(false);
        handshakeStatus = engine.getHandshakeStatus();
    }

    /*
     * Implements the handshake protocol between two peers, required for the establishment of the SSL/TLS connection.
     * During the handshake, encryption configuration information - such as the list of available cipher suites - will be exchanged
     * and if the handshake is successful will lead to an established SSL/TLS session.
     *
     * A typical handshake will usually contain the following steps:
     *
     *   1. wrap:     ClientHello
     *   2. unwrap:   ServerHello/Cert/ServerHelloDone
     *   3. wrap:     ClientKeyExchange
     *   4. wrap:     ChangeCipherSpec
     *   5. wrap:     Finished
     *   6. unwrap:   ChangeCipherSpec
     *   7. unwrap:   Finished
     *
     * Handshake is also used during the end of the session, in order to properly close the connection between the two peers.
     * A proper connection close will typically include the one peer sending a CLOSE message to another, and then wait for
     * the other's CLOSE message to close the transport link. The other peer from his perspective would read a CLOSE message
     * from his peer and then enter the handshake procedure to send his own CLOSE message as well.
     *
     */
    public Status doSSLHandshake(WebServerSSLConnState conn) {
        SSLEngineResult sslEngineResult;
        ListIterator<BufferState> iterApp = allocatedSSLAppBufferQueue.listIterator(0);
        ListIterator<BufferState> iterNet = allocatedSSLNetBufferQueue.listIterator(0);
        ByteBuffer clientAppData = iterApp.next().getBuffer();
        ByteBuffer clientNetData = iterNet.next().getBuffer();
        ByteBuffer serverAppData = iterApp.next().getBuffer();
        ByteBuffer serverNetData = iterNet.next().getBuffer();

        switch (handshakeStatus) {
            case NEED_UNWRAP:
                Future<Integer> rd = conn.readFromChannelFuture(clientNetData);
                Integer res = -1;
                try {
                    res = rd.get();
                } catch (InterruptedException e) {
                    // TODO: logging
                } catch (ExecutionException e) {
                    // TODO: logging
                }
                if (res < 0) {
                    if (engine.isInboundDone() && engine.isOutboundDone()) {
                        //TODO: is this the right action
                        return Status.CONTINUE;
                    }
                    try {
                        engine.closeInbound();
                    } catch (SSLException e) {
                    }
                    engine.closeOutbound();
                    // After closeOutbound the engine will be set to WRAP state, in order to try to send a close message to the client.
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                }
                clientNetData.flip();
                try {
                    sslEngineResult = engine.unwrap(clientNetData, clientAppData);
                    clientNetData.compact();
                    handshakeStatus = sslEngineResult.getHandshakeStatus();
                } catch (SSLException sslException) {
                    engine.closeOutbound();
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                }
                switch (sslEngineResult.getStatus()) {
                    case OK:
                        break;
                    case BUFFER_OVERFLOW:
                        // Will occur when clientAppData's capacity is smaller than the data derived from clientNetData's unwrap.
                        //FIXME: handle this condition
                        break;
                    case BUFFER_UNDERFLOW:
                        // Will occur either when no data was read from the client or when the clientNetData buffer was too small to hold all client's data.
                        //FIXME: handle this condition
                        break;
                    case CLOSED:
                        if (engine.isOutboundDone()) {
                            //TODO: is this the right action
                            return Status.CONTINUE;
                        } else {
                            engine.closeOutbound();
                            handshakeStatus = engine.getHandshakeStatus();
                            break;
                        }
                    default:
                        throw new IllegalStateException("Invalid SSL status: " + sslEngineResult.getStatus());
                }
                break;
            case NEED_WRAP:
                serverNetData.clear();
                try {
                    sslEngineResult = engine.wrap(serverAppData, serverNetData);
                    handshakeStatus = sslEngineResult.getHandshakeStatus();
                } catch (SSLException sslException) {
                    engine.closeOutbound();
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                }
                switch (sslEngineResult.getStatus()) {
                    case OK :
                        serverNetData.flip();
                        while (serverNetData.hasRemaining()) {
                            Future <Integer> wr = conn.writeToChannelFuture(serverNetData);
                            try {
                                wr.get();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (ExecutionException e) {
                                e.printStackTrace();
                            }
                        }
                        break;
                    case BUFFER_OVERFLOW:
                        // Will occur if there is not enough space in serverNetData buffer to write all the data that would be generated by the method wrap.
                        // Since serverNetData is set to session's packet size we should not get to this point because SSLEngine is supposed
                        // to produce messages smaller or equal to that, but a general handling would be the following:
                        //FIXME: handle this condition
                        return Status.BUFFER_OVERFLOW;
                    case BUFFER_UNDERFLOW:
                        //Buffer underflow occurred after a wrap. I don't think we should ever get here
                        //FIXME: handle this condition
                        return Status.BUFFER_UNDERFLOW;
                    case CLOSED:
                        try {
                            serverNetData.flip();
                            while (serverNetData.hasRemaining()) {
                                Future <Integer> wr = conn.writeToChannelFuture(serverNetData);
                                try {
                                    wr.get();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                    //FIXME: handle this condition
                                } catch (ExecutionException e) {
                                    e.printStackTrace();
                                    //FIXME: handle this condition
                                }
                            }
                            // At this point the handshake status will probably be NEED_UNWRAP so we make sure that peerNetData is clear to read.
                            clientNetData.clear();
                        } catch (Exception e) {
                            handshakeStatus = engine.getHandshakeStatus();
                        }
                        break;
                    default:
                        throw new IllegalStateException("Invalid SSL status: " + sslEngineResult.getStatus());
                }
                break;
            case NEED_TASK:
                Runnable task;
                while ((task = engine.getDelegatedTask()) != null) {
                    System.out.println("run task " + task);
                    task.run();
                }
                handshakeStatus = engine.getHandshakeStatus();
                break;
            case FINISHED:
                return Status.FINISHED;
            case NOT_HANDSHAKING:
                // FIXME: handle this condition
                // setSSLHandshakeRequired(false);
                return Status.NOT_HANDSHAKING;
            default:
                throw new IllegalStateException("Invalid SSL status: " + handshakeStatus);
        }

        return Status.CONTINUE;
    }

}
