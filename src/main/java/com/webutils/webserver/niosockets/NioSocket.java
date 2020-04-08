package com.webutils.webserver.niosockets;

import com.webutils.webserver.buffermgr.BufferAssociation;
import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.EmptyStackException;
import java.util.Stack;

/*
** This class is responsible for handling the socket connection
 */
public class NioSocket implements IoInterface {

    private static final Logger LOG = LoggerFactory.getLogger(NioSocket.class);

    /*
    ** This connection being managed by this object is associated at startup with a particular thread
    **   which in turn means there is a Selector() loop that this must use. The Selector() loop
    **   is controlled via the NioSelectHandler object.
     */
    private NioSelectHandler nioSelectHandler;

    private SelectionKey key;

    /*
    ** This is the connection being managed
     */
    private SocketChannel socketChannel;

    /*
    ** The following are how read buffers are obtained and used.
    ** The free buffer is pointed to by the readPointer. To obtain the buffer to read data in,
    **   the user will call readBufferManager.peek(readPointer) to obtain the buffer. The pointers within the
    **   ByteBuffer will allow the buffer to have data read into it multiple times (the position() index will get
    **   updated as data is placed in the ByteBuffer) if desired. When the buffer is either full or no more data
    **   will be read into it, then the filler of the buffer will need to call
    **   readBufferManager.updateProducerWritePointer(readPointer) to indicate the buffer is ready to be consumed.
    **   That will update the consumers of the data via events.
    **
    ** NOTE: This design implies that the ByteBuffer can only be filled by a single producer that must be single
    **   single threaded. The design does not allow for multiple threads performing SocketChannel.read() into the
    **   ByteBuffer.
     */
    private BufferManager readBufferManager;
    private BufferManagerPointer readPointer;

    private BufferManager writeBufferManager;
    private BufferManagerPointer writePointer;

    /*
    ** This is the Operation to call the event() handler on if there is an error either setting up
    **   or while using the socket (i.e. the other side disconnected).
     */
    private Operation socketErrorHandler;

    /*
    ** This is the Operation that will be called when a Connect() occurs for a client socket. This is
    **   setup through the startInitiator() call for NIO SocketChannel.
     */
    private Operation connectCompleteHandler;

    /*
    ** The Stack is used to allow different BufferManagers to be used to perform reads and writes
     */
    private final Stack<BufferAssociation> readBufferAssociations;
    private final Stack<BufferAssociation> writeBufferAssociations;

    private int writePostion;

    private int currentInterestOps;

    public NioSocket(final NioSelectHandler nioSelectHandler) {

        this.nioSelectHandler = nioSelectHandler;
        this.key = null;

        this.readBufferManager = null;
        this.writeBufferManager = null;
        this.readPointer = null;
        this.writePointer = null;

        readBufferAssociations = new Stack<>();
        writeBufferAssociations = new Stack<>();

        writePostion = 0;

        currentInterestOps = 0;
    }

    /*
    ** This is the actual method to call to start all of the processing threads for a TCP port. The SocketChannel
    **   is assigned as part of the startClient() method to allow the NioSocket objects to be allocated out of a
    **   pool if so desired.
     */
    public void startClient(final SocketChannel socket) {
        socketChannel = socket;
        this.nioSelectHandler.addNioSocketToSelector(this);
    }

    public void startClient(final String readFileName, final Operation errorHandler) {
        /*
        ** This is not used for the NIO based I/O
         */
    }

    public void registerClientErrorHandler(final Operation clientErrorHandler) {
        socketErrorHandler = clientErrorHandler;
    }

    /*
    ** The startInitiator() call is used to open up a connection to (at least initially) write data out of. This
    **   requires opening a connection and attaching it to a remote listener.
     */
    public boolean startInitiator(final InetAddress targetAddress, final int targetPort, final Operation connectComplete,
                                  final Operation errorHandler) {

        boolean success = true;

        this.connectCompleteHandler = connectComplete;
        this.socketErrorHandler = errorHandler;

        InetSocketAddress socketAddress = new InetSocketAddress(targetAddress, targetPort);

        LOG.info("startInitiator() addr: " + socketAddress.toString());

        try {
            socketChannel = SocketChannel.open();
        } catch (IOException io_ex) {
            /*
            ** What to do if the socket cannot be opened
             */
            LOG.warn("Unable to open SocketChannel " + io_ex.getMessage());
            errorHandler.event();
            return false;
        }

        /*
        ** This is in a separate try{} so that the socketChannel can be closed it if
        **   fails.
         */
        try {
            socketChannel.configureBlocking(false);

            socketChannel.connect(socketAddress);
        } catch (IOException io_ex) {
            try {
                socketChannel.close();
            } catch (IOException ex) {
                /*
                ** Unable to close the socket as it might have already been closed
                 */
            }
            socketChannel = null;

            errorHandler.event();
            return false;
        }

        this.nioSelectHandler.addNioSocketToSelector(this);

        /*
        ** Register with the selector for this thread to know when the connection is available to
        **   perform writes and reads.
         */
        currentInterestOps = SelectionKey.OP_CONNECT;
        key = nioSelectHandler.registerWithSelector(socketChannel, SelectionKey.OP_CONNECT, this);
        if (key == null) {
            try {
                socketChannel.close();
            } catch (IOException ex) {
                /*
                 ** Unable to close the socket as it might have already been closed
                 */
                LOG.warn("close(1) exception: " + ex.getMessage());
            }
            socketChannel = null;
            success = false;

            errorHandler.event();
        }

        return success;
    }

    /*
    ** The following startInitiator() is not used for the NIO I/O
     */
    public boolean startInitiator(final String writeFileName, final Operation errorHandler) {
        /*
        ** Not used for NIO
         */
        return true;
    }

    /*
    ** The following is used to register with the NIO handling layer for reads. When a server connection is made, this
    **   registration is used to know where to pass the information from the socket.
    ** If there is currently a BufferManager/BufferManagerPointer associated with this NioSocket, it will push that
    **   combination on the readBufferAssociation stack to be pulled off when this association is removed. The idea
    **   of a stack is to allow different BufferManager(s) to be used to perform different operations.
     */
    public void registerReadBufferManager(final BufferManager readBufferMgr, final BufferManagerPointer readPtr) {

        LOG.info(" readPtr register (" + readPtr.getIdentifier() + ":" + readPtr.getOperationType() + ") bufferIndex: " +
                readPtr.getCurrIndex());

        if (readBufferManager != null) {
            LOG.info(" readPtr push (" + readPtr.getIdentifier() + ":" + readPtr.getOperationType() + ") bufferIndex: " +
                    readPtr.getCurrIndex());

            BufferAssociation assocation = new BufferAssociation(readBufferManager, readPointer);

            readBufferAssociations.push(assocation);
        }

        this.readBufferManager = readBufferMgr;
        this.readPointer = readPtr;
    }

    /*
     ** The following is used to register with the NIO handling layer for writes. When a server connection is made, this
     **   registration is used to know where to obtain the bytes to write out the SocketChannel.
     ** If there is currently a BufferManager/BufferManagerPointer associated with this NioSocket, it will push that
     **   combination on the writeBufferAssociation stack to be pulled off when this association is removed. The idea
     **   of a stack is to allow different BufferManager(s) to be used to perform different write operations.
     **   The stack is currently used for handling the HTTP Request header write for the Storage Server(s) and the
     **   write of the encrypted data. In this case, there is a small BufferManager that is used to hold the HTTP
     **   Request and once that is written, then encrypted data is written from a different BufferManager. This is
     **   done so that there is no need to interject empty ByteBuffer(s) in the ring buffer managed by the
     **   BufferManager that could be used for writing the HTTP Request and the Shaw-256 value. This allows unique
     **   information to be written to different Storage Server(s) if needed.
     */
    public void registerWriteBufferManager(final BufferManager writeBufferMgr, final BufferManagerPointer writePtr) {

        LOG.info(" writePtr register (" + writePtr.getIdentifier() + ":" + writePtr.getOperationType() + ") bufferIndex: " +
                writePtr.getCurrIndex());

        if (writeBufferManager != null) {
            LOG.info(" writePtr push (" + writePtr.getIdentifier() + ":" + writePtr.getOperationType() + ") bufferIndex: " +
                    writePtr.getCurrIndex());

            BufferAssociation assocation = new BufferAssociation(writeBufferManager, writePointer);

            writeBufferAssociations.push(assocation);
        }

        this.writeBufferManager = writeBufferMgr;
        this.writePointer = writePtr;
    }

    /*
    ** This removes the last BufferManager and BufferManagerPointer read registration from the NioSocket. If there is
    **   a BufferManager/BufferManagerPointer on the stack, then it will start using that combination for
    **   read buffers.
     */
    public void unregisterReadBufferManager() {

        LOG.info(" readPtr unregister (" + readPointer.getIdentifier() + ":" + readPointer.getOperationType() + ") bufferIndex: " +
                readPointer.getCurrIndex());

        if (!readBufferAssociations.empty()) {
            try {
                BufferAssociation association = readBufferAssociations.pop();

                readBufferManager = association.getBufferManager();
                readPointer = association.getBufferManagerPointer();

                LOG.warn(" POP readPtr unregister (" + readPointer.getIdentifier() + ":" + readPointer.getOperationType() + ") bufferIndex: " +
                        readPointer.getCurrIndex());

            } catch (EmptyStackException ex) {
                LOG.warn(" ERROR readPtr unregister (" + readPointer.getIdentifier() + ":" + readPointer.getOperationType() + ") bufferIndex: " +
                        readPointer.getCurrIndex());

                readBufferManager = null;
                readPointer = null;
            }
        } else {
            readBufferManager = null;
            readPointer = null;
        }
    }

    /*
     ** This removes the last BufferManager and BufferManagerPointer write registration from the NioSocket. If there is
     **   a BufferManager/BufferManagerPointer on the stack, then it will start using that combination for
     **   write buffers.
     */
    public void unregisterWriteBufferManager() {
        LOG.info(" writePtr unregister (" + writePointer.getIdentifier() + ":" + writePointer.getOperationType() + ") bufferIndex: " +
                writePointer.getCurrIndex());

        if (!writeBufferAssociations.empty()) {
            try {
                BufferAssociation association = writeBufferAssociations.pop();

                writeBufferManager = association.getBufferManager();
                writePointer = association.getBufferManagerPointer();

                LOG.info(" POP writePtr unregister (" + writePointer.getIdentifier() + ":" + writePointer.getOperationType() + ") bufferIndex: " +
                        writePointer.getCurrIndex());

            } catch (EmptyStackException ex) {
                LOG.warn(" ERROR writePtr unregister (" + writePointer.getIdentifier() + ":" + writePointer.getOperationType() + ") bufferIndex: " +
                        writePointer.getCurrIndex());

                writeBufferManager = null;
                writePointer = null;
            }
        } else {
            writeBufferManager = null;
            writePointer = null;
        }
    }

    /*
    ** Update the key interest ops
     */
    public boolean updateInterestOps() {
        if (currentInterestOps != 0) {
            if (key == null) {
                key = nioSelectHandler.registerWithSelector(socketChannel, currentInterestOps, this);
            } else {
                /*
                 ** Use the key again
                 */
                LOG.info("currentInterestOps: " + currentInterestOps);
                try {
                    key.interestOps(currentInterestOps);
                } catch (CancelledKeyException ex) {
                    LOG.error("NioSocket updateInterestOps() currentInterestOps: " + currentInterestOps + " exception: " +
                            ex.getMessage());
                    return false;
                }
            }
        } else {
            removeKey();
            return false;
        }

        return true;
    }

    /*
     ** This is called when there is a buffer in the BufferManager that is ready to accept data from
     **   the SocketChannel.
     ** It sets the OP_READ flag for the Selector that is managed by the NioSelectHandler object.
     */
    public void readBufferAvailable() {
        LOG.info(" readBufferAvailable (" + readPointer.getIdentifier() + ":" + readPointer.getOperationType() + ") bufferIndex: " +
                readPointer.getCurrIndex() + " interestOps: " + currentInterestOps);

        currentInterestOps |= SelectionKey.OP_READ;
    }

    /*
    ** This is called from the Select loop for the OP_READ case. The Select loop runs within the NioSelectHandler
    **   object and runs within the context of one of the NioEventPollThreads. There is one Selector that handles
    **   multiple NioSocket(s). The NioSocket can be a target or a server (initiator when communicating with the
    **   Storage Server).
     */
    public int performRead() {
        ByteBuffer readBuffer;

        /*
        ** Clear out the OP_READ flag
         */
        currentInterestOps &= (SelectionKey.OP_CONNECT | SelectionKey.OP_WRITE);
        while ((readBuffer = readBufferManager.peek(readPointer)) != null) {

            LOG.info(" read(" + readPointer.getIdentifier() + ":" + readPointer.getOperationType() + ") bufferIndex: " +
                    readPointer.getCurrIndex() + " position: " + readBuffer.position() + " limit: " + readBuffer.limit());

            try {
                int bytesRead = socketChannel.read(readBuffer);

                LOG.info(" read(" + readPointer.getIdentifier() + ":" + readPointer.getOperationType() + ") bufferIndex: " +
                        readPointer.getCurrIndex() + " position: " + readBuffer.position() + " bytesRead: " + bytesRead);
                if (bytesRead > 0) {
                    /*
                     ** Update the pointer and the number of bytes actually read into the buffer.
                     */
                    readBuffer.limit(bytesRead);
                    readBuffer.rewind();

                    readBufferManager.updateProducerWritePointer(readPointer);
                } else if (bytesRead == 0) {
                    currentInterestOps |= SelectionKey.OP_READ;
                } else {
                    /*
                    ** Need to close the SocketChannel and event() the error handler.
                     */
                    LOG.warn("read(" + readPointer.getIdentifier() + ":" + readPointer.getOperationType() + ") bufferIndex: " +
                            readPointer.getCurrIndex() + " bytesRead -1");
                    closeConnection();
                    sendErrorEvent();
                    break;
                }
            } catch (IOException io_ex) {
                LOG.error(" (" + readPointer.getIdentifier() + ":" + readPointer.getOperationType() + ") bufferIndex: " +
                        readPointer.getCurrIndex() + " exception: " + io_ex.getMessage());
                closeConnection();
                sendErrorEvent();
                break;
            }

        }

        return currentInterestOps;
    }

    /*
    ** This is called when there is a buffer in the BufferManager with data that is ready to be written out
    **   the SocketChannel. It sets the OP_WRITE flag for the Selector that is managed by the NioSelectHandler
    **   object.
     */
    public void writeBufferReady() {
        LOG.info(" writeBufferReady (" + writePointer.getIdentifier() + ":" + writePointer.getOperationType() + ") bufferIndex: " +
                writePointer.getCurrIndex() + " interestOps: " + currentInterestOps);

        currentInterestOps |= SelectionKey.OP_WRITE;
    }

    /*
     ** This is called from the Select loop (really the handleSelector method) from within NioSelectHandler for the
     **   OP_WRITE case.
     */
    public int performWrite() {
        ByteBuffer writeBuffer;

        if (writePointer == null) {
            LOG.error("null writePointer socketChannel: " + socketChannel.toString());
        }

        /*
         ** Clear out the OP_WRITE flag
         */
        currentInterestOps &= (SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
        while ((writeBuffer = writeBufferManager.peek(writePointer)) != null) {

            LOG.info(" write (" + writePointer.getIdentifier() + ":" + writePointer.getOperationType() + ") bufferIndex: " +
                    writePointer.getCurrIndex() + " position: " + writeBuffer.position() + " limit: " + writeBuffer.limit());

            /*
            ** If the same ByteBuffer is going to be written to multiple places, then there must be a duplicate() made
            **   and the position() must be kept on a per writer basis. Otherwise, the first writer will change the
            **   "base" ByteBuffer's position() and the following writers will do nothing.
             */
            ByteBuffer tempBuffer = writeBuffer.duplicate();
            tempBuffer.position(writePostion);

            try {
                int bytesWritten = socketChannel.write(tempBuffer);

                if (bytesWritten > 0) {
                    LOG.info(" write (" + writePointer.getIdentifier() + ":" + writePointer.getOperationType() + ") bytesWritten: " +
                            bytesWritten + " position: " + tempBuffer.position() + " reamining: " + tempBuffer.remaining());

                    /*
                     ** Update the pointer if the entire buffer was written out
                     */
                    if (tempBuffer.remaining() == 0) {
                        writePostion = 0;
                        writeBufferManager.updateProducerWritePointer(writePointer);
                    } else {
                        /*
                         ** Need to set the OP_WRITE flag and try again later when the Select loop fires
                         */
                        writePostion = tempBuffer.position();
                        currentInterestOps |= SelectionKey.OP_WRITE;
                        break;
                    }
                } else if (bytesWritten == 0) {
                    currentInterestOps |= SelectionKey.OP_WRITE;
                } else {
                    /*
                     ** Need to close the SocketChannel and event() the error handler.
                     */
                    LOG.warn(" (" + writePointer.getIdentifier() + ":" + writePointer.getOperationType() + ") bufferIndex: " +
                            writePointer.getCurrIndex() + " bytesWritten -1");
                    closeConnection();
                    sendErrorEvent();
                    writePostion = 0;
                    break;
                }
            } catch (IOException io_ex) {
                LOG.error(" (" + writePointer.getIdentifier() + ":" + writePointer.getOperationType() + ") bufferIndex: " +
                        writePointer.getCurrIndex() + " exception: " + io_ex.getMessage());
                closeConnection();
                sendErrorEvent();
                writePostion = 0;
                break;
            }
        }

        return currentInterestOps;
    }

    /*
    ** This is used when there are no outstanding SelectionKey interest ops to look at
     */
    public void removeKey() {
        if (key != null) {
            LOG.info("NioSocket removeKey()");

            key.attach(null);
            key.cancel();
            key = null;
        }
    }

    /*
    ** The following is used to force the NIO socket to be closed and release the resources associated with that
    **   socket.
     */
    public void closeConnection() {

        LOG.info("NioSocket closeConnection()");
        removeKey();

        try {
            socketChannel.close();
        } catch (IOException io_ex) {
            LOG.warn("close(2) exception: " + io_ex.getMessage());
        }

        socketChannel = null;

        nioSelectHandler.removeNioSocketFromSelector(this);
        nioSelectHandler = null;
    }

    /*
    ** Accessor method to call the Operation that is setup to handle when there is an error on
    **   the SocketChannel.
     */
    public void sendErrorEvent() {
        /*
        ** First remove the Key so it can no longer be used.
         */
        removeKey();

        socketChannel = null;

        /*
        ** Then make sure this NioSocket is not on the lsit to have work performed. It it is not removed, there will
        **   likely be a CancelledKeyException when trying to us it.
         */
        nioSelectHandler.removeNioSocketFromSelector(this);
        nioSelectHandler = null;

        socketErrorHandler.event();
    }

    /*
    ** This is called when the OP_CONNECT flag is set within the Selector. This means that the initiator SocketChannel
    **   has connected to the remote socket and is ready to start handling data transfers. This provides the
    **   Operation a clean indication that writes or reads on the NioSocket can begin. For the case where the
    **   initiator is to start sending a Chunk to a Storage Server, this means that the HTTP Request can be sent.
     */
    public void connectComplete() {
        /*
        ** Clear out the OP_CONNECT SelectionKey
         */
        currentInterestOps &= (SelectionKey.OP_WRITE | SelectionKey.OP_READ);
        if (connectCompleteHandler != null) {
            connectCompleteHandler.event();
        } else {
            LOG.warn("No connect complete handler registered");
        }
    }

    public String getIdentifierInfo() {

        int port;
        try {
            SocketAddress address = socketChannel.getLocalAddress();
            port = ((InetSocketAddress) address).getPort();
        } catch (IOException io_ex) {
            port = 0;
        }

        System.out.println("SocketChannel " + socketChannel.toString() + " listeningPort: " + port);

        return Integer.toString(port);
    }

    public String getAddressInfo() {
        String addressStr;

        try {
            SocketAddress localAddress = socketChannel.getLocalAddress();
            SocketAddress remoteAddress = socketChannel.getRemoteAddress();

            if ((localAddress != null) && (remoteAddress != null)) {
                addressStr = "localAddress: " + localAddress.toString() + " - remoteAddress: " + remoteAddress.toString();
            } else if (localAddress != null) {
                addressStr = "localAddress: " + localAddress.toString() + " - remoteAddress: null";
            } else if (remoteAddress != null) {
                addressStr = "localAddress: null" + " - remoteAddress: " + remoteAddress.toString();
            } else {
                addressStr = "localAddress: null - remoteAddress: null";
            }
        } catch (IOException io_ex) {
            addressStr = "getAddressInfo() exception: " + io_ex.getMessage();
        }

        return addressStr;
    }
}

