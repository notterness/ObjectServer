package com.webutils.webserver.niosockets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class NioSelectHandler {

    private static final Logger LOG = LoggerFactory.getLogger(NioSelectHandler.class);

    /*
    ** This is the Selector to handle a portion of the connections in the system. There is
    **   one Selector per event thread.
     */
    private Selector nioSelector;

    private final List<NioSocket> openSockets;

    /*
    **
     */
    NioSelectHandler() {

        openSockets = new LinkedList<>();
    }

    /*
    **
     */
    void addNioSocketToSelector(final NioSocket nioSocket) {
        if (!openSockets.contains(nioSocket)) {

            LOG.info("Adding NioSocket " + nioSocket.getAddressInfo());
            openSockets.add(nioSocket);
        }
    }

    void removeNioSocketFromSelector(final NioSocket nioSocket) {
        if (openSockets.contains(nioSocket)) {
            LOG.info("Removing NioSocket");
            openSockets.remove(nioSocket);
        } else {
            LOG.error("Removing NioSocket but not on openSockets list");
        }
    }

    /*
    ** This is how a SocketChannel is registered with the Selector()
     */
    SelectionKey registerWithSelector(final SocketChannel socketChannel, final int interestOps, final NioSocket nioSocket) {

        SelectionKey key;
        try {
            key = socketChannel.register(nioSelector, interestOps, nioSocket);
        } catch (CancelledKeyException ex) {
            LOG.warn("registerWithSelector CancelledKeyException: " + ex.getMessage());
            key = null;
        } catch (ClosedChannelException | ClosedSelectorException ex) {
            key = null;
        }

        return key;
    }

    /*
    **
     */

    /*
    ** Setup the Selector() to handle sockets
     */
    Selector setupSelector() {

        try {
            nioSelector = SelectorProvider.provider().openSelector();
        } catch (IOException io_ex) {
            nioSelector = null;
        }

        return nioSelector;
    }

    void releaseSelector() {
        try {
            nioSelector.close();
        } catch (IOException io_ex) {

        }

        nioSelector = null;
    }

    /*
    ** This is the method to iterate over all of the active keys in the Selector and perform
    **   the ready work.
     */
    void handleSelector() {
        try {
            /*
            ** First find all the SocketChannels (via the NioSocket) that are registered with this
            **   NioSelectHandler. Check if any of those SocketChannels need to update their
            **   select keys information.
             */
            boolean work = false;
            Iterator<NioSocket> currSocketsIter = openSockets.iterator();
            while (currSocketsIter.hasNext()) {
                work |= currSocketsIter.next().updateInterestOps();
            }

            /*
            ** The select() cannot block since this will be called from a thread that will perform
            **   other work.
             */
            if (work) {
                nioSelector.select(10);
            } else {
                nioSelector.select(100);
            }

            Iterator selectedKeys = nioSelector.selectedKeys().iterator();

            while (selectedKeys.hasNext()) {
                SelectionKey key = (SelectionKey) selectedKeys.next();
                selectedKeys.remove();

                if (!key.isValid()) {
                    continue;
                }

                int updatedInterestOps = 0;
                if (key.isConnectable()) {
                    handleConnect(key);
                } else {
                    /*
                    ** Only perform the following if the isConnectable() not true. Otherwise, if the
                    **   isConnectable() is set and then there is an error processing the the
                    **   handleConnect(), if it falls through and checks the isReadable() it will
                    **   get a CancelledKeyException.
                    ** Need to make sure and handle the case where the current interest ops have either
                    **   OP_READ or OP_WRITE set, but there is not a firing for that interest op. Need to
                    **   retain the interest op for the next time around.
                     */
                    if (key.isReadable()) {
                        updatedInterestOps |= handleRead(key);
                    }

                    if (key.isWritable()) {
                        updatedInterestOps |= handleWrite(key);
                    }
                }

            }
        } catch (IOException io_ex) {
            LOG.warn("NioSelectHandler handleSelector() " + io_ex.getMessage());
        }
    }

    /*
    ** This is used to handle the connect() completion for the SocketChannel being opened
     */
    private void handleConnect(final SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        try {
            socketChannel.finishConnect();
        } catch (IOException io_ex) {
            LOG.warn("handleConnect() error: " + io_ex.getMessage());

            try {
                SocketAddress addr = socketChannel.getRemoteAddress();
                LOG.warn("handleConnect() error - remoteAddress: " + addr.toString());
            } catch (IOException ex) {
                LOG.warn("handleConnect() error - unable to obtain remoteAddress " + ex.getMessage());
            }

            /*
            ** Need to close out this connection and let the user of the channel know
            **   that the connection is no longer valid.
             */
            NioSocket nioSocket = (NioSocket) key.attachment();
            if (nioSocket != null) {
                nioSocket.sendErrorEvent();
                key.attach(null);
            }
            key.cancel();


            return;
        }

        /*
        ** Call the registered ConnectComplete operation if one is registered.
         */
        NioSocket nioSocket = (NioSocket) key.attachment();
        if (nioSocket != null) {
            nioSocket.connectComplete();
        }
    }

    private int handleRead(final SelectionKey key) {
        NioSocket nioSocket = (NioSocket) key.attachment();

        return nioSocket.performRead();
    }

    private int handleWrite(final SelectionKey key) {
        NioSocket nioSocket = (NioSocket) key.attachment();

        return nioSocket.performWrite();
    }

}
