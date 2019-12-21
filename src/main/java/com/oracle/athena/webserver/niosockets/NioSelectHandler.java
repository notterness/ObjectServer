package com.oracle.athena.webserver.niosockets;

import java.io.IOException;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;

public class NioSelectHandler {

    /*
    ** This is the Selector to handle a portion of the connections in the system. There is
    **   one Selector per event thread.
     */
    private Selector nioSelector;

    /*
    **
     */
    NioSelectHandler() {

    }

    /*
    ** This is how a SocketChannel is registered with the Selector()
     */
    boolean registerWithSelector(final SocketChannel socketChannel, final int interestOps, final NioSocket nioSocket) {

        try {
            SelectionKey key = socketChannel.register(nioSelector, interestOps, nioSocket);
        } catch (ClosedChannelException | ClosedSelectorException ex) {
            return false;
        }

        return true;
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

    /*
    ** This is the method to iterate over all of the active keys in the Selector and perform
    **   the ready work.
     */
    void handleSelector() {
        try {
            /*
            ** The select() cannot block since this will be called from a thread that will perform
            **   other work.
             */
            nioSelector.select(0);

            Iterator selectedKeys = nioSelector.selectedKeys().iterator();
            while (selectedKeys.hasNext()) {
                SelectionKey key = (SelectionKey) selectedKeys.next();
                selectedKeys.remove();

                if (!key.isValid()) {
                    continue;
                }

                if (key.isConnectable()) {

                }

                if (key.isReadable()) {

                }

                if (key.isWritable()) {

                }
            }
        } catch (IOException io_ex) {

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
            /*
            ** Need to close out this connection and let the user of the channel know
            **   that the connection is no longer valid.
             */
            key.cancel();

            NioSocket nioSocket = (NioSocket) key.attachment();
            if (nioSocket != null) {
                nioSocket.sendErrorEvent();
            }
        }
    }
}
