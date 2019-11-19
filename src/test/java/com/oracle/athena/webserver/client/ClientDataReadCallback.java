package com.oracle.athena.webserver.client;

import java.nio.ByteBuffer;

abstract public class ClientDataReadCallback {

    abstract public void dataBufferRead(final int result, final ByteBuffer readBuffer);

}
