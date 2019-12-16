package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.connectionstate.BufferState;

public class ParseHttpRequest implements Operation {

    BufferManager bufferManager;
    BufferManagerPointer httpBufferPointer;

    public void initialize() {

        /*
        ** Register this as dependent upon the read Operation registration
         */
    }

    public void eventHandler() {

        /*
        ** Add this to the execute queue if it is not already on it.
         */
    }

    public void execute() {
        BufferState bufferState;

        while ((bufferState = bufferManager.poll(httpBufferPointer)) != null) {
            /*
            ** Now run the Buffer State through the Http Parser
             */
        }
    }

    public void complete() {

    }
}
