package com.oracle.athena.webserver.buffermgr;

import com.oracle.athena.webserver.operations.Operation;

import java.util.LinkedList;
import java.util.ListIterator;

public class BufferManagerPointer {

    private final Operation operation;
    private final int bufferArraySize;

    /*
    ** Buffer index is either the readIndex for consumers or the
    **   writeIndex for producers
     */
    private int bufferIndex;


    /*
    ** Back link used to clean up any dependencies this might have on other
    **   BufferManagerPointer(s)
     */
    BufferManagerPointer ptrThisDependsOn;

    LinkedList<Operation> ptrWhoDependOnThisList;

    BufferManagerPointer(final Operation operation, final int bufferArraySize) {

        this.operation = operation;
        this.bufferArraySize = bufferArraySize;

        ptrWhoDependOnThisList = new LinkedList<>();

        this.ptrThisDependsOn = null;
        this.bufferIndex = 0;
    }

    BufferManagerPointer(final Operation operation, final BufferManagerPointer dependsOnPointer,
                         final int bufferArraySize) {

        this.operation = operation;
        this.bufferArraySize = bufferArraySize;
        this.ptrWhoDependOnThisList = new LinkedList<>();

        this.ptrThisDependsOn = dependsOnPointer;

        this.bufferIndex = this.ptrThisDependsOn.getCurrIndex();
    }

    /*
    ** For producers, this returns where the next write will take place. This index does
    **   not have valid data in it.
    **
    ** For consumers, this returns where it will read from. If the readIndex is the same as the
    **   the producers writeIndex, there is no valid data available.
     */
    int getCurrIndex() {
        return bufferIndex;
    }

    int getReadIndex(final boolean updatePointer) {
        if (ptrThisDependsOn != null) {
            if (bufferIndex != ptrThisDependsOn.getCurrIndex()) {
                int readIndex = bufferIndex;

                if (updatePointer) {
                    bufferIndex++;
                    if (bufferIndex == bufferArraySize) {
                        bufferIndex = 0;
                    }
                }

                return readIndex;
            }
        }

        return -1;
    }

    /*
    ** This returns the current write index, but does not change it. This is used to add a ByteBuffer
    **   to the BufferManager and after the buffer is added, then updateWriteIndex() should be called
    **   if the buffer is actually to be made available to the writer.
     */
    int getWriteIndex() {
        return bufferIndex;
    }

    /*
    ** This updates the producers writeIndex after data has been placed into the BufferState
    **   and it returns the location to place the next data write.
    **
    ** This will also call all the Operations eventHandlers that are registered as
    **   depending upon this producer.
     */
    int updateWriteIndex() {

        generateDependsOnEvents();

        bufferIndex++;
        if (bufferIndex == bufferArraySize) {
            bufferIndex = 0;
        }

        return bufferIndex;
    }

    /*
    ** This updates where the next read will take place for a consumer
     */
    int updateReadIndex() {

        bufferIndex++;
        if (bufferIndex == bufferArraySize) {
            bufferIndex = 0;
        }

        return bufferIndex;
    }

    void addDependsOn(final Operation operation) {
        if (ptrWhoDependOnThisList.contains(operation) == false) {
            ptrWhoDependOnThisList.add(operation);
        }
    }

    void generateDependsOnEvents() {
        ListIterator<Operation> iter = ptrWhoDependOnThisList.listIterator(0);

        while (iter.hasNext()) {
            iter.next().execute();
        }
    }

    void removeDependency(final Operation dependsOnOperation) {
        ptrWhoDependOnThisList.remove(dependsOnOperation);
    }

    void terminate() {
        if (ptrThisDependsOn != null) {
            ptrThisDependsOn.removeDependency(operation);
        }
    }
}
