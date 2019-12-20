package com.oracle.athena.webserver.buffermgr;

import com.oracle.athena.webserver.operations.Operation;
import com.oracle.athena.webserver.operations.OperationTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.ListIterator;

public class BufferManagerPointer {

    private static final Logger LOG = LoggerFactory.getLogger(BufferManager.class);

    private final Operation operation;
    private final int bufferArraySize;

    private final int identifier;

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

    BufferManagerPointer(final Operation operation, final int bufferArraySize, final int identifier) {

        this.operation = operation;
        this.bufferArraySize = bufferArraySize;
        this.identifier = identifier;

        ptrWhoDependOnThisList = new LinkedList<>();

        this.ptrThisDependsOn = null;
        this.bufferIndex = 0;
    }

    BufferManagerPointer(final Operation operation, final BufferManagerPointer dependsOnPointer,
                         final int bufferArraySize, final int identifier) {

        this.operation = operation;
        this.bufferArraySize = bufferArraySize;
        this.ptrWhoDependOnThisList = new LinkedList<>();
        this.identifier = identifier;

        this.ptrThisDependsOn = dependsOnPointer;

        this.bufferIndex = this.ptrThisDependsOn.getCurrIndex();
    }

    /*
    ** A useful debug routine to show dependency trees
     */
    OperationTypeEnum getOperationType() {
        return operation.getOperationType();
    }

    /*
    **
     */
    int getIdentifier() {
        return identifier;
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
            } else {
                LOG.error("Consumer(" + identifier + ":" + getOperationType() + ") bufferIndex: " + bufferIndex +
                        " producer(" + ptrThisDependsOn.getIdentifier() + ":" + ptrThisDependsOn.getOperationType() + ") writeIndex: " +
                        ptrThisDependsOn.getCurrIndex());
            }
        } else {
            LOG.error("Consumer("  + identifier + ":" + getOperationType() + ") must have a depends on relationship");
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

        //LOG.info("Producer("  + identifier + ":" + getOperationType() + ") writeIndex: " + bufferIndex);

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
            iter.next().event();
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
