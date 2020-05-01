package com.webutils.webserver.buffermgr;

import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntBinaryOperator;

/*
** The BufferManagerPointer is used to allow multiple Producers (Operations that produce data) and multiple
**   Consumers (Operations that perform work on data that has been produced) to use the same
**   BufferManager.
** The BufferManagerPointer allows dependencies to be setup between Producers and Consumers. This allows
**   multiple Consumers to be notified when a Producer has placed data into the BufferManager.
**
** A good example of a Producer/Consumer dependency is the NIO code which reads data off the wire and places
**   it into a ByteBuffer (that is the Producer) and the HTTP Parser (the Consumer) which uses the data in
**   in the ByteBuffer to parse out the URI and the HTTP headers.
*
** The BufferManagerPointer uses an AtomicInteger for its current location (bufferIndex) in the ring buffer. In
**   actuality, this is a performance issue as the access to the AtomicInteger is slower than accessing an
**   int. But, by having an AtomicInteger this allows the BufferManager to be used from different threads to
**   access the same ring buffer.
 */
public class BufferManagerPointer {

    private static final Logger LOG = LoggerFactory.getLogger(BufferManager.class);

    private Operation operation;
    private final int bufferArraySize;

    private final int identifier;

    /*
    ** Buffer index is either the readIndex for consumers or the writeIndex for producers.
     */
    private AtomicInteger bufferIndex;

    /*
    ** The computeNextIndex is used in the computing of the bufferIndex and handling the wrap condition
    **   in the ring buffer. This is used in the AtomicInteger.accumulateAndGet() and
    **   AtomicInteger.getAndAccumulate() methods.
     */
    private IntBinaryOperator computeNextIndex = (curr, maxSize) -> (((curr + 1) == maxSize) ? 0 : (curr + 1));

    private int bookmark;

    /*
    ** The following are used for pointers that are only going to consume a specified number of
    **   bytes of data and then stop.
     */
    private final int maxBytesToConsume;
    private int bytesConsumed;


    /*
    ** Back link used to clean up any dependencies this might have on other
    **   BufferManagerPointer(s)
     */
    BufferManagerPointer ptrThisDependsOn;
    LinkedBlockingQueue<BufferManagerPointer> dependentPointers;
    LinkedBlockingQueue<Operation> ptrWhoDependOnThisList;

    /*
    ** The following constructor is used by Producers.
    ** When a Producer is first setup, it starts at the beginning of the BufferManager and will
    **   setup a few other pieces of information to allow dealing with the fact that the
    **   BufferManager is implemented as a ring buffer.
     */
    BufferManagerPointer(final Operation operation, final int bufferArraySize, final int identifier) {

        this.operation = operation;
        this.bufferArraySize = bufferArraySize;
        this.identifier = identifier;

        dependentPointers = new LinkedBlockingQueue<>();
        ptrWhoDependOnThisList = new LinkedBlockingQueue<>();

        this.bookmark = -1;
        this.ptrThisDependsOn = null;
        this.bufferIndex = new AtomicInteger(0);

        this.maxBytesToConsume = -1;
        this.bytesConsumed = 0;
    }

    /*
    ** The following constructor is used by Consumers.
    **
    ** TODO: The bufferArraySize should be pulled from the dependsOnPointer to insure there are
    **   no mismatches.
    *
    ** TODO: Add a enum that identifies the type of BufferManagerPointer as either a Producer
    **   or a Consumer. Once that is done validate the a Consumer is never setting up a
    **   dependency on another Consumer.
     */
    BufferManagerPointer(final Operation operation, final BufferManagerPointer dependsOnPointer,
                         final int bufferArraySize, final int identifier, final int maxBytesToConsume) {

        this.operation = operation;
        this.bufferArraySize = bufferArraySize;
        dependentPointers = new LinkedBlockingQueue<>();
        this.ptrWhoDependOnThisList = new LinkedBlockingQueue<>();
        this.identifier = identifier;

        this.bookmark = -1;
        this.ptrThisDependsOn = dependsOnPointer;

        this.maxBytesToConsume = maxBytesToConsume;
        this.bytesConsumed = 0;

        /*
        ** Add the Operation to the depends on list for the producer
         */
        if (operation != null) {
            dependsOnPointer.addDependsOn(operation);
        }

        /*
        ** Add the consumer BufferManagerPointer (this) to the depends on list for the producer. The
        **   list of consumer pointers is to insure that the producer does not catch up and
        **   overwrite a consumer.
         */
        dependsOnPointer.addDependsOn(this);


        int dependsOnBookmark = this.ptrThisDependsOn.getBookmark();
        if (dependsOnBookmark == -1) {
            this.bufferIndex = new AtomicInteger(this.ptrThisDependsOn.getCurrIndex());
        } else {
            this.bufferIndex = new AtomicInteger(dependsOnBookmark);

            /*
             ** Since this registration has a dependency, check if the event() for the operation should be
             **   called.
             */
            if ((dependsOnBookmark != this.ptrThisDependsOn.getCurrIndex()) && (operation != null)) {
                operation.event();
            }
        }
    }

    /*
    ** The normal case for Consumers is they consume data until there is no more. This means the
    **   maxBytesToConsume is not passed in to the constructor.
     */
    BufferManagerPointer(final Operation operation, final BufferManagerPointer dependsOnPointer,
                         final int bufferArraySize, final int identifier) {
        this(operation, dependsOnPointer, bufferArraySize, identifier, -1);
    }

    /*
    ** For pre-allocated BufferManagers, the Operation is not set for the BufferManagerPointer used to
    **   add the ByteBuffers to the BufferManager. This is where it is associated with the pointer.
     */
    public void setOperation(final Operation operation) {
        this.operation = operation;

        LOG.info("setOperation() Producer("  + identifier + ":" + getOperationType() + ") bufferIndex: " + bufferIndex +
                " bookmark: " + bookmark);
    }

    /*
    ** A useful debug routine to show dependency trees in trace statements.
     */
    public OperationTypeEnum getOperationType() {
        if (operation != null) {
            return operation.getOperationType();
        } else {
            return OperationTypeEnum.NULL_OPERATION;
        }
    }

    /*
    ** Each BufferManagerPointer is given a unique ID by the owning BufferManager. This allows
    **   for an easier tracking of dependencies between the pointers and their owning BufferManager.
     */
    public int getIdentifier() {
        return identifier;
    }

    public void dumpPointerInfo() {
        if (ptrThisDependsOn == null) {
            LOG.info("     Producer("  + identifier + ":" + getOperationType() + ") bufferIndex: " + bufferIndex +
                    " bookmark: " + bookmark);
        } else {
            LOG.info("     Consumer("  + identifier + ":" + getOperationType() + ") depends on Producer(" +
                    ptrThisDependsOn.getIdentifier() + ":" + ptrThisDependsOn.getOperationType() +
                    ") bufferIndex: " + bufferIndex + " maxBytesToConsume: " + maxBytesToConsume);

        }
    }

    /*
    ** For producers, this returns where the next write will take place. This index does
    **   not have valid data in it.
    **
    ** For consumers, this returns where it will read from. If the readIndex is the same as the
    **   the producers writeIndex, there is no valid data available.
     */
    public int getCurrIndex() {
        return bufferIndex.get();
    }

    /*
    ** The getBookmark() will only be called for Producers. It does not make sense for a Consumer
    **   to have a bookmark as there must never be a Consumer that depends on a Consumer.
     */
    int getBookmark() {
        /*
        ** If no bookmark has been set, it will be -1. This is a normal condition and just means to use
        **   the current index. Generally, this happens when the Producer is being setup.
         */
        if (bookmark == -1) {
            LOG.info("Producer("  + identifier + ":" + getOperationType() + ") getBookmark: -1, will use: " +
                    bufferIndex.get());

        } else {
            LOG.info("Producer("  + identifier + ":" + getOperationType() + ") getBookmark: " + bookmark +
                    " bufferIndex: " + bufferIndex.get());
        }

        return bookmark;
    }

    /*
    ** The setBookmark() with no parameters is only used for Consumer to indicate where they left off so that the
    **   next (could be more than one) Consumer who registers with the same Producer will pick up where this
    **   Consumer left off. This allows Consumers to register with a Producer some time after the Producer
    **   has moved on a placed more data into buffers. It allows the Producers to run at a different rate,
    **   without requiring barriers, than Consumers.
    **
    ** A good example of the use of a bookmark is a Producer that is generating encrypted data for a chunk write.
    **   The Consumer needs to register with the first buffer that holds data for that chunk, but there is no
    **   reason to stop the consumer from encrypting more buffers while it is waiting for the registration to
    **   take place. In that case, the Producer determines the first byte of the buffer that will begin a
    **   chunk and sets a bookmark there. Later, when the Consumers register, they use that bookmark to start
    **   reading (or using) that encrypted data at the beginning of the chunk.
     */
    void setBookmark() {
        int currIndex = bufferIndex.get();
        if (ptrThisDependsOn != null) {
            /*
            LOG.error("Consumer(" + identifier + ":" + getOperationType() + ") setBookmark: " + currIndex + " on Producer(" +
                    ptrThisDependsOn.getIdentifier() + ":" + ptrThisDependsOn.getOperationType() + ")");
            */
            ptrThisDependsOn.setBookmark(currIndex);
        } else {
            LOG.info("Producer(" + identifier + ":" + getOperationType() + ") setBookmark(1): " + currIndex);

            bookmark = currIndex;
        }
    }

    /*
    ** The following is the call to set a bookmark in a Producer.
     */
    void setBookmark(final int consumerBookmarkValue) {
        LOG.info("Producer("  + identifier + ":" + getOperationType() + ") setBookmark(2): " + consumerBookmarkValue);
        bookmark = consumerBookmarkValue;
    }

    /*
    ** This checks if the consumer has buffers available to return. The consumer must have a producer
    **   that it depends on. If the consumer's read index matches the producer's write index, then
    **   there are no buffers available.
     */
    int getReadIndex(final boolean updatePointer) {
        int readIndex = bufferIndex.get();
        if (ptrThisDependsOn != null) {
            int dependsOnIndex = ptrThisDependsOn.getCurrIndex();

            /*
            ** Handle the normal case where the Consumer is trying to catch up to the Producer
             */
            if (readIndex != dependsOnIndex) {

                if (updatePointer) {
                    bufferIndex.accumulateAndGet(bufferArraySize, computeNextIndex);
                }

                return readIndex;
            } else {
                /*
                ** This is the normal case when the consumer has caught up to the producer so there
                **   are no more buffers to consume at this moment.
                 */
                LOG.info("getReadIndex() waiting for buffers Consumer(" + identifier + ":" + getOperationType() + ") bufferIndex: " +
                        bufferIndex + " Producer(" + ptrThisDependsOn.getIdentifier() + ":" +
                        ptrThisDependsOn.getOperationType() + ") writeIndex: " + ptrThisDependsOn.getCurrIndex());
            }
        } else {
            /*
            ** This is the case for the Producer accessing the buffer. It must check that it is not catching up to
            **   to the Consumers who are dependent upon it.
             */
            int returnIndex = readIndex;
            readIndex++;
            if (readIndex == bufferArraySize) {
                readIndex = 0;
            }

            /*
            ** Now check that this will not run into any dependent pointers
             */
            for (BufferManagerPointer consumer : dependentPointers) {
                if (consumer.getCurrIndex() == readIndex) {
                    /*
                     ** This is the wrap condition, cannot proceed
                     */
                    LOG.warn("Producer(" + identifier + ":" + getOperationType() + ") wrapCondition nextIndex: " + readIndex +
                            " Consumer(" + consumer.getIdentifier() + ":" + consumer.getOperationType() + ") readIndex: " +
                            consumer.getCurrIndex());
                    return -1;
                }
            }

            /*
            ** Advance the pointer for the next read since it has not wrapped
             */
            if (updatePointer) {
                bufferIndex.getAndAccumulate(bufferArraySize, computeNextIndex);
            }

            //LOG.info("Consumer("  + identifier + ":" + getOperationType() + ") readIndex: " + returnIndex);
            return returnIndex;
        }

        return -1;
    }

    /*
    ** This returns the current write index, but does not change it. This is used to add a ByteBuffer
    **   to the BufferManager and after the buffer is added, then updateWriteIndex() should be called
    **   if the buffer is actually to be made available to the writer.
     */
    int getWriteIndex() {
        return bufferIndex.get();
    }

    /*
    ** This is used to reset a BufferManagerPointer back to its initial state.
     */
    int reset() {
        int tempIndex = bufferIndex.getAndSet(0);

        LOG.info("reset() Producer("  + identifier + ":" + getOperationType() + ") bufferIndex: " + bufferIndex);
        bookmark = -1;

        return tempIndex;
    }

    /*
    ** This updates the producers writeIndex after data has been placed into the BufferState
    **   and it returns the location to place the next data write.
    **
    ** This will also call all the Operations eventHandlers that are registered as
    **   depending upon this producer.
     */
    int updateWriteIndex() {
        /*
        ** The index must be incremented prior to the dependent Operations are evented otherwise there is the
        **   potential of an Operation that is running on a compute thread missing the index update and then
        **   simply exiting without doing any work.
         */
        int newIndex = bufferIndex.accumulateAndGet(bufferArraySize, computeNextIndex);

        generateDependsOnEvents();

        //LOG.info("Producer("  + identifier + ":" + getOperationType() + ") writeIndex: " + newIndex);

        return newIndex;
    }

    /*
    ** This updates where the next read will take place for a consumer
     */
    int updateReadIndex() {

        return bufferIndex.accumulateAndGet(bufferArraySize, computeNextIndex);
    }

    /*
     ** This adds a consumer BufferManagerPointer to the dependency list if it is not already on it.
     ** The dependency list is used to insure that the producer does not wrap over a consumer and
     **   overwrite its data. This is to prevent the case where to producer is much quicker
     **   than the consumer(s).
     */
    void addDependsOn(final BufferManagerPointer consumerPtr) {
        if (!dependentPointers.contains(consumerPtr)) {
            dependentPointers.add(consumerPtr);
        }
    }


    /*
    ** This adds an Operation to the dependency list if it is not already on it. The dependency list
    **   is used to event() all of the Operations when a change is made by the Producer.
     */
    void addDependsOn(final Operation operation) {
        if (!ptrWhoDependOnThisList.contains(operation)) {
            ptrWhoDependOnThisList.add(operation);
        }
    }

    /*
    ** The following walks the depends on list and call event() for all of the registered operations
    **   who are consumers of the data generated by the Producer who owns this BufferManagerPointer.
     */
    void generateDependsOnEvents() {

        for (Operation operation : ptrWhoDependOnThisList) {
            operation.event();
        }
    }

    void removeDependency(final Operation dependsOnOperation) {
        LOG.info("removeDependency() Producer(" + identifier + ":" + getOperationType() + ") operation: " +
                dependsOnOperation.getOperationType());

        ptrWhoDependOnThisList.remove(dependsOnOperation);
    }

    void terminate() {
        if (ptrThisDependsOn != null) {
            LOG.info("terminate() Consumer(" + identifier + ":" + getOperationType() + ")" +
                    " Producer(" + ptrThisDependsOn.getIdentifier() + ":" + ptrThisDependsOn.getOperationType() + ")" );

            ptrThisDependsOn.removeDependency(operation);

            /*
            ** Remove from the dependentPointers list that is used to check for wrap conditions
             */
            ptrThisDependsOn.dependentPointers.remove(this);

            ptrThisDependsOn = null;
        }

        /*
        ** This must not have any dependencies when this is being terminated
         */
        Iterator<Operation> iter = ptrWhoDependOnThisList.iterator();
        while (iter.hasNext()) {
            LOG.info("  Producer(" + identifier + ":" + getOperationType() + ") operation: " +  iter.next().getOperationType());
            iter.remove();
        }
        ptrWhoDependOnThisList = null;
    }
}
