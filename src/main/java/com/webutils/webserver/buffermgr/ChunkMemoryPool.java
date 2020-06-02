package com.webutils.webserver.buffermgr;

import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.operations.ChunkMetering;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;


/*
** This allocates a BufferManger that holds a chunks worth of data
 */
public class ChunkMemoryPool {

    private static final Logger LOG = LoggerFactory.getLogger(ChunkMemoryPool.class);

    private final static OperationTypeEnum operationType = OperationTypeEnum.CHUNK_BUFFER_MGR_ALLOC;

    private final static int NUMBER_CHUNK_BUFFER_MANAGERS = 4;

    private final static int CHUNK_BUFFER_ALLOCATE_BASE_ID = 9000;

    private final MemoryManager memoryManager;

    private final int chunkBufferCount;

    private final Queue<ChunkAllocBufferInfo> availableChunks;

    public ChunkMemoryPool(final MemoryManager memoryManager, final int chunkBufferCount) {
        this.memoryManager = memoryManager;
        this.chunkBufferCount = chunkBufferCount;

        this.availableChunks = new ConcurrentLinkedDeque<>();

        initialize();
    }

    /*
    ** Allocate a BufferManager and a ChunkMetering operation to manage it
     */
    public ChunkAllocBufferInfo allocateChunk(final RequestContext requestContext) {
        ChunkAllocBufferInfo allocInfo = availableChunks.poll();

        if (allocInfo != null) {
            BufferManagerPointer addPtr = allocInfo.getAddBufferPointer();

            ChunkMetering chunkMetering = new ChunkMetering(requestContext, allocInfo.getBufferManager(), addPtr);
            addPtr.setOperation(chunkMetering);

            allocInfo.setMetering(chunkMetering);
        }

        return allocInfo;
    }

    /*
    ** Free the chunk BufferManager and wake up any operations that are waiting for a BufferManger
     */
    public void releaseChunk(final ChunkAllocBufferInfo allocInfo) {
        availableChunks.add(allocInfo);
    }

    /*
    ** Free the pool of chunk size BufferManagers
     */
    public void reset() {
        ChunkAllocBufferInfo allocInfo;
        int chunksFreed = 0;

        while ((allocInfo = availableChunks.poll()) != null) {
            BufferManager mgr = allocInfo.getBufferManager();
            BufferManagerPointer removePtr = allocInfo.getAddBufferPointer();

            mgr.reset(removePtr);

            for (int i = 0; i < chunkBufferCount; i++) {
                ByteBuffer buffer = mgr.getAndRemove(removePtr);
                if (buffer != null) {
                    memoryManager.poolMemFree(buffer, mgr);
                } else {
                    LOG.warn("ChunkMemoryPool null buffer i: " + i);
                }
            }

            chunksFreed++;
        }

        if (chunksFreed != NUMBER_CHUNK_BUFFER_MANAGERS) {
            LOG.error("ChunkMemoryPool missing chunks expected: " + NUMBER_CHUNK_BUFFER_MANAGERS + " freed: " + chunksFreed);
        }
    }

    /*
    ** Allocate the pool of chunk size BufferManagers
     */
    private void initialize() {
        for (int i = 0; i < NUMBER_CHUNK_BUFFER_MANAGERS; i++) {
            BufferManager mgr = new BufferManager(chunkBufferCount, "ChunkBufferAllocate_" + i, CHUNK_BUFFER_ALLOCATE_BASE_ID + i);

            BufferManagerPointer addPtr = mgr.register(null);
            for (int j = 0; j < chunkBufferCount; j++) {
                ByteBuffer buffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, mgr, operationType);

                mgr.offer(addPtr, buffer);
            }

            ChunkAllocBufferInfo allocInfo = new ChunkAllocBufferInfo(mgr, addPtr);
            availableChunks.add(allocInfo);
        }
    }


}
