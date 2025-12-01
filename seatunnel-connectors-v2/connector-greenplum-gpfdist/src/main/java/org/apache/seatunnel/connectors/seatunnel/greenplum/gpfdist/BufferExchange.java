/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.greenplum.gpfdist;

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class BufferExchange {

    private final int watermark;
    private final ReentrantLock putLock = new ReentrantLock();
    private final ReentrantLock getLock = new ReentrantLock();
    private final Condition notFull = putLock.newCondition();
    private final Condition notEmpty = getLock.newCondition();

    private final BlockingQueue<ByteBuffer> emptyBuffers = new LinkedBlockingQueue<>();
    private final BlockingQueue<ByteBuffer> filledBuffers = new LinkedBlockingQueue<>();

    private ByteBuffer currentBuffer;
    private final AtomicLong totalFill = new AtomicLong(0);
    private final AtomicLong totalDrain = new AtomicLong(0);
    private final AtomicLong enqueueCount = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private static final int MAX_BUFFER_COUNT = 10;
    private static final int DEFAULT_BUFFER_SIZE_MULTIPLIER = 2;

    public BufferExchange(int watermark) {
        this.watermark = watermark;
        for (int i = 0; i < 5; i++) {
            emptyBuffers.offer(ByteBuffer.allocate(watermark * DEFAULT_BUFFER_SIZE_MULTIPLIER));
        }
        log.debug("BufferExchange initialized with watermark: {}", watermark);
    }

    public void put(byte[] data) {
        if (data == null || data.length == 0 || closed.get()) {
            return;
        }

        putLock.lock();
        try {
            while (filledBuffers.size() >= MAX_BUFFER_COUNT && !closed.get()) {
                try {
                    notFull.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            if (closed.get()) {
                return;
            }

            if (currentBuffer == null || currentBuffer.remaining() < data.length) {
                flushCurrentBuffer();
                allocateNewBuffer(data.length);
            }

            if (currentBuffer != null && currentBuffer.remaining() >= data.length) {
                currentBuffer.put(data);
                totalFill.addAndGet(data.length);

                if (currentBuffer.position() >= watermark) {
                    flushCurrentBuffer();
                }
            } else {
                ByteBuffer largeBuffer = ByteBuffer.wrap(data.clone());
                largeBuffer.position(data.length);
                filledBuffers.offer(largeBuffer);
                totalFill.addAndGet(data.length);
                enqueueCount.addAndGet(data.length);

                getLock.lock();
                try {
                    notEmpty.signalAll();
                } finally {
                    getLock.unlock();
                }
            }
        } finally {
            putLock.unlock();
        }
    }

    public ByteBuffer get(long timeoutMs) {
        getLock.lock();
        try {
            long deadline = timeoutMs > 0 ? System.currentTimeMillis() + timeoutMs : Long.MAX_VALUE;

            while (!closed.get() || !filledBuffers.isEmpty()) {
                ByteBuffer buffer = filledBuffers.poll();
                if (buffer != null) {
                    buffer.flip();
                    long drainedBytes = buffer.remaining();
                    totalDrain.addAndGet(drainedBytes);

                    putLock.lock();
                    try {
                        notFull.signalAll();
                    } finally {
                        putLock.unlock();
                    }

                    log.trace("Retrieved buffer with {} bytes", drainedBytes);
                    return buffer;
                }

                if (closed.get()) {
                    break;
                }

                if (timeoutMs <= 0) {
                    break;
                }

                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    break;
                }

                try {
                    notEmpty.awaitNanos(remaining * 1_000_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } finally {
            getLock.unlock();
        }

        return null;
    }

    public void flush() {
        putLock.lock();
        try {
            flushCurrentBuffer();
        } finally {
            putLock.unlock();
        }
    }

    public void recycleBuffer(ByteBuffer buffer) {
        if (buffer != null
                && buffer.capacity() >= watermark
                && emptyBuffers.size() < MAX_BUFFER_COUNT) {
            buffer.clear();
            emptyBuffers.offer(buffer);
        }
    }

    public void close() {
        putLock.lock();
        try {
            if (closed.compareAndSet(false, true)) {
                flushCurrentBuffer();
                notFull.signalAll();
                log.debug(
                        "BufferExchange closed. Stats: totalFill={}, totalDrain={}, enqueue={}",
                        totalFill.get(),
                        totalDrain.get(),
                        enqueueCount.get());
            }
        } finally {
            putLock.unlock();
        }

        getLock.lock();
        try {
            notEmpty.signalAll();
        } finally {
            getLock.unlock();
        }
    }

    private void flushCurrentBuffer() {
        if (currentBuffer != null && currentBuffer.position() > 0) {
            filledBuffers.offer(currentBuffer);
            enqueueCount.addAndGet(currentBuffer.position());
            currentBuffer = null;

            getLock.lock();
            try {
                notEmpty.signalAll();
            } finally {
                getLock.unlock();
            }

            log.trace("Flushed buffer to filled queue");
        }
    }

    private void allocateNewBuffer(int minSize) {
        int allocSize = Math.max(watermark * DEFAULT_BUFFER_SIZE_MULTIPLIER, minSize);

        ByteBuffer reusedBuffer = emptyBuffers.poll();
        if (reusedBuffer != null && reusedBuffer.capacity() >= allocSize) {
            reusedBuffer.clear();
            currentBuffer = reusedBuffer;
            log.trace("Reused buffer with capacity: {}", reusedBuffer.capacity());
        } else {
            currentBuffer = ByteBuffer.allocate(allocSize);
            log.trace("Allocated new buffer with capacity: {}", allocSize);
        }
    }

    public long getTotalFill() {
        return totalFill.get();
    }

    public long getTotalDrain() {
        return totalDrain.get();
    }

    public long getEnqueueCount() {
        return enqueueCount.get();
    }

    public boolean isTransferComplete() {
        putLock.lock();
        try {
            return closed.get()
                    && filledBuffers.isEmpty()
                    && (currentBuffer == null || currentBuffer.position() == 0);
        } finally {
            putLock.unlock();
        }
    }

    public int getAvailableBuffers() {
        return filledBuffers.size();
    }

    public boolean isClosed() {
        return closed.get();
    }
}
