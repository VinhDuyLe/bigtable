package com.bigtable.cache;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Segmented/striped LRU cache for SStable blocks.
 * <p>
 * Rationale: reduce global lock contention by sharding the keyspace across multiple
 * segments, each with its own LRU map and byte budget. Hash-based stripping yields near O(1)
 * operations under concurrency while preserving LRU within a segment.
 * <ul>
 *     <li>Off-heap storage via direct {@link ByteBuffer} to lower GC pressure.</li>
 *     <li>Per-segment eviction to honor global {@code maxBytes} budget.</li>
 *     <li>Read returns a read-only duplicate, rewound to position 0.</li>
 * </ul>
 * Sharded by a spread hash into N segments (power-of-two; defaults to ~2× cores, min 8, max 64;
 * override via -bigtable.cache.segments=).
 * <p>
 * Each segment has its own access-ordered LinkedHashMap + ReentrantLock.
 * Off-heap storage via direct ByteBuffers; reads return read-only duplicates.
 * Eviction happens within the owning segment to maintain a global maxBytes budget (split evenly per segment).
 *
 * System properties:
 *   -bigtable.cache.segments=N        # Number of segments (default: 2 * cores, min 8, max 64)
 */

public class BlockCache {
    private final int segments;
    private final long maxBytes;
    private final long perSegBudget;
    private Segment[] segs;
    private final AtomicLong totalBytes = new AtomicLong(0);

    private static final class Segment {
        final ReentrantLock lock = new ReentrantLock();
        final LinkedHashMap<String, ByteBuffer> map = new LinkedHashMap<>(16, 0.75f, true);
        long bytes;
    }

    /**
     * Create a segmented cache. Segment count defaults to nearest power-of-two
     * not less than 2 * CPU cores (min 8, max 64), unless overridden via system
     * property {@code bigtable.cache.segments}.
     */
    public BlockCache(long maxBytes) {
        this.maxBytes = Math.max(1, maxBytes);
        int cfg = Integer.getInteger("bigtable.cache.segments", 0);
        int suggested = cfg > 0 ? cfg : Math.min(64, Math.max(8, Integer.highestOneBit(Runtime.getRuntime().availableProcessors() * 2 - 1) << 1));
        this.segments = Math.max(1, suggested);
        this.perSegBudget = Math.max(1, this.maxBytes / this.segments);
        this.segs = new Segment[this.segments];
        for (int i = 0; i < this.segments; i++) {
            segs[i] = new Segment();
        }
    }

    /**
     * Get a read-only duplicate of the cached block by key, or null.
     */
    public ByteBuffer get(String key) {
        Segment s = seg(key);
        s.lock.lock();
        try {
            ByteBuffer b = s.map.get(key);
            if (b == null) return null;
            ByteBuffer dup = b.asReadOnlyBuffer();
            dup.rewind();
            return dup;
        } finally {
            s.lock.unlock();
        }
    }

    /**
     * Put a block into cache; evict LRU in the owning segment until size constraint satisfied.
     */
    public void put(String key, ByteBuffer src) {
        if (src == null) return;
        int len = src.remaining();
        if (len <= 0 || len > perSegBudget) return; // too large for any segment
        ByteBuffer direct = ByteBuffer.allocateDirect(len);
        direct.put(src.duplicate());
        direct.flip();
        Segment s = seg(key);
        s.lock.lock();
        try {
            ByteBuffer prev = s.map.put(key, direct);
            if (prev != null) {
                s.bytes -= prev.capacity();
                totalBytes.addAndGet(-prev.capacity());
            }
            s.bytes += direct.capacity();
            totalBytes.addAndGet(direct.capacity());

            // Evict LRU until within pre-segment budget
            while (s.bytes > perSegBudget && !s.map.isEmpty()) {
                Map.Entry<String, ByteBuffer> eldest = s.map.entrySet().iterator().next();
                s.map.remove(eldest.getKey());
                int cap = eldest.getValue().capacity();
                s.bytes -= cap;
                totalBytes.addAndGet(-cap);
            }
        } finally {
            s.lock.unlock();
        }
    }

    /**
     * Remove a specific entry.
     */
    public void remove(String key) {
        Segment s = seg(key);
        s.lock.lock();
        try {
            ByteBuffer b = s.map.remove(key);
            if (b != null) {
                s.bytes -= b.capacity();
                totalBytes.addAndGet(-b.capacity());
            }
        } finally {
            s.lock.unlock();
        }
    }

    /**
     * Drop all entries
     */
    public void clear() {
        for (Segment s : segs) {
            s.lock.lock();
            try {
                for (ByteBuffer b : s.map.values()) {
                    // let GC/free of direct buffers happen when dereferenced
                }
                s.map.clear();
                totalBytes.addAndGet(-s.bytes);
                s.bytes = 0L;
            } finally {
                s.lock.unlock();
            }
        }
    }

    /**
     * Approximate total bytes across segments.
     */
    public long currentBytes() {
        return Math.max(0L, totalBytes.get());
    }

    private Segment seg(String key) {
        int h = spread(key.hashCode());
        return segs[(h & (segments - 1))];
    }

    private static int spread(int h) {
        // Similar to ConcurrentHashMap's spreader
        h ^= (h >>> 16);
        return h;
    }

}
