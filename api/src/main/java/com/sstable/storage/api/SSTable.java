package com.sstable.storage.api;

import io.netty.buffer.ByteBuf;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

/**
 * Primary interface for immutable Sorted String Table (SSTable) storage.
 *
 * <p>An SSTable is an immutable, sorted multimap from keys to values persisted on disk.
 * A key can have multiple values (versions/timestamps).
 *
 * <p><b>Sharding:</b> Large SSTables are split across multiple shard files using
 * a deterministic sharding function. Each shard file is named:
 * <pre>
 *   basename-00000-of-00010.sst  // shard 0 of 10
 *   basename-00001-of-00010.sst  // shard 1 of 10
 *   ...
 * </pre>
 *
 * <p><b>Multi-value Support:</b> Each key can have multiple values, typically
 * representing different versions/timestamps. Values for the same key are stored
 * contiguously and returned in sorted order (newest to oldest by timestamp).
 *
 * @author V
 * @since 1.0.0
 */


public interface SSTable extends Closeable {

    /**
     * Retrieves all values associated with the given key.
     *
     * <p>If the key has multiple values (versions), all are returned in
     * sorted order (newest timestamp first).
     *
     * @param key the key to look up
     * @return list of values (empty if key not found)
     * @throws IOException if I/O error occurs
     */
    List<ByteBuf> get(ByteBuf key) throws IOException;

    /**
     * Returns an iterator over all key-value pairs in sorted order.
     *
     * <p>For keys with multiple values, the iterator will return multiple
     * entries with the same key but different values.
     *
     * @return iterator over entries
     * @throws IOException if I/O error occurs
     */
    Iterator<Entry> scan() throws IOException;

    /**
     * Returns an iterator over entries in the range [startKey, endKey).
     */
    Iterator<Entry> scan(ByteBuf startKey, ByteBuf endKey) throws IOException;

    /**
     * Returns an iterator over entries with keys starting with the given prefix.
     */
    Iterator<Entry> scanPrefix(ByteBuf prefix) throws IOException;

    /**
     * Returns metadata about this SSTable.
     */
    SSTableMetadata getMetadata();

    /**
     * Returns the total number of key-value entries (across all values).
     */
    long entries();

    /**
     * Returns the number of unique keys.
     */
    long uniqueKeys();

    /**
     * Returns the number of shards this SSTable is split across.
     */
    int numShards();

    /**
     * Returns the name of the sharding function used.
     */
    String sharderName();

    /**
     * Returns the sharding function instance.
     */
    ShardingFunction sharder();

    /**
     * Checks if the given key might exist in this SSTable.
     */
    boolean mightContain(ByteBuf key);

    /**
     * Returns the file path of this SSTable (for single-shard SSTables)
     * or the base path (for sharded SSTables).
     */
    Path getPath();

    /**
     * Represents a key-value entry with optional timestamp.
     */
    interface Entry {
        ByteBuf getKey();

        ByteBuf getValue();

        long getTimestamp();      // microseconds since epoch

        EntryType getType();
    }

    enum EntryType {
        PUT,
        DELETE,
        MERGE
    }
}
