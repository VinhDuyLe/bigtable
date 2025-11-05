package com.sstable.storage.api;

import io.netty.buffer.ByteBuf;

/**
 * Strategy for deterministically mapping keys to shards.
 *
 * <p>A sharding function must be:
 * <ul>
 *   <li>Deterministic: same key always maps to same shard</li>
 *   <li>Stable: implementation never changes for same name+version</li>
 *   <li>Uniform: roughly equal distribution across shards</li>
 * </ul>
 *
 * @author V
 * @since 1.0.0
 */

public interface ShardingFunction {

    /**
     * Returns the shard index for the given key.
     *
     * @param key the key to shard
     * @param numShards total number of shards (must be > 0)
     * @return shard index in [0, numShards)
     */
    int shardOf(ByteBuf key, int numShards);

    /**
     * Returns a stable name for this sharding function.
     *
     * <p>The name is recorded in the SSTable manifest and must match
     * exactly when reopening the SSTable.
     *
     * @return stable name (e.g., "mod", "range:v1", "fingerprint:v2")
     */
    String name();

    /**
     * Returns serializable configuration for this sharder.
     *
     * <p>For range sharding, this might be the boundary points.
     * For simple sharders, this can be empty.
     *
     * @return configuration bytes (can be empty)
     */
    byte[] config();
}
