package com.sstable.storage.api.sharding;

import com.sstable.storage.api.ShardingFunction;
import com.google.common.hash.Hashing;
import io.netty.buffer.ByteBuf;

/**
 * Default sharding function using modulo of MurmurHash3.
 *
 * <p>Formula: shard = (murmur3_32(key) & 0x7FFFFFFF) % numShards
 *
 * <p>This provides uniform distribution and is compatible with most
 * existing data pipelines.
 *
 * @author V
 * @since 1.0.0
 */
public final class ModSharder implements ShardingFunction {

    public static final ModSharder INSTANCE = new ModSharder();
    private static final String NAME = "mod";

    private ModSharder() {
    }

    @Override
    public int shardOf(ByteBuf key, int numShards) {
        if (numShards <= 0) {
            throw new IllegalArgumentException("numShards must be positive");
        }

        // Compute MurmurHash3_32
        int hash;
        if (key.hasArray()) {
            hash = Hashing.murmur3_32_fixed().hashBytes(key.array(), key.arrayOffset() + key.readerIndex(), key.readableBytes())
                    .asInt();
        } else {
            byte[] temp = new byte[key.readableBytes()];
            key.getBytes(key.readerIndex(), temp);
            hash = Hashing.murmur3_32_fixed().hashBytes(temp).asInt();
        }

        // Make positive and mod
        return (hash & 0x7FFFFFFF) % numShards;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public byte[] config() {
        return new byte[0]; // No configuration needed
    }
}
