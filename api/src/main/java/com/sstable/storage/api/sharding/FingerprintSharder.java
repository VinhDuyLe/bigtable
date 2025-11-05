package com.sstable.storage.api.sharding;

import com.sstable.storage.api.ShardingFunction;
import com.google.common.hash.Hashing;
import io.netty.buffer.ByteBuf;

/**
 * High-entropy fingerprint-based sharding.
 *
 * <p>Uses 64-bit fingerprint for maximum uniformity across shards.
 * Best for workloads requiring strict load balancing.
 *
 * @author V
 * @since 1.0.0
 */
public final class FingerprintSharder implements ShardingFunction {

    public static final FingerprintSharder INSTANCE = new FingerprintSharder();
    private static final String NAME = "fingerprint:v2";

    private FingerprintSharder() {
    }

    @Override
    public int shardOf(ByteBuf key, int numShards) {
        if (numShards <= 0) {
            throw new IllegalArgumentException("numShards must be positive");
        }

        // Compute 64-bit fingerprint
        long fingerprint;
        if (key.hasArray()) {
            fingerprint = Hashing.murmur3_128()
                    .hashBytes(key.array(),
                            key.arrayOffset() + key.readerIndex(),
                            key.readableBytes())
                    .asLong();

        } else {
            byte[] temp = new byte[key.readableBytes()];
            key.getBytes(key.readerIndex(), temp);
            fingerprint = Hashing.murmur3_128().hashBytes(temp).asLong();
        }
        // Make positive and mod
        return (int) ((fingerprint & 0x7FFFFFFFFFFFFFFFL) % numShards);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public byte[] config() {
        return new byte[0];
    }

}
