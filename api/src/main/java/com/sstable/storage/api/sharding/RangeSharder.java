package com.sstable.storage.api.sharding;

import com.sstable.storage.api.ShardingFunction;
import com.sstable.storage.common.ByteBufComparator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Range-based sharding using sorted boundary keys.
 *
 * <p>Provides better locality for range scans but requires knowing
 * the key distribution in advance.
 *
 * <p>Example with 3 shards and boundaries ["m", "s"]:
 * <pre>
 *   Shard 0: keys < "m"
 *   Shard 1: "m" <= keys < "s"
 *   Shard 2: keys >= "s"
 * </pre>
 *
 * @author V
 * @since 1.0.0
 */
public final class RangeSharder implements ShardingFunction {
    private static final String NAME = "range:v1";

    private final ByteBuf[] boundaries; // sorted, length = numShards - 1

    /**
     * Creates a range sharder with the given boundaries.
     *
     * @param boundaries sorted boundary keys (length = numShards - 1)
     */
    public RangeSharder(ByteBuf[] boundaries) {
        if (boundaries == null || boundaries.length == 0) {
            throw new IllegalArgumentException("Boundaries required");
        }

        // Verify sorted order
        for (int i = 1; i < boundaries.length; i++) {
            if (ByteBufComparator.INSTANCE.compare(boundaries[i - 1], boundaries[i]) >= 0) {
                throw new IllegalArgumentException("Boundaries must be in ascending order");
            }
        }

        this.boundaries = Arrays.copyOf(boundaries, boundaries.length);
    }

    @Override
    public int shardOf(ByteBuf key, int numShards) {
        if (numShards != boundaries.length + 1) {
            throw new IllegalArgumentException("numShards must equal boundaries.length + 1");
        }

        // Binary search to find shard
        int left = 0;
        int right = boundaries.length;

        while (left < right) {
            int mid = left + (right - left) / 2;
            int cmp = ByteBufComparator.INSTANCE.compare(key, boundaries[mid]);

            if (cmp < 0) {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        return left;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public byte[] config() {
        // Serialize boundaries: [count:4][len:4][bytes]...[len:4][bytes]
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            // Write count
            writeInt(baos, boundaries.length);

            // Write each boundary
            for (ByteBuf boundary : boundaries) {
                writeInt(baos, boundary.readableBytes());
                byte[] bytes = new byte[boundary.readableBytes()];
                boundary.getBytes(boundary.readerIndex(), bytes);
                baos.write(bytes);
            }

            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize boundaries", e);
        }
    }

    /**
     * Deserializes boundaries from config bytes.
     */
    public static RangeSharder fromConfig(byte[] config) {
        if (config == null || config.length < 4) {
            throw new IllegalArgumentException("Invalid config");
        }

        int offset = 0;
        int count = readInt(config, offset);
        offset += 4;

        ByteBuf[] boundaries = new ByteBuf[count];

        for (int i = 0; i < count; i++) {
            int len = readInt(config, offset);
            offset += 4;

            byte[] bytes = Arrays.copyOfRange(config, offset, offset + len);
            boundaries[i] = Unpooled.wrappedBuffer(bytes);
            offset += len;
        }

        return new RangeSharder(boundaries);
    }

    private static void writeInt(ByteArrayOutputStream out, int value) throws IOException {
        out.write((value >>> 24) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    private static int readInt(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 24) | ((data[offset + 1] & 0xFF) << 16) | ((data[offset + 2] & 0xFF) << 8) | (data[offset + 3] & 0xFF);
    }
}
