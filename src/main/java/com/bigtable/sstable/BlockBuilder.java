package com.bigtable.sstable;

import com.bigtable.codec.Varint;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds a single data block payload (no outer header/CRC).
 * <p>
 * Entries use prefix compression with periodic restart points to bound seek time.
 * Appends at the end: varint(restartOffsets...) then varint(restartCount).
 */
public final class BlockBuilder {
    private final int restartInterval;
    private final List<Integer> restarts = new ArrayList<>();
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private byte[] lastKey = new byte[0];
    private int counter = 0;
    private int estimatedSize = 0;

    public BlockBuilder(int restartInterval) {
        this.restartInterval = restartInterval;
        restarts.add(0);
    }

    /**
     * Append an entry with prefix compression.
     * Header per entry: varint(shared), varint(nonShared), varint(valueLen), then key suffix and value bytes.
     */
    public void add(byte[] key, byte[] value) throws IOException {
        int shared = 0;
        if (counter % restartInterval != 0) {
            shared = sharedPrefixLength(lastKey, key);
        } else {
            restarts.add(out.size());
            estimatedSize += 4; // rough accounting for restart offsets
        }
        int nonShared = key.length - shared;
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        Varint.writeUnsignedVarInt(header, shared);
        Varint.writeUnsignedVarInt(header, nonShared);
        Varint.writeUnsignedVarInt(header, value.length);
        byte[] h = header.toByteArray();
        out.write(h);
        out.write(key, shared, nonShared);
        out.write(value);
        estimatedSize += h.length + nonShared + value.length;
        lastKey = key;
        counter++;
    }

    private static int sharedPrefixLength(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        int i = 0;
        while (i < n && a[i] == b[i]) i++;
        return i;
    }

    /**
     * Finish block by writing restart array and count; return final payload bytes.
     */
    public byte[] finish() throws IOException {
        for (int off : restarts) {
            out.write(intToBytes(off));
        }
        out.write(intToBytes(restarts.size()));
        return out.toByteArray();
    }

    /**
     * Estimated encoded size so far (approximate; used to trigger flush at blockSize boundary).
     */
    public int estimatedSize() {
        return estimatedSize + 4 * restarts.size() + 4;
    }

    private static byte[] intToBytes(int v) {
        return new byte[]{
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v
        };
    }

}
