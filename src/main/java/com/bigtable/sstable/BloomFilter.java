package com.bigtable.sstable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Compact mmap-able Bloom filter.
 * <p>
 * Format: [magic(8)][k:1][m:4][bit-array...].
 * Writer builds an in-memory bit-array, then persists in a compact binary form.
 * Reader maps file into memory for zero-copy presence checks.
 */
public final class BloomFilter {
    private static final long MAGIC = 0xBF1DBEAD0B11L;
    private final int k;        // number of hash functions
    private final int m;        // number of bits
    private final byte[] bits;

    public BloomFilter(int m, int k) {
        this.k = k;
        this.m = m;
        this.bits = new byte[(m + 7) >>> 3]; // allocating a byte array big enough to store m bits. (m + 7) >>> 3 is an efficient way to compute ceiling(m / 8). Adding 7 before shifting ensures that if m isn’t a multiple of 8, you still allocate enough bytes to cover the remainder.
    }

    /**
     * Add a key to the filter.
     */
    public void add(byte[] key) {
        int h1 = murmur3(key, 0);
        int h2 = murmur3(key, 4);
        for (int i = 0; i < k; i++) {
            int idx = Math.floorMod(h1 + i * h2, m);
            int byteIdx = idx >>> 3;    // The >>> 3 is an unsigned right shift by 3, which is equivalent to dividing by 8
            int bit = idx & 7;
            bits[byteIdx] |= (1 << bit);
        }
    }

    /**
     * Return false if definitely not present; true if maybe present.
     */
    public boolean mightContain(byte[] key) {
        int h1 = murmur3(key, 0);
        int h2 = murmur3(key, 4);
        for (int i = 0; i < k; i++) {
            int idx = Math.floorMod(h1 + i * h2, m);
            int byteIdx = idx >>> 3;
            int bit = idx & 7;
            if ((bits[byteIdx] & (1 << bit)) == 0) return false;
        }
        return true;
    }

    /**
     * Persist the Bloom filter to a compact on-disk representation and fsync.
     */
    public void writeTo(File f) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(f)) {
            fos.write(longToBytes(MAGIC));
            fos.write((byte) k);
            fos.write(intToBytes(m));
            fos.write(bits);
            fos.getFD().sync();
        }
    }

    /**
     * Memory-map a Bloom filter from a file previously written by {@link #writeTo(File)}.
     */
    public static BloomFilter mapFrom(File f) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            FileChannel ch = raf.getChannel();
            MappedByteBuffer mb = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size());
            long magic = mb.getLong();
            if (magic != MAGIC) throw new IOException("bad bloom magic");
            int k = mb.get() & 0xFF;
            int m = mb.getInt();
            int bytes = (m + 7) >>> 3;
            byte[] bits = new byte[bytes];
            mb.get(bits);
            BloomFilter bf = new BloomFilter(m, k);
            System.arraycopy(bits, 0, bf.bits, 0, Math.min(bits.length, bf.bits.length));
            return bf;
        }
    }

    // --- helpers---
    private static int murmur3(byte[] data, int seed) {
        int h = seed;
        for (byte b : data) h = h * 31 + (b & 0xFF);
        return h;
    }

    private static byte[] longToBytes(long v) {
        return new byte[]{(byte) (v >>> 56), (byte) (v >>> 48), (byte) (v >>> 40), (byte) (v >>> 32), (byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
    }

    private static byte[] intToBytes(int v) {
        return new byte[]{(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
    }

}
