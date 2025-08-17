package com.bigtable.codec;

import com.github.luben.zstd.Zstd;

/**
 * Thin Zstd compression/decompression adapter.
 * <p>
 * Purpose: per-block compression on write, decompression on read.
 */
public final class ZstdCompression {
    private ZstdCompression() {
    }

    /**
     * Compress an entire byte array using a given level.
     *
     * @param src input data
     * @param lvl compression level (typ. 1-9)
     * @return compressed bytes
     */
    public static byte[] compress(byte[] src, int lvl) {
        return Zstd.compress(src, lvl);
    }

    /**
     * Decompress a block into an array of exactly {@code originalSize} bytes.
     *
     * @param compressed   zstd-compressed payload
     * @param originalSize uncompressed size expected
     * @return decompressed bytes
     */
    public static byte[] decompress(byte[] compressed, int orginalSize) {
        byte[] dest = new byte[orginalSize];
        long r = Zstd.decompress(dest, compressed);
        if (r <= 0) throw new RuntimeException("zstd decompress error: " + r);
        return dest;
    }
}
