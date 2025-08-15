package com.bigtable.codec;

import com.github.luben.zstd.Zstd;

public final class ZstdCompression {
    private ZstdCompression() {
    }

    public static byte[] compress(byte[] src, int lvl) {
        return Zstd.compress(src, lvl);
    }

    public static byte[] decompress(byte[] compressed, int orginalSize) {
        byte[] dest = new byte[orginalSize];
        long r = Zstd.decompress(dest, compressed);
        if (r <= 0) throw new RuntimeException("zstd decompress error: " + r);
        return dest;
    }
}
