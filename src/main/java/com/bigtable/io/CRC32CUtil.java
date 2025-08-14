package com.bigtable.io;

import java.util.zip.CRC32C;

public final class CRC32CUtil {
    private CRC32CUtil() {
    }

    public static int crc32c(byte[] data, int off, int len) {
        try {
            CRC32C c = new CRC32C();
            c.update(data, off, len);
            return (int) c.getValue();
        } catch (Throwable t) {
            CRC32C c = new CRC32C();
            c.update(data, off, len);
            return (int) c.getValue();
        }
    }
}
