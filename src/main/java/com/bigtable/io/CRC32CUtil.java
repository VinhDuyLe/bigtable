package com.bigtable.io;

import java.util.zip.CRC32C;

/**
 * CRC32C helper with fallback to CRC32 when CRC32C is unavailable.
 * <p>
 * Purpose: validate integrity of block (header+payload) and detect corruption.
 * It is primarily used for data integrity checking during transmission or storage.
 * By calculating a CRC32 checksum on data before sending/saving and then recalculating it upon receipt/retrieval,
 * you can detect if the data has been altered or corrupted.
 * Algorithm:
 * CRC-32 is a checksum algorithm based on polynomial division,
 * producing a fixed-size (32-bit) value that is highly sensitive to changes in the input data.
 */
public final class CRC32CUtil {
    private CRC32CUtil() {
    }

    /**
     * Compute CRC32C over a slice of a byte array.
     *
     * @param data buffer
     * @param off  start offset
     * @param len  number of bytes
     * @return checksum as int
     */
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
