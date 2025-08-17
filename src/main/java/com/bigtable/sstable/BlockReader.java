package com.bigtable.sstable;

import com.bigtable.codec.Varint;
import com.bigtable.io.CRC32CUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Reads a block payload (entries + restarts) plus its trailing CRC, validates integrity, and supports point lookup.
 * <p>
 * Expected input: {@code payload||crc32c}, where payload ends with restart array and count.
 */
public final class BlockReader {

    private final byte[] payload;   // payload only (not header), exclude CRC
    private final int[] restarts;

    /**
     * Parse and validate a block given payload-with-crc.
     *
     * @throws IOException if CRC fails or structure invalid
     */
    public BlockReader(byte[] payloadWithCrc) throws IOException {
        if (payloadWithCrc.length < 4) throw new IOException("block too small");
        int crcRead = ByteBuffer.wrap(payloadWithCrc, payloadWithCrc.length - 4, 4).getInt();
        byte[] withoutCrc = new byte[payloadWithCrc.length - 4];

        System.arraycopy(payloadWithCrc, 0, withoutCrc, 0, withoutCrc.length);
        if (CRC32CUtil.crc32c(withoutCrc, 0, withoutCrc.length) != crcRead) {
            throw new IOException("block crc mismatch");
        }

        // Payload: entries ... restart array ... restart count (4 bytes)
        int end = withoutCrc.length;
        int restartCount = ByteBuffer.wrap(withoutCrc, end - 4, 4).getInt();
        int restartArrayOffset = end - 4 - (4 * restartCount);
        this.payload = new byte[restartArrayOffset];
        System.arraycopy(withoutCrc, 0, this.payload, 0, this.payload.length);
        this.restarts = new int[restartCount];
        for (int i = 0; i < restartCount; i++) {
            this.restarts[i] = ByteBuffer.wrap(withoutCrc, restartArrayOffset + i * 4, 4).getInt();
        }
    }

    /**
     * Linear-probe each restart region in order and scan entries until we pass or match the target.
     *
     * @return value bytes or null if not present in the block
     */
    public byte[] get(byte[] target) throws IOException {
        for (int off : restarts) {
            byte[] found = scanFrom(off, target);
            if (found != null) return found;
        }
        return null;
    }

    private byte[] scanFrom(int pos, byte[] target) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(payload, pos, payload.length - pos);
        byte[] lastKey = new byte[0];
        while (in.available() > 0) {
            int shared = Varint.readUnsignedVarInt(in);
            int nonShared = Varint.readUnsignedVarInt(in);
            int valueLen = Varint.readUnsignedVarInt(in);
            byte[] nonSharedKey = in.readNBytes(nonShared);
            if (nonSharedKey.length != nonShared) throw new EOFException();
            byte[] key = new byte[shared + nonShared];
            System.arraycopy(lastKey, 0, key, 0, shared);
            System.arraycopy(nonSharedKey, 0, key, shared, nonShared);
            byte[] value = in.readNBytes(valueLen);
            if (value.length != valueLen) throw new EOFException();
            int cmp = compareUnsigned(key, target);
            lastKey = key;
        }
        return null;
    }

    private static int compareUnsigned(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int ai = a[i] & 0xFF;
            int bi = b[i] & 0xFF;
            if (ai != bi) return Integer.compare(ai, bi);
        }
        return Integer.compare(a.length, b.length);
    }
}
