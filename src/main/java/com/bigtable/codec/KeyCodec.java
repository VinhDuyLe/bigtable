package com.bigtable.codec;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Composite-key encoder used as SSTable key bytes.
 * <p>
 * Layout: varint(rowLen) | row | varint(familyId) | varint(qualifierLen) | qualifier | u64(descTimestamp)
 * where descTimestamp = ~timestampMicros (big-endian), enabling newest-first ordering by bytes.
 */
public final class KeyCodec {
    private KeyCodec() {
    }

    /**
     * Encode (rowKey, familyId, qualifier, timestamp) into a byte-sortable composite key.
     */
    public static byte[] encodeCompositeKey(byte[] rowKey, int familyId, byte[] qualifier, long timestampMicros) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Varint.writeUnsignedVarInt(out, rowKey.length);
            out.write(rowKey);
            Varint.writeUnsignedVarInt(out, familyId);
            Varint.writeUnsignedVarInt(out, qualifier.length);
            out.write(qualifier);
            long desc = (~timestampMicros) & 0xFFFFFFFFFFFFFFFFL; // UINT64_MAX - ts
            ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
            bb.putLong(desc);
            out.write(bb.array());
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
