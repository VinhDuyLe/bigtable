package com.bigtable.codec;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Encodes composite keys: [rowLen][row][familyId][qualLen][qualifier][timestampDesc(8)]
 */
public final class KeyCodec {
    private KeyCodec() {
    }

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
