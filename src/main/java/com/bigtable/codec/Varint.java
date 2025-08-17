package com.bigtable.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Unsigned LEB128 VarInt utilities.
 * <p>
 * Purpose: compactly encode non-negative integers for block-entry headers
 * (shared/nonShared/valueLen) and index key lengths.
 * <ul>
 *   <li>{@link #writeUnsignedVarInt(OutputStream, int)} — encodes an int.</li>
 *   <li>{@link #readUnsignedVarInt(InputStream)} — decodes back to int.</li>
 * </ul>
 */
public final class Varint {
    private Varint() {
    }

    /**
     * Write an unsigned VarInt (LEB128) to the stream.
     *
     * @param out   destination stream
     * @param value non-negative value to encode
     */
    public static void writeUnsignedVarInt(OutputStream out, int value) throws IOException {
        while ((value & 0xFFFFFF80) != 0L) {           // 128
            out.write((value & 0x7F) | 0x80);
            value >>>= 7;
        }
    }

    /**
     * Read an unsigned VarInt (LEB128) from the stream.
     *
     * @param in source stream
     * @return decoded non-negative integer
     */
    public static int readUnsignedVarInt(InputStream in) throws IOException {
        int value = 0;
        int i = 0;
        int b;
        while (((b = in.read()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 35) throw new IOException("VarInt too long");
        }
        value |= b << i;
        return value;
    }
}
