package com.bigtable.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Minimal unsigned varint (LEB128) writer/reader.
 */
public final class Varint {
    private Varint() {
    }

    public static void writeUnsignedVarInt(OutputStream out, int value) throws IOException {
        while ((value & 0xFFFFFF80) != 0L) {           // 128
            out.write((value & 0x7F) | 0x80);
            value >>>= 7;
        }
    }

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
