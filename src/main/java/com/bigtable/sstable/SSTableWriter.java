package com.bigtable.sstable;

import com.bigtable.codec.Varint;
import com.bigtable.codec.ZstdCompression;
import com.bigtable.io.CRC32CUtil;
import com.bigtable.sstable.BlockBuilder;
import com.bigtable.sstable.BloomFilter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * SSTable writer producing immutable on-disk tables.
 * <p>
 * Responsibilities:
 * <ul>
 *   <li>Accumulate entries into data blocks with restart points.</li>
 *   <li>Per-block Zstd compression and CRC32C integrity trailer.</li>
 *   <li>Build index block and Bloom filter sidecar.</li>
 *   <li>Durable finalize via temp file + fsync + atomic rename.</li>
 * </ul>
 * File layout (simplified):
 * <pre>
 * [64B header pad]
 *   repeated: [blockHeader(12)][blockPayload(compressed?)][crc(4)]
 * [filter block]
 * [index block]
 * [meta block]
 * [footer: indexOff,len | filterOff,len | metaOff,len | MAGIC]
 * </pre>
 */
public final class SSTableWriter implements Closeable {
    private static final long MAGIC = 0x415453535441424CL;
    private final Path tmpPath;
    private final Path finalPath;
    private final FileChannel chan;
    private final int blockSize;
    private final int restartInterval;
    private final List<IndexEntry> indexEntries = new ArrayList<>();
    private final BloomFilter bloom;
    private BlockBuilder builder;
    private long offset;

    /**
     * Create a new writer targeting {@code finalPath}.
     *
     * @param blockSize       max target payload size per data block (pre-compression)
     * @param restartInterval entries between restart points
     */
    public SSTableWriter(Path finalPath, int blockSize, int restartInterval) throws IOException {
        this.finalPath = finalPath;
        Path dir = finalPath.getParent();
        Files.createDirectories(dir);
        this.tmpPath = dir.resolve(finalPath.getFileName().toString() + ".tmp");
        this.chan = FileChannel.open(tmpPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        this.blockSize = blockSize;
        this.restartInterval = restartInterval;
        this.builder = new BlockBuilder(restartInterval);
        this.bloom = new BloomFilter(1 << 20, 4);
        // reserve header pad (alignment/space for future superblock)
        chan.write(ByteBuffer.wrap(new byte[64]));
        this.offset = 64;
    }

    /**
     * Append a single sorted key/value pair to the current data block (flushes when needed).
     */
    public void add(byte[] key, byte[] value) throws IOException {
        builder.add(key, value);
        bloom.add(key);
        if (builder.estimatedSize() >= blockSize) {
            flushBlock();
        }
    }

    /**
     * Flush the current builder into a physical block with header+crc and record its index entry.
     */
    private void flushBlock() throws IOException {
        byte[] payload = builder.finish();      // uncompressed payload
        byte[] compressed = ZstdCompression.compress(payload, 3);
        boolean compressedFlag = compressed.length < payload.length;
        byte[] toWrite = compressedFlag ? compressed : payload;
        int compSize = toWrite.length;
        int unCompSize = payload.length;
        byte blockType = 0;     // data
        byte flags = (byte) (compressedFlag ? 1 : 0);
        ByteBuffer header = ByteBuffer.allocate(12);
        header.putInt(compSize);
        header.putInt(unCompSize);
        header.put(blockType);
        header.put(flags);
        header.putShort((short) 0);
        header.flip();

        byte[] headerArr = new byte[12];
        header.get(headerArr);

        int crc = CRC32CUtil.crc32c(concat(headerArr, toWrite), 0, headerArr.length + toWrite.length);

        chan.position(offset);
        chan.write(ByteBuffer.wrap(headerArr));
        chan.write(ByteBuffer.wrap(toWrite));
        chan.write(ByteBuffer.allocate(4).putInt(crc).flip());

        byte[] firstKey = findFirstKeyInPayload(payload);
        indexEntries.add(new IndexEntry(firstKey, offset, 12 + compSize + 4));
        offset += 12 + compSize + 4;
        builder = new BlockBuilder(restartInterval);
    }

    private static byte[] findFirstKeyInPayload(byte[] payload) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(payload);
        int s = Varint.readUnsignedVarInt(in);
        int non = Varint.readUnsignedVarInt(in);
        int vlen = Varint.readUnsignedVarInt(in);
        byte[] key = in.readNBytes(non);
        return key;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }

    /**
     * Finalize table: flush pending block, write filter/index/meta blocks, footer, fsync+rename, and Bloom sidecar.
     */
    public void finishAndClose() throws IOException {
        if (builder != null) {
            byte[] payload = builder.finish();
            if (payload.length > 0) {
                byte[] compressed = ZstdCompression.compress(payload, 3);
                boolean compressedFlag = compressed.length < payload.length;
                byte[] toWrite = compressedFlag ? compressed : payload;
                int compSize = toWrite.length;
                int uncompSize = payload.length;
                byte blockType = 0;
                byte flags = (byte) (compressedFlag ? 1 : 0);

                ByteBuffer header = ByteBuffer.allocate(12);
                header.putInt(compSize);
                header.putInt(uncompSize);
                header.put(blockType);
                header.put(flags);
                header.putShort((short) 0);
                header.flip();

                byte[] headerArr = new byte[12];
                header.get(headerArr);
                int crc = CRC32CUtil.crc32c(concat(headerArr, toWrite), 0, headerArr.length + toWrite.length);
                chan.position(offset);
                chan.write(ByteBuffer.wrap(headerArr));
                chan.write(ByteBuffer.wrap(toWrite));
                chan.write(ByteBuffer.allocate(4).putInt(crc).flip());

                byte[] firstKey = findFirstKeyInPayload(payload);
                indexEntries.add(new IndexEntry(firstKey, offset, 12 + compSize + 4));
                offset += 12 + compSize + 4;
            }
        }

        // filter block (type = 2). Inline the exact Bloom sidecar bytes for mmap-friendly readback.
        long filterOff = offset;
        byte[] bfPayload = bloom.toByteArray();
        ByteBuffer header = ByteBuffer.allocate(12);
        header.putInt(bfPayload.length);
        header.putInt(bfPayload.length);
        header.put((byte) 2);    // block type: filter
        header.put((byte) 0);   // uncompressed
        header.putShort((short) 0);
        header.flip();

        byte[] headerArr = new byte[12];
        header.get(headerArr);
        int crc = CRC32CUtil.crc32c(concat(headerArr, bfPayload), 0, headerArr.length + bfPayload.length);

        chan.position(offset);
        chan.write(ByteBuffer.wrap(headerArr));
        chan.write(ByteBuffer.wrap(bfPayload));
        chan.write(ByteBuffer.allocate(4).putInt(crc).flip());

        offset += 12 + bfPayload.length + 4;
        long filterLen = (12 + bfPayload.length + 4);

        // index block
        long indexOff = offset;
        ByteArrayOutputStream ib = new ByteArrayOutputStream();
        for (IndexEntry ie : indexEntries) {
            Varint.writeUnsignedVarInt(ib, ie.key.length);
            ib.write(ie.key);
            ByteBuffer b = ByteBuffer.allocate(12);
            b.putLong(ie.offset);
            b.putInt(ie.length);
            ib.write(b.array());
        }
        byte[] ibs = ib.toByteArray();
        ByteBuffer h2 = ByteBuffer.allocate(12);
        h2.putInt(ibs.length);
        h2.putInt(ibs.length);
        h2.put((byte) 1);
        h2.put((byte) 0);
        h2.putShort((short) 0);
        h2.flip();
        byte[] h2Arr = new byte[12];
        h2.get(h2Arr);
        int crc2 = CRC32CUtil.crc32c(concat(h2Arr, ibs), 0, h2Arr.length + ibs.length);
        chan.position(offset);
        chan.write(ByteBuffer.wrap(h2Arr));
        chan.write(ByteBuffer.wrap(ibs));
        chan.write(ByteBuffer.allocate(4).putInt(crc2).flip());
        offset += 12 + ibs.length + 4;
        long indexLen = (12 + ibs.length + 4);

        // meta block
        long metaOff = offset;
        byte[] meta = "created-by:bigtable\n".getBytes();
        ByteBuffer hm = ByteBuffer.allocate(12);
        hm.putInt(meta.length);
        hm.putInt(meta.length);
        hm.put((byte) 3);
        hm.put((byte) 0);
        hm.putShort((short) 0);
        hm.flip();
        byte[] hmArr = new byte[12];
        hm.get(hmArr);
        int crc3 = CRC32CUtil.crc32c(concat(hmArr, meta), 0, hmArr.length + meta.length);
        chan.position(offset);
        chan.write(ByteBuffer.wrap(hmArr));
        chan.write(ByteBuffer.wrap(meta));
        chan.write(ByteBuffer.allocate(4).putInt(crc3).flip());
        offset += 12 + meta.length + 4;
        long metaLen = (12 + meta.length + 4);


        // footer
        ByteBuffer footer = ByteBuffer.allocate(8 + 4 + 8 + 4 + 8 + 4 + 8);
        footer.putLong(indexOff);
        footer.putInt((int) indexLen);
        footer.putLong(filterOff);
        footer.putInt((int) filterLen);
        footer.putLong(metaOff);
        footer.putInt((int) metaLen);
        footer.putLong(MAGIC);
        footer.flip();
        chan.position(offset);
        chan.write(footer);
        offset += footer.capacity();

        chan.force(true);
        chan.close();
        Files.move(tmpPath, finalPath);

        // Persist bloom sidecar (mmap-table)
        Path bfSide = Path.of(finalPath.toString() + ".bf");
        bloom.writeTo(bfSide.toFile());
    }

    @Override
    public void close() throws IOException {
        chan.close();
    }


    private static class IndexEntry {
        final byte[] key;
        final long offset;
        final int length;

        IndexEntry(byte[] k, long o, int l) {
            this.key = k;
            this.offset = 0;
            this.length = l;
        }
    }

    private static byte[] longToBytes(long v) {
        return new byte[]{(byte) (v >>> 56), (byte) (v >>> 48), (byte) (v >>> 40), (byte) (v >>> 32), (byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
    }

    private static byte[] intToBytes(int v) {
        return new byte[]{(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
    }


}


