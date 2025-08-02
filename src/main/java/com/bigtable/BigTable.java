package com.bigtable;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.io.IOException;
import com.bigtable.model.CellKey;
import com.bigtable.model.CellValue;

public class BigTable {

    // Key structure for the multidimensional sorted map


    // In-memory store
    private TreeMap<CellKey, CellValue> memTable;
    // Write-ahead log file
    private File walFile;
    private DataOutputStream walOutput;
    // Directory for SStable
    private String storageDir;

    public BigTable(String storageDir) throws IOException {
        this.memTable = new TreeMap<>();
        this.storageDir = storageDir;
        this.walFile = new File(storageDir, "wal.log");
        Files.createDirectories(Paths.get(storageDir));
        if (!this.walFile.exists()) {
            this.walFile.createNewFile();
        }
        this.walOutput = new DataOutputStream(new FileOutputStream(walFile, true));
    }

    // put a value into the table
    public void put(String rowKey, String columnKey, byte[] value, long timestamp) throws IOException {
        // Write to WAL
        writeToWAL(rowKey, columnKey, timestamp, value, false);
        // Update in-memory table
        CellKey key = new CellKey(rowKey, columnKey, timestamp);
        memTable.put(key, new CellValue(value, false));

        // Flush to disk if memTable is too large (simplified threshold)
        if (memTable.size() > 1000) {
            flushToSStable();
        }
    }

    // Write to write-head log
    private void writeToWAL(String rowKey, String columnKey, long timestamp, byte[] value, boolean isTombstone) throws IOException {
        walOutput.writeUTF(rowKey);
        walOutput.writeUTF(columnKey);
        walOutput.writeLong(timestamp);
        walOutput.writeBoolean(isTombstone);
        if (!isTombstone) {
            walOutput.writeInt(value.length);
            walOutput.write(value);
        }
        walOutput.flush();
    }

    // Flush in-memory table to an SStable
    private void flushToSStable() throws IOException {
        File sstableFile = new File(storageDir, "sstable-" + System.currentTimeMillis() + ".sst");

        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(sstableFile))) {
            for (Map.Entry<CellKey, CellValue> entry : memTable.entrySet()) {
                out.writeUTF(entry.getKey().getRowKey());
                out.writeUTF(entry.getKey().getColumnKey());
                out.writeLong(entry.getKey().getTimestamp());
                out.writeBoolean(entry.getValue().isTombstone());

                if (!entry.getValue().isTombstone()) {
                    out.writeInt(entry.getValue().getValue().length);
                    out.write(entry.getValue().getValue());
                }
            }
        }

        // Clear in-memory table and reset WAL
        memTable.clear();
        walOutput.close();
        walFile.delete();
        walFile.createNewFile();
        walOutput = new DataOutputStream(new FileOutputStream(walFile, true));
    }

    // Close the table
    public void close() throws IOException {
        if (!memTable.isEmpty()) {
            flushToSStable();
        }
        walOutput.close();;
    }

}
