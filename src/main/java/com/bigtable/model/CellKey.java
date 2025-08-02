package com.bigtable.model;

import java.util.Objects;

// Key structure for the multidimensional sorted map
public class CellKey implements Comparable<CellKey> {
    protected String rowKey;
    protected String columnKey;
    protected long timestamp;

    public CellKey(String rowKey, String columnKey, long timestamp) {
        this.rowKey = rowKey;
        this.columnKey = columnKey;
        this.timestamp = timestamp;
    }

    public String getRowKey() {
        return rowKey;
    }

    public String getColumnKey() {
        return columnKey;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(CellKey other) {
        int cmp = this.rowKey.compareTo(other.rowKey);
        if (cmp != 0) return cmp;
        cmp = this.columnKey.compareTo(other.columnKey);
        if (cmp != 0) return cmp;
        return Long.compare(other.timestamp, this.timestamp);   // Latest timestamp first
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        CellKey cellKey = (CellKey) o;
        return this.timestamp == cellKey.timestamp
                && this.rowKey.equals(cellKey.rowKey)
                && this.columnKey.equals(cellKey.columnKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.rowKey, this.columnKey, this.timestamp);
    }
}
